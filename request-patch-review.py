#!/usr/bin/env python

# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import argparse
import sys
import os
import time
import datetime
import tempfile
from jira.client import JIRA

# Parses command line arguments.
def parse_opts():
  popt = argparse.ArgumentParser(description='Tajo patch review tool')
  popt.add_argument('-b', '--branch',
                    action='store', dest='branch', required=False,
                    help='Tracking branch to create diff against')
  popt.add_argument('-j', '--jira',
                    action='store', dest='jira', required=True,
                    help='JIRA corresponding to the reviewboard')
  popt.add_argument('-skip-rb', '--skip-reviewboard',
                    action='store_true', dest='skip_reviewboard',
                    required=False,
                    help='Skip a review request to reviewboard.')
  popt.add_argument('-s', '--summary',
                    action='store', dest='summary', required=False,
                    help='Summary for the reviewboard')
  popt.add_argument('-d', '--description',
                    action='store', dest='description', required=False,
                    help='Description for reviewboard')
  popt.add_argument('-c', '--change-description',
                    action='store', dest='change_description', required=False,
                    help='Description of what changed in this revision '
                         'of the review request when updating an existing '
                         'request')
  popt.add_argument('-r', '--rb',
                    action='store', dest='reviewboard', required=False,
                    help='Review board that needs to be updated')
  popt.add_argument('-t', '--testing-done',
                    action='store', dest='testing', required=False,
                    help='Text for the Testing Done section of the '
                         'reviewboard')
  popt.add_argument('-db', '--debug',
                    action='store_true', required=False,
                    help='Enable debug mode')
  return popt.parse_args()

# Gets the JIRA client instance.
def get_jira():
  options = {
    'server': 'https://issues.apache.org/jira'
  }
  home = os.getenv('HOME')
  home = home.rstrip('/')
  jira_config = (dict(line.strip().split('=')
                 for line in open(home + '/.jira.ini')))
  jira = JIRA(options,
              basic_auth=(jira_config['user'], jira_config['password']))
  return jira

# Reads the .reviewboardrc file from the current directory, which should be
# the root of the source tree.
def get_reviewboardrc():
  config = {}
  for line in open('.reviewboardrc'):
    parts = line.strip().split('=')
    key = parts[0]
    value = parts[1].strip('\'')
    config[key] = value
  return config

# Get remote and branch name.
def get_remote_branch(opt, rb_config):
  if opt.branch:
    branch = opt.branch
  else:
    branch = rb_config['TRACKING_BRANCH']

  parts = branch.split("/")
  if len(parts) < 2:
    print("Remote branch name must be in the following format: remote/branch.")
    print("For example: origin/master")
    sys.exit(2)
  return branch, parts[0], parts[1]

# Returns the current branch and the branch it should be diffed against.
def get_diff_branches(opt, tracking_branch):
  git_branch_hash = "git rev-parse " + tracking_branch
  p_now = os.popen(git_branch_hash)
  branch_now = p_now.read()
  p_now.close()

  git_common_ancestor = "git merge-base " + tracking_branch + " HEAD"
  p_then = os.popen(git_common_ancestor)
  branch_then = p_then.read()
  p_then.close()

  return branch_now, branch_then

# Fetches changes from the remote branch.
def fetch_remote_branch(opt, remote_name, tracking_branch):
  git_remote_update = "git fetch " + remote_name
  print("Updating your remote branch " + tracking_branch +
        " to pull the latest changes")
  p = os.popen(git_remote_update)
  p.close()

CMD_RBT_POST = ('rbt post --publish --tracking-branch %s '
                '--target-groups=Tajo --branch=%s --bugs-closed=%s')

# Posts the patch to Review Board.
def post_reviewboard(opt, branch_name, tracking_branch, issue):
  cmd_parts = []
  cmd_parts.append(CMD_RBT_POST % (tracking_branch, branch_name, opt.jira))
  if opt.reviewboard:
    cmd_parts.append(" --update -r " + opt.reviewboard)

  # Default summary is 'TAJO-{NUM}: {JIRA TITLE}'.
  # If a summary is given, this field is added or updated.
  summary = issue.key + ": " + issue.fields.summary
  if opt.summary:
    summary = opt.summary

  # If a review request is created
  if not opt.reviewboard:
    cmd_parts.append(" --summary '%s'" % summary)

  # if a descriptin is give, this field is added
  description = issue.fields.description
  if opt.description:
    description = opt.description
  if opt.reviewboard and opt.change_description:
    cmd_parts.append(" --change-description '%s'" % opt.change_description)

  # if a review request is created
  if not opt.reviewboard:
    cmd_parts.append(" --description '%s'" % description)
  if opt.testing:
    cmd_parts.append(" --testing-done=" + opt.testing)
  if opt.debug:
    cmd_parts.append(" --debug")

  # Execute command.
  rb_post_cmd = ''.join(cmd_parts)
  if opt.debug:
    print rb_post_cmd
  p = os.popen(rb_post_cmd)

  rb_url = ""
  for line in p:
    print line
    if line.startswith('http'):
      rb_url = line
    elif line.startswith("There don't seem to be any diffs"):
      print('ERROR: Your reviewboard was not created/updated since there '
            'was no diff to upload. The reasons that can cause this issue are '
            '1) Your diff is not checked into your local branch. '
            'Please check in the diff to the local branch and retry '
            '2) You are not specifying the local branch name as part of the '
            '--branch option. Please specify the remote branch name obtained '
            'from git branch -r')
      p.close()
      sys.exit(1)
    elif line.startswith('Your review request still exists, but the diff'
                         'is not attached') and not opt.debug:
      print('ERROR: Your reviewboard was not created/updated. Please run '
            'the script with the --debug option to troubleshoot the problem')
      p.close()
      sys.exit(1)

  p.close()
  if opt.debug:
    print 'rb url = ' + rb_url
  return rb_url

# Generates a patch file and posts it to the JIRA ticket.
def post_patch(opt, jira, issue, tracking_branch):
  # the patch name is determined here.
  patch_file = tempfile.gettempdir() + "/" + opt.jira + ".patch"
  if opt.reviewboard:
    ts = time.time()
    st = datetime.datetime.fromtimestamp(ts).strftime('%Y%m%d_%H:%M:%S')
    patch_file = tempfile.gettempdir() + "/" + opt.jira + '_' + st + '.patch'

  git_command = "git diff --no-prefix " + tracking_branch + " > " + patch_file
  if opt.debug:
    print git_command
  p = os.popen(git_command)
  p.close()

  print('Creating diff against ' + tracking_branch +
        ' and uploading patch to ' + opt.jira)
  attachment = open(patch_file)
  jira.add_attachment(issue, attachment)
  attachment.close()

# Posts a comment to the JIRA ticket for the posted review.
def post_reviewboard_comment(opt, jira, branch_name, rb_url):
  comment = ("Created a review request against branch " + branch_name +
             " in reviewboard")
  if opt.reviewboard:
    comment = ("Updated the review request against branch " + branch_name +
               " in reviewboard")

  comment = comment + "\n" + rb_url
  jira.add_comment(opt.jira, comment)

def main():
  ''' main(), shut up, pylint '''
  opt = parse_opts()
  rb_config = get_reviewboardrc()

  # Get remote and branch name.
  (tracking_branch, remote_name, branch_name) = get_remote_branch(opt, rb_config)

  # First check if rebase is needed.
  (branch_now, branch_then) = get_diff_branches(opt, tracking_branch)
  if branch_now != branch_then:
    print('ERROR: Your current working branch is from an older version of ' +
          tracking_branch + '. Please rebase first by using ' +
          'git pull --rebase')
    sys.exit(1)

  # Update the specified remote branch.
  fetch_remote_branch(opt, remote_name, tracking_branch)

  # Get jira and issue instance.
  jira = get_jira()
  issue = jira.issue(opt.jira)

  # Post to Review Board.
  rb_url = None
  if not opt.skip_reviewboard:
    rb_url = post_reviewboard(opt, branch_name, tracking_branch, issue)

  # Create diff file and post patch to JIRA ticket.
  post_patch(opt, jira, issue, tracking_branch)

  # Add comment about a request to reviewboard and its url.
  if not opt.skip_reviewboard:
    post_reviewboard_comment(opt, jira, branch_name, rb_url)

if __name__ == '__main__':
  sys.exit(main())

