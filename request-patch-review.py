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

def get_jira():
  options = {
    'server': 'https://issues.apache.org/jira'
  }
  # read the config file
  home=jira_home=os.getenv('HOME')
  home=home.rstrip('/')
  jira_config = dict(line.strip().split('=') for line in open(home + '/.jira.ini'))
  jira = JIRA(options,basic_auth=(jira_config['user'], jira_config['password']))
  return jira 

def main():
  ''' main(), shut up, pylint '''
  popt = argparse.ArgumentParser(description='Tajo patch review tool')
  popt.add_argument('-b', '--branch', action='store', dest='branch', required=True, help='Tracking branch to create diff against')
  popt.add_argument('-j', '--jira', action='store', dest='jira', required=True, help='JIRA corresponding to the reviewboard')
  popt.add_argument('-skip-rb', '--skip-reviewboard', action='store_true', dest='skip_reviewboard', required=False, help='Skip a review request to reviewboard.')
  popt.add_argument('-s', '--summary', action='store', dest='summary', required=False, help='Summary for the reviewboard')
  popt.add_argument('-d', '--description', action='store', dest='description', required=False, help='Description for reviewboard')
  popt.add_argument('-c', '--change-description', action='store', dest='change_description', required=False, help='Description of what changed in this revision of the review request when updating an existing request')
  popt.add_argument('-pa', '--patch-available', action='store_true', dest='patch_available', required=False, help='Transite the JIRA status to Patch Available. If its status is already Patch Available, it updates the status of the JIRA issue by transiting its status to Open and Patch Available sequentially.')
  popt.add_argument('-r', '--rb', action='store', dest='reviewboard', required=False, help='Review board that needs to be updated')
  popt.add_argument('-t', '--testing-done', action='store', dest='testing', required=False, help='Text for the Testing Done section of the reviewboard')
  popt.add_argument('-db', '--debug', action='store_true', required=False, help='Enable debug mode')
  opt = popt.parse_args()

  # the patch name is determined here.
  patch_file=tempfile.gettempdir() + "/" + opt.jira + ".patch"
  if opt.reviewboard:
    ts = time.time()
    st = datetime.datetime.fromtimestamp(ts).strftime('%Y%m%d_%H:%M:%S')
    patch_file=tempfile.gettempdir() + "/" + opt.jira + '_' + st + '.patch'

  # first check if rebase is needed
  git_branch_hash="git rev-parse " + opt.branch
  p_now=os.popen(git_branch_hash)
  branch_now=p_now.read()
  p_now.close()

  git_common_ancestor="git merge-base " + opt.branch + " HEAD"
  p_then=os.popen(git_common_ancestor)
  branch_then=p_then.read()
  p_then.close()

  # get remote and branch name
  remote_name=opt.branch.split("/")[0]
  branch_name=opt.branch.split("/")[1]

  if branch_now != branch_then:
    print 'ERROR: Your current working branch is from an older version of ' + opt.branch + '. Please rebase first by using git pull --rebase'
    sys.exit(1)

  git_configure_reviewboard="git config reviewboard.url https://reviews.apache.org"
  print "Configuring reviewboard url to https://reviews.apache.org"
  p=os.popen(git_configure_reviewboard)
  p.close()

  # update the specified remote branch
  git_remote_update="git fetch " + remote_name
  print "Updating your remote branche " + opt.branch + " to pull the latest changes"
  p=os.popen(git_remote_update)
  p.close()

  # get jira and issue instance 
  jira=get_jira()
  issue = jira.issue(opt.jira)

  if not opt.skip_reviewboard:
    rb_command="post-review --publish --tracking-branch " + opt.branch + " --target-groups=Tajo --branch=" + branch_name + " --bugs-closed=" + opt.jira
    if opt.reviewboard:
      rb_command=rb_command + " -r " + opt.reviewboard
    summary=issue.key + ": " + issue.fields.summary # default summary is 'TAJO-{NUM}: {JIRA TITLE}'
    if opt.summary: # if a summary is given, this field is added or updated
      summary=opt.summary
    if not opt.reviewboard: # if a review request is created
      rb_command=rb_command + " --summary '" + summary + "'"
    description=issue.fields.description 
    if opt.description: # if a descriptin is give, this field is added
      description = opt.description
    if opt.reviewboard and opt.change_description:
      rb_command=rb_command + " --change-description '" + opt.change_description + "'"
    if not opt.reviewboard: # if a review request is created
      rb_command=rb_command + " --description '" + description + "'"
    if opt.testing:
      rb_command=rb_command + " --testing-done=" + opt.testing
    if opt.debug:
      rb_command=rb_command + " --debug" 
      print rb_command
    p=os.popen(rb_command)

    rb_url=""
    for line in p:
      print line
      if line.startswith('http'):
        rb_url = line
      elif line.startswith("There don't seem to be any diffs"):
        print 'ERROR: Your reviewboard was not created/updated since there was no diff to upload. The reasons that can cause this issue are 1) Your diff is not checked into your local branch. Please check in the diff to the local branch and retry 2) You are not specifying the local branch name as part of the --branch option. Please specify the remote branch name obtained from git branch -r'
        p.close()
        sys.exit(1)
      elif line.startswith("Your review request still exists, but the diff is not attached") and not opt.debug:
        print 'ERROR: Your reviewboard was not created/updated. Please run the script with the --debug option to troubleshoot the problem'
        p.close()
        sys.exit(1)
    p.close()
    if opt.debug: 
      print 'rb url=',rb_url
 
  git_command="git diff --no-prefix " + opt.branch + " > " + patch_file
  if opt.debug:
    print git_command
  p=os.popen(git_command)
  p.close()

  print 'Creating diff against', opt.branch, 'and uploading patch to ',opt.jira
  attachment=open(patch_file)
  jira.add_attachment(issue,attachment)
  attachment.close()

  # Add comment about a request to reviewboard and its url.
  if not opt.skip_reviewboard:
    comment="Created a review request against branch " + branch_name + " in reviewboard " 
    if opt.reviewboard:
      comment="Updated the review request against branch " + branch_name + " in reviewboard "

    comment = comment + "\n" + rb_url
    jira.add_comment(opt.jira, comment)

  # Transition the jira status to Patch Available
  if opt.patch_available:
    if issue.fields.status.id == '10002': # If the jira status is already Patch Available (id - 10002)
      jira.transition_issue(issue, '731') # Cancel (id - 731) the uploaded patch
      issue = jira.issue(opt.jira)
    jira.transition_issue(issue, '10002')

if __name__ == '__main__':
  sys.exit(main())

