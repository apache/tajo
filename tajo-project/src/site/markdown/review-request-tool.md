<!--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

## Table of Contents

* [Tajo Review Request Tool] (#ReviewTool)
 * [Setup](#Setup)
 * [Usage](#Usage)
 * [Upload the first patch] (#UploadFirstPatch)
 * [Update the existing patch] (#UpdatePatch)
* [Preparation] (#Preparation)
 * [Jira command line tool] (#JiraTool)
 * [Reviewboard] (#Reviewboard)
* [FAQ] (#FAQ)

## <a name="ReviewTool"></a>Tajo Review Request Tool

### <a name="Setup"></a>Setup
 1. Follow instructions here to [setup the jira-python package] (#JiraTool)
 1. Follow instructions here to [setup the reviewboard python tools] (#Reviewboard)
 1. Install the argparse module
 
Linux

```
sudo yum install python-argparse
```

Mac

```
sudo easy_install argparse
```

### <a name="Usage"></a>Usage

```
$ ./request-patch-review.py --help
usage: request-patch-review.py [-h] -b BRANCH -j JIRA [-skip-rb] [-s SUMMARY]
                               [-d DESCRIPTION] [-c CHANGE_DESCRIPTION] [-pa]
                               [-r REVIEWBOARD] [-t TESTING] [-db]

Kafka patch review tool

optional arguments:
  -h, --help            show this help message and exit
  -b BRANCH, --branch BRANCH
                        Tracking branch to create diff against
  -j JIRA, --jira JIRA  JIRA corresponding to the reviewboard
  -skip-rb, --skip-reviewboard
                        Skip a review request to reviewboard.
  -s SUMMARY, --summary SUMMARY
                        Summary for the reviewboard
  -d DESCRIPTION, --description DESCRIPTION
                        Description for reviewboard
  -c CHANGE_DESCRIPTION, --change-description CHANGE_DESCRIPTION
                        Description of what changed in this revision of the
                        review request when updating an existing request
  -pa, --patch-available
                        Transite the JIRA status to Patch Available. If its
                        status is already Patch Available, it updates the
                        status of the JIRA issue by transiting its status to
                        Open and Patch Available sequentially.
  -r REVIEWBOARD, --rb REVIEWBOARD
                        Review board that needs to be updated
  -t TESTING, --testing-done TESTING
                        Text for the Testing Done section of the reviewboard
  -db, --debug          Enable debug mode

```

### <a name="UploadFirstPatch"></a> Upload the first patch
**You should create a jira issue prior to a patch review request.**

 * Specify the branch against which the patch should be created (-b)
 * Specify the corresponding JIRA (-j)
 * Specify an optional summary (-s) and description (-d) for the reviewboard
  * If they are not given, the summary and description of the first review request in reviewboard are borrowed from the corresponding JIRA issue.
 
Example:

```
$ ./request-patch-review.py -b origin/master -j TAJO-543
```

### <a name="UpdatePatch"></a>Update the existing patch
 * Specify the remote branch against which the patch should be created (-b)
 * Specify the corresponding JIRA (--jira)
 * Specify the rb to be updated (-r)
 * Specify an optional summary (-s) and description (-d) for the reviewboard, if you want to update them.
 * Specify a change description (-c) for the reviewboard, if you want to describe it. The change description is what changed in this revision of the review request.
 
Example:

```
$ ./request-patch-review.py -b origin/master -j TAJO-543 -r 14081 -c "Add more unit tests"
```

## <a name="Preparation"></a>Preparation
### <a name="JiraTool"></a>JIRA command line tool
 1. Download the JIRA command line package

Install the jira-python package

```
sudo easy_install jira-python
```

 2. Configure JIRA username and password
 
Create a ${HOME}/.jira.ini file in your $HOME directory that contains your Apache JIRA username and password

```
$ cat ~/.jira.ini
user=hyunsik
password=***********
```

### <a name="Reviewboard"></a>Reviewboard
This is a quick tutorial on using Review Board with Tajo.

#### 1. Install the post-review tool
If you are on RHEL, Fedora or CentOS, follow these steps

```
sudo yum install python-setuptools
sudo easy_install -U RBTools
```

If you are on Mac, follow these steps

```
sudo easy_install -U setuptools
sudo easy_install -U RBTools
```

For other platforms, follow the instructions here to setup the post-review tool.


#### 2. Configure Stuff
Then you need to configure a few things to make it work:
First set the review board url to use. You can do this from in git:

```
git config reviewboard.url https://reviews.apache.org
```

If you checked out using the git wip http url that confusingly won't work with review board. So you need to configure an override to use the non-http url. You can do this by adding a config file like this:

```
$ cat ~/.reviewboardrc
REPOSITORY = 'git://git.apache.org/incubator-tajo.git'
TARGET_GROUPS = 'Tajo'
```

## <a name="FAR"></a>FAQ
**When I run the script, it throws the following error and exits**

```
$ tajo-patch-review.py -b asf/master -j TAJO-593
There don't seem to be any diffs
```

There are 2 reasons that can cause this -
 * The code is not checked into your local branch
 * The -b branch is not pointing to the remote branch. In the example above, "master" is specified as the branch, which is the local branch. The correct value for the -b (--branch) option is the remote branch. "git branch -r" gives the list of the remote branch names.
 
**When I run the script, it throws the following error and exits**

```
Error uploading diff

Your review request still exists, but the diff is not attached.
```

One of the most common root causes of this error are that the git remote branches are not up-to-date. Since the script already does that, it is probably due to some other problem. You can run the script with the --debug option that will make post-review run in the debug mode and list the root cause of the issue.