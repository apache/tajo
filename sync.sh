#!/usr/bin/env bash

####################
### Contributing ###
####################
# 1. Fork (and then git clone https://github.com/<your-username-here>/tajo.git).
# 2. Create a branch (git checkout -b branch_name).
# 3. Commit your changes (git commit -am "Description of contribution").
# 4. Push the branch (git push origin branch_name).
# 5. Open a Pull Request.
# 6. Thank you for your contribution! Wait for a response...

# add the main Tajo repository as upstream
git remote add upstream https://github.com/apache/tajo.git

# pull in case there are some changes in the origin
git pull origin

# fetch all the branches of the main Tajo repository
git fetch upstream

# checkout the local "master" branch
git checkout master

# merge the local "master" branch with the "master" branch of the main Tajo repository
git merge upstream/master
