# Contributing

## GitHub process

* Fork (and then `git clone https://github.com/<your-username-here>/tajo.git`).
* Create a branch (`git checkout -b branch_name`).
* Commit your changes (`git commit -am "Description of contribution"`).
* Push the branch (`git push origin branch_name`).
* Open a Pull Request.
* Thank you for your contribution! Wait for a response...

## Source Synchronization
* add the main Tajo repository as upstream

     ```shell
     git remote add upstream https://github.com/apache/tajo.git
     ```

* pull in case there are some changes in the origin

     ```shell
     git pull origin
     ```

* fetch all the branches of the main Tajo repository

     ```shell
     git fetch upstream
     ```

* checkout the local "master" branch

     ```shell
     git checkout master
     ```

* merge the local "master" branch with the "master" branch of the main Tajo repository

     ```shell
     git merge upstream/master
     ```