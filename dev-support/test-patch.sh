#!/usr/bin/env bash
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.


#set -x

### Setup some variables.  
### GIT_REVISION and BUILD_URL are set by Hudson if it is run by patch process
### Read variables from properties file
bindir=$(dirname $0)

# Defaults
if [ -z "$MAVEN_HOME" ]; then
  MVN=mvn
else
  MVN=$MAVEN_HOME/bin/mvn
fi

PROJECT_NAME=Tajo
JENKINS=false
PATCH_DIR=/tmp
SUPPORT_DIR=/tmp
BASEDIR=$(pwd)

PS=${PS:-ps}
AWK=${AWK:-awk}
WGET=${WGET:-wget}
GIT=${GIT:-git}
GREP=${GREP:-grep}
PATCH=${PATCH:-patch}
DIFF=${DIFF:-diff}
JIRACLI=${JIRA:-jira}
FINDBUGS_HOME=${FINDBUGS_HOME}
FORREST_HOME=${FORREST_HOME}
ECLIPSE_HOME=${ECLIPSE_HOME}
REMOTE_NAME=${REMOTE_NAME:-origin}
BRANCH_NAME=${BRANCH_NAME:-master}
TAJO_MVN_OPTS=${TAJO_MVN_OPTS}

###############################################################################
printUsage() {
  echo "Usage: $0 [options] patch-file | defect-number"
  echo
  echo "Where:"
  echo "  patch-file is a local patch file containing the changes to test"
  echo "  defect-number is a JIRA defect number (e.g. 'TAJO-1234') to test (Jenkins only)"
  echo
  echo "Options:"
  echo "--patch-dir=<dir>      The directory for working and output files (default '/tmp')"
  echo "--basedir=<dir>        The directory to apply the patch to (default current directory)"
  echo "--mvn-cmd=<cmd>        The 'mvn' command to use (default \$MAVEN_HOME/bin/mvn, or 'mvn')"
  echo "--mvn-opts=<opt>       The maven options to use"
  echo "--ps-cmd=<cmd>         The 'ps' command to use (default 'ps')"
  echo "--awk-cmd=<cmd>        The 'awk' command to use (default 'awk')"
  echo "--git-cmd=<cmd>        The 'git' command to use (default 'git')"
  echo "--grep-cmd=<cmd>       The 'grep' command to use (default 'grep')"
  echo "--patch-cmd=<cmd>      The 'patch' command to use (default 'patch')"
  echo "--diff-cmd=<cmd>       The 'diff' command to use (default 'diff')"
  echo "--findbugs-home=<path> Findbugs home directory (default FINDBUGS_HOME environment variable)"
  echo "--forrest-home=<path>  Forrest home directory (default FORREST_HOME environment variable)"
  echo "--dirty-workspace      Allow the local GIT workspace to have uncommitted changes"
  echo "--run-tests            Run all tests below the base directory"
  echo
  echo "Jenkins-only options:"
  echo "--jenkins              Run by Jenkins (runs tests and posts results to JIRA)"
  echo "--support-dir=<dir>    The directory to find support files in"
  echo "--wget-cmd=<cmd>       The 'wget' command to use (default 'wget')"
  echo "--jira-cmd=<cmd>       The 'jira' command to use (default 'jira')"
  echo "--jira-user=<user>     The username for the 'jira' command"
  echo "--jira-password=<pw>   The password for the 'jira' command"
  echo "--eclipse-home=<path>  Eclipse home directory (default ECLIPSE_HOME environment variable)"
  echo "--remote-name=<name>   Git Repository Remote name (default origin)"
  echo "--branch-name=<branch> Git Repository Branch name (default master)"
}

###############################################################################
parseArgs() {
  for i in $*
  do
    case $i in
    --jenkins)
      JENKINS=true
      ;;
    --patch-dir=*)
      PATCH_DIR=${i#*=}
      ;;
    --support-dir=*)
      SUPPORT_DIR=${i#*=}
      ;;
    --basedir=*)
      BASEDIR=${i#*=}
      ;;
    --mvn-cmd=*)
      MVN=${i#*=}
      ;;
    --ps-cmd=*)
      PS=${i#*=}
      ;;
    --awk-cmd=*)
      AWK=${i#*=}
      ;;
    --wget-cmd=*)
      WGET=${i#*=}
      ;;
    --git-cmd=*)
      GIT=${i#*=}
      ;;
    --grep-cmd=*)
      GREP=${i#*=}
      ;;
    --patch-cmd=*)
      PATCH=${i#*=}
      ;;
    --diff-cmd=*)
      DIFF=${i#*=}
      ;;
    --jira-cmd=*)
      JIRACLI=${i#*=}
      ;;
    --jira-user=*)
      JIRA_USER=${i#*=}
      ;;
    --jira-password=*)
      JIRA_PASSWD=${i#*=}
      ;;
    --findbugs-home=*)
      FINDBUGS_HOME=${i#*=}
      ;;
    --forrest-home=*)
      FORREST_HOME=${i#*=}
      ;;
    --eclipse-home=*)
      ECLIPSE_HOME=${i#*=}
      ;;
    --branch-name=*)
      BRANCH_NAME=${i#*=}
      ;;
    --remote-name=*)
      REMOTE_NAME=${i#*=}
      ;;
    --mvn-opts=*)
      TAJO_MVN_OPTS=${i#*=}
      ;;
    --dirty-workspace)
      DIRTY_WORKSPACE=true
      ;;
    --run-tests)
      RUN_TESTS=true
      ;;
    *)
      PATCH_OR_DEFECT=$i
      ;;
    esac
  done
  if [ -z "$PATCH_OR_DEFECT" ]; then
    printUsage
    exit 1
  fi
  if [[ $JENKINS == "true" ]] ; then
    echo "Running in Jenkins mode"
    defect=$PATCH_OR_DEFECT
    ECLIPSE_PROPERTY="-Declipse.home=$ECLIPSE_HOME"
    
    if [[ ! -e "$PATCH_DIR" ]] ; then
      echo ""
      echo "ERROR: No such the patch dir ($PATCH_DIR)"
      echo ""
      cleanupAndExit 0
    fi
  else
    echo "Running in developer mode"
    JENKINS=false
    ### PATCH_FILE contains the location of the patchfile
    PATCH_FILE=$PATCH_OR_DEFECT
    if [[ ! -e "$PATCH_FILE" ]] ; then
      echo "Unable to locate the patch file $PATCH_FILE"
      cleanupAndExit 0
    fi
    ### Check if $PATCH_DIR exists. If it does not exist, create a new directory
    if [[ ! -e "$PATCH_DIR" ]] ; then
      mkdir "$PATCH_DIR"
      if [[ $? == 0 ]] ; then 
        echo "$PATCH_DIR has been created"
      else
        echo "Unable to create $PATCH_DIR"
        cleanupAndExit 0
      fi
    fi
    ### Obtain the patch filename to append it to the version number
    defect=`basename $PATCH_FILE`
  fi
}

###############################################################################
checkout () {
  echo ""
  echo ""
  echo "======================================================================"
  echo "======================================================================"
  echo "    Testing patch for ${defect}."
  echo "======================================================================"
  echo "======================================================================"
  echo ""
  echo ""
  ### When run by a developer, if the workspace contains modifications, do not continue
  ### unless the --dirty-workspace option was set
    
  cd $BASEDIR
  $GIT reset --hard
  $GIT clean -d -f
  $GIT pull ${REMOTE_NAME} ${BRANCH_NAME}
  
  return $?
}

###############################################################################
downloadPatch () {
  echo ""
  echo ""
  echo "======================================================================"
  echo "======================================================================"
  echo "    Downloading patch for ${defect}."
  echo "======================================================================"
  echo "======================================================================"
  echo ""
  echo ""
  ### Download latest patch file (ignoring .htm and .html) when run from patch process
  if [[ $JENKINS == "true" ]] ; then
    $WGET -q -O $PATCH_DIR/jira http://issues.apache.org/jira/browse/$defect
    if [[ `$GREP -c 'Patch Available' $PATCH_DIR/jira` == 0 ]] ; then
      echo "$defect is not \"Patch Available\".  Exiting."
      cleanupAndExit 0
    fi
    relativePatchURL=`$GREP -o '"/jira/secure/attachment/[0-9]*/[^"]*' $PATCH_DIR/jira | $GREP -v -e 'htm[l]*$' | sort | tail -1 | $GREP -o '/jira/secure/attachment/[0-9]*/[^"]*'`
    patchURL="http://issues.apache.org${relativePatchURL}"
    patchNum=`echo $patchURL | $GREP -o '[0-9]*/' | $GREP -o '[0-9]*'`
    echo "$defect patch is being downloaded at `date` from"
    echo "$patchURL"
    $WGET -q -O $PATCH_DIR/patch $patchURL
    GIT_REVISION=`$GIT describe --abbr=7 --always`
    VERSION=${GIT_REVISION}_${defect}_PATCH-${patchNum}
    JIRA_COMMENT="Here are the results of testing the latest attachment 
  $patchURL
  against master revision ${GIT_REVISION}."

    ### Copy in any supporting files needed by this process
    # cp -r $SUPPORT_DIR/lib/* ./lib
    #PENDING: cp -f $SUPPORT_DIR/etc/checkstyle* ./src/test
  ### Copy the patch file to $PATCH_DIR
  else
    VERSION=PATCH-${defect}
    cp $PATCH_FILE $PATCH_DIR/patch
    if [[ $? == 0 ]] ; then
      echo "Patch file $PATCH_FILE copied to $PATCH_DIR"
    else
      echo "Could not copy $PATCH_FILE to $PATCH_DIR"
      cleanupAndExit 0
    fi
  fi
}

###############################################################################
verifyPatch () {
  echo ""
  echo ""
  echo "======================================================================"
  echo "======================================================================"
  echo "    Simulating the application of the downloaded patch"
  echo "======================================================================"
  echo "======================================================================"
  echo ""
  echo ""
  # Before building, check to make sure that the patch is valid
  ${GIT} apply --check -p0 $PATCH_DIR/patch
  if [[ $? != 0 ]] ; then
    ${GIT} apply --check -p1 $PATCH_DIR/patch
    if [[ $? != 0 ]] ; then
      echo "PATCH APPLICATION FAILED"
      JIRA_COMMENT="$JIRA_COMMENT

      {color:red}-1 patch.{color}  The patch command could not apply the patch."
      return -1
    else
      return 1
    fi
  else
    return 0
  fi
}

###############################################################################
preBuildTrunk () {
  echo ""
  echo ""
  echo "======================================================================"
  echo "======================================================================"
  echo " Pre-build master to verify the stability and javac, javadoc warnings" 
  echo "======================================================================"
  echo "======================================================================"
  echo ""
  echo ""
  echo "Compiling $(pwd)"
  echo "$MVN clean test -DskipTests $TAJO_MVN_OPTS > $PATCH_DIR/trunkJavacWarnings.txt 2>&1"
  $MVN clean test -DskipTests $TAJO_MVN_OPTS > $PATCH_DIR/trunkJavacWarnings.txt 2>&1
  if [[ $? != 0 ]] ; then
    echo "Trunk compilation is broken?"
    cleanupAndExit 1
  fi

  echo ""
  echo "Generating Javadocs"
  echo "$MVN test javadoc:javadoc -DskipTests $TAJO_MVN_OPTS > $PATCH_DIR/trunkJavadocsWarnings.txt 2>&1"
  $MVN test javadoc:javadoc -DskipTests $TAJO_MVN_OPTS > $PATCH_DIR/trunkJavadocWarnings.txt 2>&1
}

###############################################################################
buildTrunk () {
  echo ""
  echo ""
  echo "======================================================================"
  echo "======================================================================"
  echo " Build patch to verify the stability and javac, javadoc warnings"
  echo "======================================================================"
  echo "======================================================================"
  echo ""
  echo ""
  echo "Compiling $(pwd)"
  echo "$MVN clean test -DskipTests $TAJO_MVN_OPTS > $PATCH_DIR/trunkJavacWarnings.txt 2>&1"
  $MVN clean test -DskipTests $TAJO_MVN_OPTS > $PATCH_DIR/trunkJavacWarnings.txt 2>&1
  if [[ $? != 0 ]] ; then
    echo "Trunk compilation is broken?"
    cleanupAndExit 1
  fi

  echo ""
  echo "Generating Javadocs"
  echo "$MVN test javadoc:javadoc -DskipTests $TAJO_MVN_OPTS > $PATCH_DIR/trunkJavadocsWarnings.txt 2>&1"
  $MVN test javadoc:javadoc -DskipTests $TAJO_MVN_OPTS > $PATCH_DIR/trunkJavadocWarnings.txt 2>&1
}

###############################################################################
### Check for @author tags in the patch
checkAuthor () {
  echo ""
  echo ""
  echo "======================================================================"
  echo "======================================================================"
  echo "    Checking there are no @author tags in the patch."
  echo "======================================================================"
  echo "======================================================================"
  echo ""
  echo ""
  authorTags=`$GREP -c -i '@author' $PATCH_DIR/patch`
  echo "There appear to be $authorTags @author tags in the patch."
  if [[ $authorTags != 0 ]] ; then
    JIRA_COMMENT="$JIRA_COMMENT

    {color:red}-1 @author.{color}  The patch appears to contain $authorTags @author tags which the Tajo community has agreed to not allow in code contributions."
    return 1
  fi
  JIRA_COMMENT="$JIRA_COMMENT

    {color:green}+1 @author.{color}  The patch does not contain any @author tags."
  return 0
}

###############################################################################
### Check for tests in the patch
checkTests () {
  echo ""
  echo ""
  echo "======================================================================"
  echo "======================================================================"
  echo "    Checking there are new or changed tests in the patch."
  echo "======================================================================"
  echo "======================================================================"
  echo ""
  echo ""
  testReferences=`$GREP -c -i -e '^+++.*/test' $PATCH_DIR/patch`
  echo "There appear to be $testReferences test files referenced in the patch."
  if [[ $testReferences == 0 ]] ; then
    if [[ $JENKINS == "true" ]] ; then
      patchIsDoc=`$GREP -c -i 'title="documentation' $PATCH_DIR/jira`
      if [[ $patchIsDoc != 0 ]] ; then
        echo "The patch appears to be a documentation patch that doesn't require tests."
        JIRA_COMMENT="$JIRA_COMMENT

    +0 tests included.  The patch appears to be a documentation patch that doesn't require tests."
        return 0
      fi
    fi
    JIRA_COMMENT="$JIRA_COMMENT

    {color:red}-1 tests included.{color}  The patch doesn't appear to include any new or modified tests.
                        Please justify why no new tests are needed for this patch.
                        Also please list what manual steps were performed to verify this patch."
    return 1
  fi
  JIRA_COMMENT="$JIRA_COMMENT

    {color:green}+1 tests included.{color}  The patch appears to include $testReferences new or modified test files."
  return 0
}

cleanUpXml () {
  cd $BASEDIR/tajo-dist/src/main/conf
  for file in `ls *.xml.template`
    do
      rm -f `basename $file .template`
    done
  cd $BASEDIR  
}

###############################################################################
### Attempt to apply the patch
applyPatch () {
  echo ""
  echo ""
  echo "======================================================================"
  echo "======================================================================"
  echo "    Applying patch."
  echo "======================================================================"
  echo "======================================================================"
  echo ""
  echo ""
  export PATCH
  ${GIT} apply -p$1 $PATCH_DIR/patch
  if [[ $? != 0 ]] ; then
    echo "PATCH APPLICATION FAILED"
    JIRA_COMMENT="$JIRA_COMMENT

    {color:red}-1 patch.{color}  The patch command could not apply the patch."
    return 1
  fi
  return 0
}

###############################################################################
### Check there are no javadoc warnings
checkJavadocWarnings () {
  echo ""
  echo ""
  echo "======================================================================"
  echo "======================================================================"
  echo "    Determining number of patched javadoc warnings."
  echo "======================================================================"
  echo "======================================================================"
  echo ""
  echo ""
  echo "$MVN test javadoc:javadoc -DskipTests $TAJO_MVN_OPTS > $PATCH_DIR/patchJavadocWarnings.txt 2>&1"
  $MVN test javadoc:javadoc -DskipTests $TAJO_MVN_OPTS > $PATCH_DIR/patchJavadocWarnings.txt 2>&1

  $GREP '\[WARNING\]' $PATCH_DIR/trunkJavadocWarnings.txt | $AWK '/Javadoc Warnings/,EOF' | $GREP warning > $PATCH_DIR/filteredTrunkJavadocWarnings.txt
  $GREP '\[WARNING\]' $PATCH_DIR/patchJavadocWarnings.txt | $AWK '/Javadoc Warnings/,EOF' | $GREP warning > $PATCH_DIR/filteredPatchJavadocWarnings.txt

  trunkJavadocWarnings=`cat $PATCH_DIR/filteredTrunkJavadocWarnings.txt | $AWK 'BEGIN {total = 0} {total += 1} END {print total}'`
  patchJavadocWarnings=`cat $PATCH_DIR/filteredPatchJavadocWarnings.txt | $AWK 'BEGIN {total = 0} {total += 1} END {print total}'`
  echo ""
  echo ""
  echo "There appear to be $trunkJavadocWarnings javadoc warnings before the patch and $patchJavadocWarnings javadoc warnings after applying the patch."

  if [[ $patchJavadocWarnings != "" && $trunkJavadocWarnings != "" ]] ; then
    if [[ $patchJavadocWarnings -gt $trunkJavadocWarnings ]] ; then
    ### if patch warning greater than trunk warning
    JIRA_COMMENT="$JIRA_COMMENT

    {color:red}-1 javadoc.{color}  The applied patch generated $patchJavadocWarnings javadoc warnings (more than the trunk's current $trunkJavadocWarnings warnings)."
    return 1
    fi
  fi
  JIRA_COMMENT="$JIRA_COMMENT

    {color:green}+1 javadoc.{color}  The applied patch does not increase the total number of javadoc warnings."
  return 0
}

###############################################################################
### Check there are no changes in the number of Javac warnings
checkJavacWarnings () {
  echo ""
  echo ""
  echo "======================================================================"
  echo "======================================================================"
  echo "    Determining number of patched javac warnings."
  echo "======================================================================"
  echo "======================================================================"
  echo ""
  echo ""
  echo "$MVN clean test -DskipTests $TAJO_MVN_OPTS > $PATCH_DIR/patchJavacWarnings.txt 2>&1"
  $MVN clean test -DskipTests $TAJO_MVN_OPTS > $PATCH_DIR/patchJavacWarnings.txt 2>&1
  if [[ $? != 0 ]] ; then
    JIRA_COMMENT="$JIRA_COMMENT

    {color:red}-1 javac.{color}  The patch appears to cause the build to fail."
    return 2
  fi
  ### Compare trunk and patch javac warning numbers
  if [[ -f $PATCH_DIR/patchJavacWarnings.txt ]] ; then
    $GREP '\[WARNING\]' $PATCH_DIR/trunkJavacWarnings.txt > $PATCH_DIR/filteredTrunkJavacWarnings.txt
    $GREP '\[WARNING\]' $PATCH_DIR/patchJavacWarnings.txt > $PATCH_DIR/filteredPatchJavacWarnings.txt
    trunkJavacWarnings=`cat $PATCH_DIR/filteredTrunkJavacWarnings.txt | $AWK 'BEGIN {total = 0} {total += 1} END {print total}'`
    patchJavacWarnings=`cat $PATCH_DIR/filteredPatchJavacWarnings.txt | $AWK 'BEGIN {total = 0} {total += 1} END {print total}'`
    echo ""
    echo ""
    echo "There appear to be $trunkJavacWarnings javac compiler warnings before the patch and $patchJavacWarnings javac compiler warnings after applying the patch."
    if [[ $patchJavacWarnings != "" && $trunkJavacWarnings != "" ]] ; then
      if [[ $patchJavacWarnings -gt $trunkJavacWarnings ]] ; then
        JIRA_COMMENT="$JIRA_COMMENT

    {color:red}-1 javac.{color}  The applied patch generated $patchJavacWarnings javac compiler warnings (more than the trunk's current $trunkJavacWarnings warnings)."

    $DIFF $PATCH_DIR/filteredTrunkJavacWarnings.txt $PATCH_DIR/filteredPatchJavacWarnings.txt > $PATCH_DIR/diffJavacWarnings.txt 
        JIRA_COMMENT_FOOTER="Javac warnings: $BUILD_URL/artifact/incubator-tajo/patchprocess/diffJavacWarnings.txt
$JIRA_COMMENT_FOOTER"

        return 1
      fi
    fi
  fi
  JIRA_COMMENT="$JIRA_COMMENT

    {color:green}+1 javac.{color}  The applied patch does not increase the total number of javac compiler warnings."
  return 0
}

###############################################################################
### Check there are no changes in the number of release audit (RAT) warnings
checkReleaseAuditWarnings () {
  echo ""
  echo ""
  echo "======================================================================"
  echo "======================================================================"
  echo "    Determining number of patched release audit warnings."
  echo "======================================================================"
  echo "======================================================================"
  echo ""
  echo ""
  echo "$MVN apache-rat:check > $PATCH_DIR/patchReleaseAuditOutput.txt 2>&1"
  $MVN apache-rat:check > $PATCH_DIR/patchReleaseAuditOutput.txt 2>&1
  find $BASEDIR -name rat.txt | xargs cat > $PATCH_DIR/patchReleaseAuditWarnings.txt

  ### Compare trunk and patch release audit warning numbers
  if [[ -f $PATCH_DIR/patchReleaseAuditWarnings.txt ]] ; then
    patchReleaseAuditWarnings=`$GREP -c '\!?????' $PATCH_DIR/patchReleaseAuditWarnings.txt`
    echo ""
    echo ""
    echo "There appear to be $patchReleaseAuditWarnings release audit warnings after applying the patch."
    if [[ $patchReleaseAuditWarnings != "" ]] ; then
      if [[ $patchReleaseAuditWarnings -gt 0 ]] ; then
        JIRA_COMMENT="$JIRA_COMMENT

    {color:red}-1 release audit.{color}  The applied patch generated $patchReleaseAuditWarnings release audit warnings."
        $GREP '\!?????' $PATCH_DIR/patchReleaseAuditWarnings.txt > $PATCH_DIR/patchReleaseAuditProblems.txt
        echo "Lines that start with ????? in the release audit report indicate files that do not have an Apache license header." >> $PATCH_DIR/patchReleaseAuditProblems.txt
        JIRA_COMMENT_FOOTER="Release audit warnings: $BUILD_URL/artifact/incubator-tajo/patchprocess/patchReleaseAuditProblems.txt
$JIRA_COMMENT_FOOTER"
        return 1
      fi
    fi
  fi
  JIRA_COMMENT="$JIRA_COMMENT

    {color:green}+1 release audit.{color}  The applied patch does not increase the total number of release audit warnings."
  return 0
}

###############################################################################
### Check there are no changes in the number of Checkstyle warnings
checkStyle () {
  echo ""
  echo ""
  echo "======================================================================"
  echo "======================================================================"
  echo "    Determining number of patched checkstyle warnings."
  echo "======================================================================"
  echo "======================================================================"
  echo ""
  echo ""
  echo "$MVN test checkstyle:checkstyle -DskipTests $TAJO_MVN_OPTS > $PATCH_DIR/patchStyleErrors.txt 2>&1"
  $MVN test checkstyle:checkstyle -DskipTests $TAJO_MVN_OPTS > $PATCH_DIR/patchStyleErrors.txt 2>&1

# JIRA_COMMENT_FOOTER="Checkstyle results: $BUILD_URL/artifact/incubator-tajo/build/test/checkstyle-errors.html
# $JIRA_COMMENT_FOOTER"

  ### calculate actual patchStyleErrors
  patchStyleErrors=0
  if [[ -f target/checkstyle-result.xml ]] ; then
    $GREP -i '<error' target/checkstyle-result.xml > $PATCH_DIR/filteredPatchCheckstyleWarnings.txt
    patchStyleErrors=`cat $PATCH_DIR/filteredPatchCheckstyleWarnings.txt | $AWK 'BEGIN {total = 0} {total += 1} END {print total}'`
    echo ""
    echo ""
    echo "There appear to be $patchStyleErrors checkstyle warnings after applying the patch."
  fi

  if [[ $patchStyleErrors != 0 ]] ; then
    JIRA_COMMENT="$JIRA_COMMENT

    {color:red}-1 checkstyle.{color}  The patch generated $patchStyleErrors code style errors."
    return 1
  fi
  JIRA_COMMENT="$JIRA_COMMENT

    {color:green}+1 checkstyle.{color}  The patch generated 0 code style errors."
  return 0
}

###############################################################################
### Install the new jars so tests and findbugs can find all of the updated jars 
buildAndInstall () {
  echo ""
  echo ""
  echo "======================================================================"
  echo "======================================================================"
  echo "    Installing all of the jars"
  echo "======================================================================"
  echo "======================================================================"
  echo ""
  echo ""
  echo "$MVN install -Dmaven.javadoc.skip=true -DskipTests $TAJO_MVN_OPTS"
  $MVN install -Dmaven.javadoc.skip=true -DskipTests $TAJO_MVN_OPTS
  return $?
}


###############################################################################
### Check there are no changes in the number of Findbugs warnings
checkFindbugsWarnings () {
  echo ""
  echo ""
  echo "======================================================================"
  echo "======================================================================"
  echo "    Determining number of patched Findbugs warnings."
  echo "======================================================================"
  echo "======================================================================"
  echo ""
  echo ""
  findbugs_version=`${FINDBUGS_HOME}/bin/findbugs -version`

  modules=$(findModules)
  rc=0
  for module in $modules;
  do
    cd $module
    echo "  Running findbugs in $module"
    module_suffix=`basename ${module}`
    echo "$MVN test findbugs:findbugs -DskipTests $TAJO_MVN_OPTS < /dev/null > $PATCH_DIR/patchFindBugsOutput${module_suffix}.txt 2>&1" 
    $MVN test findbugs:findbugs -DskipTests $TAJO_MVN_OPTS < /dev/null > $PATCH_DIR/patchFindBugsOutput${module_suffix}.txt 2>&1
    (( rc = rc + $? ))
    cd -
  done

  if [ $rc != 0 ] ; then
JIRA_COMMENT_FOOTER="Findbugs results: $BUILD_URL/findbugsResult
$JIRA_COMMENT_FOOTER"

    JIRA_COMMENT="$JIRA_COMMENT

    {color:red}-1 findbugs.{color}  The patch appears to cause Findbugs (version ${findbugs_version}) to fail."
    return 1
  fi
    
  findbugsWarnings=0
  for file in $(find $BASEDIR -name findbugsXml.xml)
  do
    relative_file=${file#$BASEDIR/} # strip leading $BASEDIR prefix
    if [ ! $relative_file == "target/findbugsXml.xml" ]; then
      module_suffix=${relative_file%/target/findbugsXml.xml} # strip trailing path
      module_suffix=`basename ${module_suffix}`
    fi
    
    cp $file $PATCH_DIR/patchFindbugsWarnings${module_suffix}.xml
    $FINDBUGS_HOME/bin/setBugDatabaseInfo -timestamp "01/01/2000" \
      $PATCH_DIR/patchFindbugsWarnings${module_suffix}.xml \
      $PATCH_DIR/patchFindbugsWarnings${module_suffix}.xml
    newFindbugsWarnings=`$FINDBUGS_HOME/bin/filterBugs -first "01/01/2000" $PATCH_DIR/patchFindbugsWarnings${module_suffix}.xml \
      $PATCH_DIR/newPatchFindbugsWarnings${module_suffix}.xml | $AWK '{print $1}'`
    echo "Found $newFindbugsWarnings Findbugs warnings ($file)"
    findbugsWarnings=$((findbugsWarnings+newFindbugsWarnings))
    $FINDBUGS_HOME/bin/convertXmlToText -html \
      $PATCH_DIR/newPatchFindbugsWarnings${module_suffix}.xml \
      $PATCH_DIR/newPatchFindbugsWarnings${module_suffix}.html
    if [[ $newFindbugsWarnings > 0 ]] ; then
      JIRA_COMMENT_FOOTER="Findbugs warnings: $BUILD_URL/artifact/incubator-tajo/patchprocess/newPatchFindbugsWarnings${module_suffix}.html
$JIRA_COMMENT_FOOTER"
    fi
  done

  if [[ $findbugsWarnings -gt 0 ]] ; then
    JIRA_COMMENT="$JIRA_COMMENT

    {color:red}-1 findbugs.{color}  The patch appears to introduce $findbugsWarnings new Findbugs (version ${findbugs_version}) warnings."
    return 1
  fi
  JIRA_COMMENT="$JIRA_COMMENT

    {color:green}+1 findbugs.{color}  The patch does not introduce any new Findbugs (version ${findbugs_version}) warnings."
  return 0
}

###############################################################################
### Run the tests
runTests () {
  echo ""
  echo ""
  echo "======================================================================"
  echo "======================================================================"
  echo "    Running tests."
  echo "======================================================================"
  echo "======================================================================"
  echo ""
  echo ""

  failed_tests=""
  modules=$(findModules)
  for module in $modules;
  do
    cd $module
    echo "  Running tests in $module"
    echo "  $MVN test -fn $TAJO_MVN_OPTS"
    $MVN test -fn $TAJO_MVN_OPTS
    module_failed_tests=`find . -name 'TEST*.xml' | xargs $GREP  -l -E "<failure|<error" | sed -e "s|.*target/surefire-reports/TEST-|                  |g" | sed -e "s|\.xml||g"`
    # With -fn mvn always exits with a 0 exit code.  Because of this we need to
    # find the errors instead of using the exit code.  We assume that if the build
    # failed a -1 is already given for that case
    if [[ -n "$module_failed_tests" ]] ; then
      failed_tests="${failed_tests}
${module_failed_tests}"
    fi
    cd -
  done
  if [[ -n "$failed_tests" ]] ; then
    JIRA_COMMENT="$JIRA_COMMENT

    {color:red}-1 core tests.{color}  The patch failed these unit tests in $modules: $failed_tests"
    return 1
  fi
  JIRA_COMMENT="$JIRA_COMMENT

    {color:green}+1 core tests.{color}  The patch passed unit tests in $modules."
  return 0
}

###############################################################################
# Find the maven module containing the given file.
findModule (){
 dir=`dirname $1`
 while [ 1 ]
 do
  if [ -f "$dir/pom.xml" ]
  then
    echo $dir
    return
  else
    dir=`dirname $dir`
  fi
 done
}

findModules () {
  # Come up with a list of changed files into $TMP
  TMP=/tmp/tmp.paths.$$
  $GREP '^+++\|^---' $PATCH_DIR/patch | cut -c '5-' | $GREP -v /dev/null | sort | uniq > $TMP
  
  # if all of the lines start with a/ or b/, then this is a git patch that
  # was generated without --no-prefix
  if ! $GREP -qv '^a/\|^b/' $TMP ; then
    sed -i -e 's,^[ab]/,,' $TMP
  fi
  
  # Now find all the modules that were changed
  TMP_MODULES=/tmp/tmp.modules.$$
  for file in $(cut -f 1 $TMP | sort | uniq); do
    echo $(findModule $file) >> $TMP_MODULES
  done
  rm $TMP
  
  # Filter out modules without code 
  CHANGED_MODULES=""
  for module in $(cat $TMP_MODULES | sort | uniq); do
    $GREP "<packaging>pom</packaging>" $module/pom.xml > /dev/null
    if [ "$?" != 0 ]; then
      CHANGED_MODULES="$CHANGED_MODULES $module"
    fi
  done
  rm $TMP_MODULES
  echo $CHANGED_MODULES
}

###############################################################################
### Submit a comment to the defect's Jira
submitJiraComment () {
  local result=$1
  ### Do not output the value of JIRA_COMMENT_FOOTER when run by a developer
  if [[  $JENKINS == "false" ]] ; then
    JIRA_COMMENT_FOOTER=""
  fi
  if [[ $result == 0 ]] ; then
    comment="{color:green}*+1 overall.*{color}  $JIRA_COMMENT

$JIRA_COMMENT_FOOTER"
  else
    comment="{color:red}*-1 overall.*{color}  $JIRA_COMMENT

$JIRA_COMMENT_FOOTER"
  fi
  ### Output the test result to the console
  echo "



$comment"  

  if [[ $JENKINS == "true" ]] ; then
    echo ""
    echo ""
    echo "======================================================================"
    echo "======================================================================"
    echo "    Adding comment to Jira."
    echo "======================================================================"
    echo "======================================================================"
    echo ""
    echo ""
    ### Update Jira with a comment
    export USER=hudson
    $JIRACLI -s https://issues.apache.org/jira -a addcomment -u $JIRA_USER -p $JIRA_PASSWD --comment "$comment" --issue $defect
    $JIRACLI -s https://issues.apache.org/jira -a logout -u $JIRA_USER -p $JIRA_PASSWD
  fi
}

###############################################################################
### Cleanup files
cleanupAndExit () {
  local result=$1
  if [[ $JENKINS == "true" ]] ; then
    if [ -e "$PATCH_DIR" ] ; then
      mv $PATCH_DIR $BASEDIR
    fi
  fi
  echo ""
  echo ""
  echo "======================================================================"
  echo "======================================================================"
  echo "    Finished build."
  echo "======================================================================"
  echo "======================================================================"
  echo ""
  echo ""
  exit $result
}

###############################################################################
###############################################################################
###############################################################################

JIRA_COMMENT=""
JIRA_COMMENT_FOOTER="Console output: $BUILD_URL/console

This message is automatically generated."

### Check if arguments to the script have been specified properly or not
parseArgs $@
cd $BASEDIR

checkout
RESULT=$?
if [[ $JENKINS == "true" ]] ; then
  if [[ $RESULT != 0 ]] ; then
    exit 100
  fi
fi
preBuildTrunk
downloadPatch
verifyPatch
PLEVEL=$?
if [[ $PLEVEL == -1  ]] ; then
  submitJiraComment 1
  cleanupAndExit 1
fi
applyPatch $PLEVEL
APPLY_PATCH_RET=$?
(( RESULT = RESULT + $APPLY_PATCH_RET ))
if [[ $APPLY_PATCH_RET != 0 ]] ; then
  submitJiraComment 1
  cleanupAndExit 1
fi
buildTrunk
checkAuthor

if [[ $JENKINS == "true" ]] ; then
  cleanUpXml
fi
checkTests
(( RESULT = RESULT + $? ))
checkJavacWarnings
JAVAC_RET=$?
#2 is returned if the code could not compile
if [[ $JAVAC_RET == 2 ]] ; then
  submitJiraComment 1
  cleanupAndExit 1
fi
(( RESULT = RESULT + $JAVAC_RET ))
checkJavadocWarnings
(( RESULT = RESULT + $? ))
checkStyle
(( RESULT = RESULT + $? ))
checkFindbugsWarnings
(( RESULT = RESULT + $? ))
checkReleaseAuditWarnings
(( RESULT = RESULT + $? ))
buildAndInstall
### Run tests for Jenkins or if explicitly asked for by a developer
if [[ $JENKINS == "true" || $RUN_TESTS == "true" ]] ; then
  runTests
  (( RESULT = RESULT + $? ))
fi
JIRA_COMMENT_FOOTER="Test results: $BUILD_URL/testReport/
$JIRA_COMMENT_FOOTER"

submitJiraComment $RESULT
cleanupAndExit $RESULT
