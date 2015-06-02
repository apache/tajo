#!/bin/sh
TAJO_VERSION=$1
RELEASE_HOME=`dirname "$0"`
RELEASE_HOME=`cd "$RELEASE_HOME"; pwd`
#Verify SOURCE 

function verifyMD5 {
  echo "Verify MD5 " $1
  MD5=`openssl md5 $1`
  
  if [ "$MD5" = "`cat $1.md5`" ] ; then
    echo 'MD5 OK'
  else
    echo 'MD5 failed ' $MD5", " `cat $1.md5`
    exit -1
  fi
}

function verifySHA512 {
  echo "Verify SHA512 " $1
  SHA512=`openssl sha512 $1`

  if [ "$SHA512" = "`cat $1.sha512`" ] ; then
    echo 'SHA512 OK'
  else
    echo 'SHA512 failed ' $SHA512", " `cat $1.sha512`
    exit -1
  fi
}

function verifyPGP {
  echo "Verify PGP SIGNATURE " $1
  echo=`gpg --verify $1.asc $1`
}

SOURCE_TARBALL=./tajo-$TAJO_VERSION-src.tar.gz
BINARY_TARBALL=./tajo-$TAJO_VERSION.tar.gz
JDBC_JAR=./tajo-jdbc-$TAJO_VERSION.jar

for i in $SOURCE_TARBALL $BINARY_TARBALL $JDBC_JAR
do
  echo "========================================"
  echo "Verify " $i
  if [ ! -f $i ]; then
    echo "$i must be a valid file";
    exit 4;
  fi
  echo "========================================"
  verifyPGP $i
  verifyMD5 $i
  verifySHA512 $i
  echo "========================================"

done

