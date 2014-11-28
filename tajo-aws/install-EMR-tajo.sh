#!/bin/bash

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
#
#
# The EMR Bootstrap for Tajo
#
# Arguments
#
# -t	The tajo binary Tarball URL.
#	ex) s3://tajo-release/tajo-0.9.0/tajo-0.9.0.tar.gz 
#	or 
#	http://apache.claz.org/tajo/tajo-0.9.0/tajo-0.9.0.tar.gz
#
# -c	The tajo conf directory URL.
#	ex) s3://tajo-emr/tajo-0.9.0/ami-3.3.0/m1.medium/conf
#
# -l	The tajo third party lib URL.
#	ex) s3://tajo-emr/tajo-0.9.0/ami-3.3.0/m1.medium/lib
#

TAJO_PACKAGE_URI=
TAJO_CONF_URI=
LIBRARY_URI=
STORAGE=S3
TAJO_MASTER=$(grep -i "yarn.resourcemanager.address<" ${HADOOP_HOME}/etc/hadoop/yarn-site.xml | grep -o '[0-9]\{1,3\}\.[0-9]\{1,3\}\.[0-9]\{1,3\}\.[0-9]\{1,3\}')
NAME_NODE=$TAJO_MASTER
function start_configuration {
   echo "<?xml version=\"1.0\"?>"
   echo "<?xml-stylesheet type=\"text/xsl\" href=\"configuration.xsl\"?>"
   echo "<configuration>"
}
function set_property {
   echo "<property><name>$1</name><value>$2</value></property>"
}
function end_configuration {
   echo "</configuration>"
}

## Get Arguments
while getopts ":t::c::l:" opt;
do
   case $opt in
   t) TAJO_PACKAGE_URI=$OPTARG;;
   c) TAJO_CONF_URI=$OPTARG;;
   l) LIBRARY_URI=$OPTARG;;
   esac
done

## Clean up
rm -rf $HADOOP_HOME/tajo*

## Extract package name
tajo_package_file=$TAJO_PACKAGE_URI
index=1
while [ : ]; do
   index=`expr index "$tajo_package_file" "/"`
   if [  ${index} -gt 0 ]
   then
      tajo_package_file=${tajo_package_file:$index}
   else
      break;
   fi
done

## Download package
if [ `expr "$TAJO_PACKAGE_URI" : http` -gt 0 ]
then
wget -P $HADOOP_HOME $TAJO_PACKAGE_URI
else
$HADOOP_HOME/bin/hadoop dfs -copyToLocal $TAJO_PACKAGE_URI $HADOOP_HOME
fi

## Unpack
cd $HADOOP_HOME
tar -xvf $HADOOP_HOME/$tajo_package_file
ln -s $HADOOP_HOME/${tajo_package_file%.tar*} $HADOOP_HOME/tajo
export TAJO_HOME=$HADOOP_HOME/tajo

## Setting tajo conf
if [ ! -z $TAJO_CONF_URI ]
then
   mkdir $TAJO_HOME/conf/temp
   $HADOOP_HOME/bin/hadoop dfs -copyToLocal ${TAJO_CONF_URI}/* $TAJO_HOME/conf/temp
   mv $TAJO_HOME/conf/temp/* $TAJO_HOME/conf
   chmod 755 $TAJO_HOME/conf/tajo-env.sh
   rm -rf $TAJO_HOME/conf/temp
fi
if [ -f "${TAJO_HOME}/conf/tajo-site.xml" ] 
then
   sed -e 's:</configuration>::g' $TAJO_HOME/conf/tajo-site.xml > $TAJO_HOME/conf/tajo-site.xml.tmp
   mv $TAJO_HOME/conf/tajo-site.xml.tmp $TAJO_HOME/conf/tajo-site.xml
else
   echo $(start_configuration) >> ${TAJO_HOME}/conf/tajo-site.xml
fi
echo $(set_property "tajo.master.umbilical-rpc.address" "${TAJO_MASTER}:26001") >> ${TAJO_HOME}/conf/tajo-site.xml
echo $(set_property "tajo.master.client-rpc.address" "${TAJO_MASTER}:26002") >> ${TAJO_HOME}/conf/tajo-site.xml
echo $(set_property "tajo.resource-tracker.rpc.address" "${TAJO_MASTER}:26003") >> ${TAJO_HOME}/conf/tajo-site.xml
echo $(set_property "tajo.catalog.client-rpc.address" "${TAJO_MASTER}:26005") >> ${TAJO_HOME}/conf/tajo-site.xml
# Default rootdir is EMR hdfs
if [ -z `grep tajo.rootdir ${TAJO_HOME}/conf/tajo-site.xml` ]
then
   STORAGE=local
   echo $(set_property "tajo.rootdir" "hdfs://${NAME_NODE}:9000/tajo") >> ${TAJO_HOME}/conf/tajo-site.xml
fi
echo $(end_configuration) >> ${TAJO_HOME}/conf/tajo-site.xml

## Copy aws-fs(TODO: Find smartly libs) 
cp ${HADOOP_HOME}/.versions/hbase-0.94.18/lib/emr-* ${TAJO_HOME}/lib
cp ${HADOOP_HOME}/.versions/hbase-0.94.18/lib/guice-* ${TAJO_HOME}/lib
cp ${HADOOP_HOME}/.versions/hbase-0.94.18/lib/Emr* ${TAJO_HOME}/lib
cp ${HADOOP_HOME}/.versions/hbase-0.94.18/lib/javax.inject* ${TAJO_HOME}/lib
cp ${HADOOP_HOME}/.versions/hbase-0.94.18/lib/scala-* ${TAJO_HOME}/lib
cp ${HADOOP_HOME}/.versions/hbase-0.94.18/lib/aopalliance-* ${TAJO_HOME}/lib
cp ${HADOOP_HOME}/.versions/hbase-0.94.18/lib/commons-exec* ${TAJO_HOME}/lib
cp ${HADOOP_HOME}/.versions/hbase-0.94.18/lib/jscience-* ${TAJO_HOME}/lib

## Download Third party Library
if [ ! -z $LIBRARY_URI ]
then
   if [ `expr "$LIBRARY_URI" : http` -gt 0 ]
   then
      wget -P ${TAJO_HOME}/lib $LIBRARY_URI
   else
      $HADOOP_HOME/bin/hadoop dfs -copyToLocal ${LIBRARY_URI}/* ${TAJO_HOME}/lib
   fi
fi

## Start tajo
echo '#!/bin/bash' >> ${TAJO_HOME}/start-emr-tajo.sh
echo 'grep -Fq "\"isMaster\": true" /mnt/var/lib/info/instance.json' >> ${TAJO_HOME}/start-emr-tajo.sh
echo 'if [ $? -eq 0 ]; then' >> ${TAJO_HOME}/start-emr-tajo.sh
if [ $STORAGE = "local" ]; then
echo "   nc -z $NAME_NODE 9000" >> ${TAJO_HOME}/start-emr-tajo.sh
echo '   while [ $? -eq 1 ]; do' >> ${TAJO_HOME}/start-emr-tajo.sh
echo "      sleep 5" >> ${TAJO_HOME}/start-emr-tajo.sh
echo "      nc -z $NAME_NODE 9000" >> ${TAJO_HOME}/start-emr-tajo.sh
echo "   done" >> ${TAJO_HOME}/start-emr-tajo.sh
fi
echo "   ${TAJO_HOME}/bin/tajo-daemon.sh start master" >> ${TAJO_HOME}/start-emr-tajo.sh
echo "else" >> ${TAJO_HOME}/start-emr-tajo.sh
echo "   nc -z $TAJO_MASTER 26001" >> ${TAJO_HOME}/start-emr-tajo.sh
echo '   while [ $? -eq 1 ]; do' >> ${TAJO_HOME}/start-emr-tajo.sh
echo "      sleep 5" >> ${TAJO_HOME}/start-emr-tajo.sh
echo "      nc -z $TAJO_MASTER 26001" >> ${TAJO_HOME}/start-emr-tajo.sh
echo "   done" >> ${TAJO_HOME}/start-emr-tajo.sh
echo "   ${TAJO_HOME}/bin/tajo-daemon.sh start worker" >> ${TAJO_HOME}/start-emr-tajo.sh
echo "fi" >> ${TAJO_HOME}/start-emr-tajo.sh
chmod 755 ${TAJO_HOME}/start-emr-tajo.sh
${TAJO_HOME}/start-emr-tajo.sh &