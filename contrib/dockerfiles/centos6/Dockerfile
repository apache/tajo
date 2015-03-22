#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

FROM centos:centos6
MAINTAINER Tajo Community <dev@tajo.apache.org>

RUN yum update -y && yum install -y which tar openssh-server openssh-clients

RUN ssh-keygen -q -N "" -t rsa -f /root/.ssh/id_rsa
RUN cp /root/.ssh/id_rsa.pub /root/.ssh/authorized_keys

RUN sed 's@session\s*required\s*pam_loginuid.so@session optional pam_loginuid.so@g' -i /etc/pam.d/sshd

RUN echo 'StrictHostKeyChecking no' > /root/.ssh/config
RUN chmod 400 /root/.ssh/config
RUN echo -e '#!/bin/bash\nservice sshd start\nwhile true; do sleep 1000; done' > /root/start.sh
RUN chmod +x /root/start.sh

# java
RUN \
  curl -LO 'http://download.oracle.com/otn-pub/java/jdk/7u75-b13/jdk-7u75-linux-x64.rpm' -H 'Cookie: gpw_e24=http%3A%2F%2Fwww.oracle.com%2F; oraclelicense=accept-securebackup-cookie' && \
  rpm -i jdk-7u75-linux-x64.rpm && \
  rm jdk-7u75-linux-x64.rpm

ENV JAVA_HOME /usr/java/default
ENV PATH $PATH:$JAVA_HOME/bin
RUN echo 'JAVA_HOME="/usr/java/default"' >> /etc/environment

# hadoop
RUN \
  cd /usr/local && curl -LO http://www.apache.org/dist/hadoop/common/hadoop-2.6.0/hadoop-2.6.0.tar.gz && \
  tar -xzf hadoop-2.6.0.tar.gz && \
  ln -s ./hadoop-2.6.0 hadoop && \
  rm hadoop-2.6.0.tar.gz

ENV HADOOP_PREFIX /usr/local/hadoop
ENV YARN_CONF_DIR /usr/local/hadoop/etc/hadoop
RUN \
  echo 'HADOOP_PREFIX="/usr/local/hadoop"' >> /etc/environment && \
  echo 'HADOOP_COMMON_HOME="/usr/local/hadoop"' >> /etc/environment && \
  echo 'HADOOP_HDFS_HOME="/usr/local/hadoop"' >> /etc/environment && \
  echo 'HADOOP_MAPRED_HOME="/usr/local/hadoop"' >> /etc/environment && \
  echo 'HADOOP_YARN_HOME="/usr/local/hadoop"' >> /etc/environment && \
  echo 'HADOOP_CONF_DIR="/usr/local/hadoop/etc/hadoop"' >> /etc/environment && \
  echo 'YARN_CONF_DIR="/usr/local/hadoop/etc/hadoop"' >> /etc/environment

RUN sed -i '/^export JAVA_HOME/ s:.*:export JAVA_HOME=/usr/java/default\nexport HADOOP_PREFIX=/usr/local/hadoop\nexport HADOOP_HOME=/usr/local/hadoop\n:' $HADOOP_PREFIX/etc/hadoop/hadoop-env.sh
RUN sed -i '/^export HADOOP_CONF_DIR/ s:.*:export HADOOP_CONF_DIR=/usr/local/hadoop/etc/hadoop/:' $HADOOP_PREFIX/etc/hadoop/hadoop-env.sh

RUN echo -e '<configuration>\n<property>\n<name>fs.defaultFS</name>\n<value>hdfs://hnn-001-01:9000</value>\n</property>\n</configuration>' > $HADOOP_PREFIX/etc/hadoop/core-site.xml
RUN echo -e '<configuration>\n<property>\n<name>dfs.replication</name>\n<value>1</value>\n</property>\n</configuration>' > $HADOOP_PREFIX/etc/hadoop/hdfs-site.xml
RUN echo -e '<configuration>\n<property>\n<name>mapreduce.framework.name</name>\n<value>yarn</value>\n</property>\n</configuration>' > $HADOOP_PREFIX/etc/hadoop/mapred-site.xml
COPY yarn-site.xml $HADOOP_PREFIX/etc/hadoop/

RUN chmod +x /usr/local/hadoop/etc/hadoop/*-env.sh
COPY init-nn.sh /root/
RUN chmod +x /root/init-nn.sh

# tajo
RUN \
  cd /usr/local && curl -LO http://www.us.apache.org/dist/tajo/tajo-0.10.0/tajo-0.10.0.tar.gz && \
  tar -xzf tajo-0.10.0.tar.gz && \
  ln -s tajo-0.10.0 tajo && \
  rm tajo-0.10.0.tar.gz

ENV TAJO_HOME /usr/local/tajo
ENV HADOOP_HOME $HADOOP_PREFIX
ENV PATH $PATH:$HADOOP_PREFIX/bin:$TAJO_HOME/bin
RUN \
  echo 'TAJO_HOME="/usr/local/tajo"' >> /etc/environment && \
  echo 'HADOOP_HOME="/usr/local/hadoop"' >> /etc/environment

COPY tajo-site.xml $TAJO_HOME/conf/
COPY init-tajo.sh /root/
RUN chmod +x /root/init-tajo.sh
RUN echo -e '1|abc|1.1|a\n2|def|2.3|b\n3|ghi|3.4|c\n4|jkl|4.5|d\n5|mno|5.6|e' > /root/data.csv
COPY test-tajo.sh /root/
RUN chmod +x /root/test-tajo.sh

EXPOSE 22 8020 8030 8031 8032 8033 8040 8042 8088 10000 26080 26081 26880 50010 50020 50060 50070 50075 50090
