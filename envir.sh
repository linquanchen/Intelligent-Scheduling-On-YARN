#!/bin/bash

# set up environment variables in ~/.bashrc
./mpssh -s -f vmips "echo \"export HADOOP_HOME=/opt/projects/advcc/hadoop/hadoop-2.2.0\" >> ~/.bashrc"
./mpssh -s -f vmips "echo \"export HADOOP_CONF_DIR=${HADOOP_HOME}/etc/hadoop\" >> ~/.bashrc"
./mpssh -s -f vmips "echo \"export HADOOP_PREFIX=$HADOOP_HOME\" >> ~/.bashrc"
./mpssh -s -f vmips "echo \"export HADOOP_YARN_HOME=$HADOOP_HOME\" >> ~/.bashrc"
./mpssh -s -f vmips "echo \"export JAVA_HOME=/usr/lib/jvm/java-6-openjdk-amd64\" >> ~/.bashrc"
./mpssh -s -f vmips "source ~/.bashrc"


# On all the slaves
#./mpssh -s -f slavevmips "$HADOOP_PREFIX/sbin/hadoop-daemon.sh --config $HADOOP-CONF_DIR --script hdfs start datanode"
#./mpssh -s -f slavevmips "$HADOOP_YARN_HOME/sbin/yarn-daemon.sh --config $HADOOP_CONF_DIR start nodemanager"
#
## On master, format HDFS on namenode (r0h0) and start namenode
#$HADOOP_PREFIX/bin/hdfs namenode -format Phase1Team10
#$HADOOP_PREFIX/sbin/hadoop-daemon.sh --config $HADOOP_CONF_DIR --script hdfs start namenode
#$HADOOP_YARN_HOME/sbin/yarn-daemon.sh --config $HADOOP_CONF_DIR start resourcemanager
#$HADOOP_YARN_HOME/sbin/yarn-daemon.sh start proxyserver --config $HADOOP_CONF_DIR
#$HADOOP_PREFIX/sbin/mr-jobhistory-daemon.sh start historyserver --config $HADOOP_CONF_DIR
