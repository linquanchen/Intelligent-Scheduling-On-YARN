#!/bin/bash

username="kewu"

# On the master, stop the node manager
./mpssh -u $username -s -f masterip "\$HADOOP_HOME/sbin/yarn-daemon.sh  --config \$HADOOP_CONF_DIR/amnode stop nodemanager"

# On all the slaves
./mpssh -u $username -s -f slavevmips "\$HADOOP_YARN_HOME/sbin/yarn-daemon.sh --config \$HADOOP_CONF_DIR stop nodemanager"
./mpssh -u $username -s -f slavevmips "\$HADOOP_PREFIX/sbin/hadoop-daemon.sh --config \$HADOOP_CONF_DIR --script hdfs stop datanode"

# On master
./mpssh -u $username -s -f masterip "\$HADOOP_PREFIX/sbin/mr-jobhistory-daemon.sh stop historyserver --config \$HADOOP_CONF_DIR"
./mpssh -u $username -s -f masterip "\$HADOOP_YARN_HOME/sbin/yarn-daemon.sh stop proxyserver --config \$HADOOP_CONF_DIR"
./mpssh -u $username -s -f masterip "\$HADOOP_YARN_HOME/sbin/yarn-daemon.sh --config \$HADOOP_CONF_DIR stop resourcemanager"
./mpssh -u $username -s -f masterip "\$HADOOP_PREFIX/sbin/hadoop-daemon.sh --config \$HADOOP_CONF_DIR --script hdfs stop namenode"







