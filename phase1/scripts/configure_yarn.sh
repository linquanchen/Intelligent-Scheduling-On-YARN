#!/bin/bash

#################################################################
# Before run the shell, add a file called masterip contains the #
# master VM ip; modify the file called slavevmips contains all  #
# ips of slaves.                                                #
#################################################################

username="kewu" 

# Mount
./mpssh -u $username -s -f vmips "mount -a"

# set up environment variables in ~/.bashrc
./mpssh -u $username -s -f vmips "echo \"export HADOOP_HOME=/opt/projects/advcc/hadoop/hadoop-2.2.0\" >> ~/.bashrc"
./mpssh -u $username -s -f vmips "echo \"export HADOOP_CONF_DIR=\${HADOOP_HOME}/etc/hadoop\" >> ~/.bashrc"
./mpssh -u $username -s -f vmips "echo \"export HADOOP_PREFIX=\$HADOOP_HOME\" >> ~/.bashrc"
./mpssh -u $username -s -f vmips "echo \"export HADOOP_YARN_HOME=\$HADOOP_HOME\" >> ~/.bashrc"
./mpssh -u $username -s -f vmips "echo \"export JAVA_HOME=/usr/lib/jvm/java-6-openjdk-amd64\" >> ~/.bashrc"
./mpssh -u $username -s -f vmips "source ~/.bashrc"

# Run modified setup-scratch-user.sh on all 25 VMs to create a scratch space
./mpssh -u $username -s -f vmips "/opt/projects/advcc/hadoop/scripts/setup-scratch-user.sh" 

# On master
./mpssh -u $username -s -f masterip "\$HADOOP_PREFIX/bin/hdfs namenode -format Phase1Team10"
./mpssh -u $username -s -f masterip "\$HADOOP_PREFIX/sbin/hadoop-daemon.sh --config \$HADOOP_CONF_DIR --script hdfs start namenode"
./mpssh -u $username -s -f masterip "\$HADOOP_YARN_HOME/sbin/yarn-daemon.sh --config \$HADOOP_CONF_DIR start resourcemanager"
./mpssh -u $username -s -f masterip "\$HADOOP_YARN_HOME/sbin/yarn-daemon.sh start proxyserver --config \$HADOOP_CONF_DIR"
./mpssh -u $username -s -f masterip "\$HADOOP_PREFIX/sbin/mr-jobhistory-daemon.sh start historyserver --config \$HADOOP_CONF_DIR"

# On all the slaves
./mpssh -u $username -s -f slavevmips "\$HADOOP_PREFIX/sbin/hadoop-daemon.sh --config \$HADOOP_CONF_DIR --script hdfs start datanode"
./mpssh -u $username -s -f slavevmips "\$HADOOP_YARN_HOME/sbin/yarn-daemon.sh --config \$HADOOP_CONF_DIR start nodemanager"

# On the master, start the node manager
./mpssh -u $username -s -f masterip "\$HADOOP_HOME/sbin/yarn-daemon.sh  --config \$HADOOP_CONF_DIR/amnode start nodemanager"
