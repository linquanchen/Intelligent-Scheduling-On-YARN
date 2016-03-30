#!/bin/bash

#################################################################
# Before run the shell, add a file called masterip contains the #
# master VM ip; modify the file called slavevmips contains all  #
# ips of slaves.                                                #
#################################################################



# Mount
./mpssh -u kewu -s -f vmips "mount -a"

# set up environment variables in ~/.bashrc
./mpssh -u kewu -s -f vmips "echo \"export HADOOP_HOME=/opt/projects/advcc/hadoop/hadoop-2.2.0\" >> ~/.bashrc"
./mpssh -u kewu -s -f vmips "echo \"export HADOOP_CONF_DIR=\${HADOOP_HOME}/etc/hadoop\" >> ~/.bashrc"
./mpssh -u kewu -s -f vmips "echo \"export HADOOP_PREFIX=\$HADOOP_HOME\" >> ~/.bashrc"
./mpssh -u kewu -s -f vmips "echo \"export HADOOP_YARN_HOME=\$HADOOP_HOME\" >> ~/.bashrc"
./mpssh -u kewu -s -f vmips "echo \"export JAVA_HOME=/usr/lib/jvm/java-6-openjdk-amd64\" >> ~/.bashrc"
./mpssh -u kewu -s -f vmips "source ~/.bashrc"

# Run modified setup-scratch-user.sh on all 25 VMs to create a scratch space
./mpssh -u kewu -s -f vmips "/opt/projects/advcc/hadoop/scripts/setup-scratch-user.sh" 

# On master
./mpssh -u kewu -s -f masterip "\$HADOOP_PREFIX/bin/hdfs namenode -format Phase1Team10"
./mpssh -u kewu -s -f masterip "\$HADOOP_PREFIX/sbin/hadoop-daemon.sh --config \$HADOOP_CONF_DIR --script hdfs start namenode"
./mpssh -u kewu -s -f masterip "\$HADOOP_YARN_HOME/sbin/yarn-daemon.sh --config \$HADOOP_CONF_DIR start resourcemanager"
./mpssh -u kewu -s -f masterip "\$HADOOP_YARN_HOME/sbin/yarn-daemon.sh start proxyserver --config \$HADOOP_CONF_DIR"
./mpssh -u kewu -s -f masterip "\$HADOOP_PREFIX/sbin/mr-jobhistory-daemon.sh start historyserver --config \$HADOOP_CONF_DIR"

# On all the slaves
./mpssh -u kewu -s -f slavevmips "\$HADOOP_PREFIX/sbin/hadoop-daemon.sh --config \$HADOOP_CONF_DIR --script hdfs start datanode"
./mpssh -u kewu -s -f slavevmips "\$HADOOP_YARN_HOME/sbin/yarn-daemon.sh --config \$HADOOP_CONF_DIR start nodemanager"

# On the master, start the node manager
./mpssh -u kewu -s -f masterip "\$HADOOP_HOME/sbin/yarn-daemon.sh  --config \$HADOOP_CONF_DIR/amnode start nodemanager"
