#!/bin/bash
mkdir bigram_classes
mkdir rank_classes

hadoop fs -mkdir bigram
hadoop fs -mkdir bigram/input
hadoop fs -put bible+shakes.nopunc bigram/input

hadoop fs -mkdir rank

javac -classpath ${HADOOP_HOME}/share/hadoop/common/hadoop-common-2.3.0-cdh5.1.2.jar:${HADOOP_HOME}/share/hadoop/mapreduce/hadoop-mapreduce-client-core-2.3.0-cdh5.1.2.jar -d bigram_classes Bigram.java
jar cvf bigram.jar -C bigram_classes org
hadoop jar bigram.jar org.myorg.Bigram bigram/input bigram/output
hadoop fs -cat bigram/output/part-00000 > bigram.txt

javac -classpath ${HADOOP_HOME}/share/hadoop/common/hadoop-common-2.3.0-cdh5.1.2.jar:${HADOOP_HOME}/share/hadoop/mapreduce/hadoop-mapreduce-client-core-2.3.0-cdh5.1.2.jar -d rank_classes Rank.java
jar cvf rank.jar -C rank_classes org
hadoop jar rank.jar org.myorg.Rank bigram/output rank/output
hadoop fs -cat rank/output/part-00000 > rank.txt

hadoop fs -rm -r -f bigram/output
hadoop fs -rm -r -f rank/output
