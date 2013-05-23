#!/bin/sh
echo "add jar to classpath"
export HADOOP_CLASSPATH=~/test/styhadoop-ch2-1.0.0-SNAPSHOT.jar
echo "run hadoop task"
~/hadoop/bin/hadoop com.sweetop.styhadoop.MaxTemperature   input/  output/