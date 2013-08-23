#!/bin/sh
echo "add jar to classpath"
export HADOOP_CLASSPATH=~/test/ch2/styhadoop-ch2-1.0.0-SNAPSHOT.jar
#echo "MaxTemperature"
#~/hadoop/bin/hadoop com.sweetop.styhadoop.MaxTemperature   input/  output/

echo "MaxTemperatureWithCompression"
~/hadoop/bin/hadoop com.sweetop.styhadoop.MaxTemperatureWithCompression   input/data.gz  output/
