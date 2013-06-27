#!/bin/sh
echo "add jar to classpath"
export HADOOP_CLASSPATH=~/test/ch4/styhadoop-ch4-1.0.0-SNAPSHOT.jar
echo "============================================"
#echo "StreamCompressor"
#echo "Hello lastsweetop" | ~/hadoop/bin/hadoop com.sweetop.styhadoop.StreamCompressor  org.apache.hadoop.io.compress.GzipCodec | gunzip -
#echo "FileDecompressor"
#~/hadoop/bin/hadoop com.sweetop.styhadoop.FileDecompressor   news.gz
echo "PooledStreamCompressor"
echo "Hello lastsweetop" | ~/hadoop/bin/hadoop com.sweetop.styhadoop.PooledStreamCompressor  org.apache.hadoop.io.compress.GzipCodec | gunzip -
