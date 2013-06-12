#!/bin/sh
echo "add jar to classpath"
export HADOOP_CLASSPATH=~/test/ch3/styhadoop-ch3-1.0.0-SNAPSHOT.jar
echo "============================================"
#echo "URLcat news.txt"
#~/hadoop/bin/hadoop com.sweetop.styhadoop.URLCat   hdfs://namenode:9000/user/hadoop/news.txt
#echo "FileSystemCat news.txt"
#~/hadoop/bin/hadoop com.sweetop.styhadoop.FileSystemCat   hdfs://namenode:9000/user/hadoop/news.txt
#echo "FileSystemDoubleCat news.txt"
#~/hadoop/bin/hadoop com.sweetop.styhadoop.FileSystemDoubleCat   hdfs://namenode:9000/user/hadoop/news.txt
#echo "FileCopyWithProgress news.txt hdfsnews.txt"
#~/hadoop/bin/hadoop com.sweetop.styhadoop.FileCopyWithProgress /home/hadoop/test/news.txt  hdfs://namenode:9000/user/hadoop/news.txt
#echo "ShowFileStatus news.txt"
#~/hadoop/bin/hadoop com.sweetop.styhadoop.ShowFileStatus hdfs://namenode:9000/user/hadoop/news.txt
#echo "ListStatus"
#~/hadoop/bin/hadoop com.sweetop.styhadoop.ListStatus hdfs://namenode:9000/ hdfs://namenode:9000/user/hadoop/
echo "GlobStatus"
~/hadoop/bin/hadoop com.sweetop.styhadoop.GlobStatus hdfs://namenode:9000/user/hadoop/input/*