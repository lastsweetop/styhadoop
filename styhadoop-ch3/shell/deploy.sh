#!/bin/sh
echo "deploy jar"
rsync -avlH  ../target/styhadoop-ch3-1.0.0-SNAPSHOT.jar hadoop@namenode:~/test/ch3/
echo "deploy run.sh"
rsync -avlH  run.sh hadoop@namenode:~/test/ch3/
echo "change authority"
ssh hadoop@namenode "chmod 755 ~/test/ch3/run.sh"
echo "start run.sh"
ssh hadoop@namenode "~/test/ch3/run.sh"