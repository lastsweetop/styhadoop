#!/bin/sh
echo "deploy jar"
rsync -avlH  ../target/styhadoop-ch4-1.0.0-SNAPSHOT.jar hadoop@namenode:~/test/ch4/
echo "deploy run.sh"
rsync -avlH  run.sh hadoop@namenode:~/test/ch4/
echo "change authority"
ssh hadoop@namenode "chmod 755 ~/test/ch4/run.sh"
echo "start run.sh"
ssh hadoop@namenode "~/test/ch4/run.sh"