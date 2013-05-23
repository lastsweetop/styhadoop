#!/bin/sh
echo "deploy jar"
scp ../target/styhadoop-ch2-1.0.0-SNAPSHOT.jar hadoop@namenode:~/test/
echo "deploy run.sh"
scp run.sh hadoop@namenode:~/test/
echo "change authority"
ssh hadoop@namenode "chmod 755 ~/test/run.sh"
echo "start run.sh"
ssh hadoop@namenode "~/test/run.sh"