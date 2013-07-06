#!/bin/bash
#./maketestfile.sh
#sbt assembly
# start the server. the -d is to prevent reading from stdin
java -jar target/scala-2.10/iobench.jar test 127.0.0.1 2222 $@
