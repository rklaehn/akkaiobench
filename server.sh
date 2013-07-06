#!/bin/bash
nohup netcat -d -l -n -p 2222 > test.stored &
