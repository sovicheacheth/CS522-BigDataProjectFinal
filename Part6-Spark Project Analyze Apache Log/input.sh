#!/bin/sh

hadoop fs -rm -r input
hadoop fs -mkdir input 2>&1
hadoop fs -put ./access_log.log input/

