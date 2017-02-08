#!/bin/sh

rm -fr output.txt

STARTTIME=`date +%s.%N`

spark-submit --class "edu.mum.cs522.spark.LogAnalyzer" --master local[4] spark.jar input/access_log.log >> output.txt

ENDTIME=`date +%s.%N`

TIMEDIFF=`echo "$ENDTIME - $STARTTIME" | bc | awk -F"." '{print $1"."substr($2,1,3)}'`

echo "Execution Time : $TIMEDIFF"
