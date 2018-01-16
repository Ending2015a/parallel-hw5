#!/bin/bash

# Do not uncomment these lines to directly execute the script
# Modify the path to fit your need before using this script
#hdfs dfs -rm -r /user/TA/CalculateAverage/Output/
#hadoop jar CalculateAverage.jar calculateAverage.CalculateAverage /user/shared/CalculateAverage/Input /user/TA/CalculateAverage/Output
#hdfs dfs -cat /user/TA/CalculateAverage/Output/part-*

ROOT_FILE=PageRank
INPUT_FILE=/user/ta/PageRank/Input/input-100M
OUTPUT_FILE=PageRank/Output
JAR=PageRank.jar

if [[ "$1" == "" ]]; then
    INPUT_FILE=/user/ta/PageRank/Input/input-100M
else
    INPUT_FILE=$1
fi

if [[ "$2" == "" ]]; then
    OUTPUT_FILE=PageRank/Output
else
    OUTPUT_FILE=$2
fi

if [[ "$3" == "" ]]; then
	iter=-1
else
	iter=$3
fi

if [[ "$4" == "" ]]; then
    reducer=16
else
    reducer=$4
fi

echo "input: ${INPUT_FILE}, output: ${OUTPUT_FILE}, iter: ${iter}, reducer: ${reducer}"

hdfs dfs -rm -r $ROOT_FILE
hdfs dfs -rm -r $OUTPUT_FILE
hadoop jar $JAR page_rank.PageRank $ROOT_FILE $INPUT_FILE $OUTPUT_FILE $iter $reducer
hdfs dfs -getmerge $OUTPUT_FILE 103062372_${INPUT_FILE##*-}.out
