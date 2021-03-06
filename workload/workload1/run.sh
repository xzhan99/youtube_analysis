#!/bin/bash

if [ $# -ne 2 ]; then
    echo "Invalid number of parameters!"
    echo "Usage: ./run.sh [input_location] [output_location]"
    exit 1
fi

hadoop jar /usr/lib/hadoop/hadoop-streaming-2.8.5-amzn-1.jar \
-D mapreduce.job.maps=3 \
-D mapreduce.job.reduces=1 \
-D mapreduce.job.name='Task1' \
-file mapper.py \
-file combiner.py \
-file reducer.py \
-mapper mapper.py \
-combiner combiner.py \
-reducer reducer.py \
-input $1 \
-output $2
