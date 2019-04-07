#!/bin/bash

spark-submit \
    --master local[4] \
    spark_scripts.py \
    --input file:///home/hadoop/task2/AllVideos_short.csv \
    --output file:///home/hadoop/task2/output