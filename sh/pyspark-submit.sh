#!/bin/bash

SPARK_FILE="$1"

echo "$SPARK_FILE"

/Users/kimdohoon/app/spark/spark-3.2.4-bin-hadoop3.2/bin/spark-submit \
--master spark://neivekim76.local:7077 \
--executor-memory 512m \
--total-executor-cores 2 \
$SPARK_FILE
