#!/bin/bash

# run master
/Users/kimdohoon/app/spark/spark-3.2.4-bin-hadoop3.2/sbin/start-master.sh

# run worker
/Users/kimdohoon/app/spark/spark-3.2.4-bin-hadoop3.2/sbin/start-worker.sh \
spark://neivekim76.local:7077 \
-m 1g