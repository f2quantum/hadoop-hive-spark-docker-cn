#!/bin/bash

echo "Starting Hadoop data node..."
hdfs --daemon start datanode

echo "Starting Hadoop node manager..."
yarn --daemon start nodemanager

echo "Starting Spark slave node..."
spark-class org.apache.spark.deploy.worker.Worker "spark://master:7077"
# 使用yarn模式不启动spark worker
# while true
# do
#   sleep 10
#   # loop infinitely
# done