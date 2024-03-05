#!/bin/bash

echo "Starting jupyter notebook..."
# CMD在docker中必须前台一直启动才代表其启动
# while true
# do
#   sleep 3
#   echo "go"
#   # loop infinitely
# done
jupyter notebook --ip=0.0.0.0 --port=8888 --no-browser --NotebookApp.token='yiyishiwushiyiyijiuyijiubayiling' --notebook-dir=/home/jupyter/notebook