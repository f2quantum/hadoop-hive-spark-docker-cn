# Hadoop-Hive-Spark cluster + Jupyter on Docker

## Software

* [Hadoop 3.3.4](https://hadoop.apache.org/)

* [Hive 3.1.3](http://hive.apache.org/)

* [Spark 3.3.1](https://spark.apache.org/)

## Quick Start

To deploy the cluster, run:
```
make
docker-compose up
```

## Access interfaces with the following URL

### Hadoop

ResourceManager: http://localhost:8088

NameNode: http://localhost:9870

HistoryServer: http://localhost:19888

Datanode1: http://localhost:9864
Datanode2: http://localhost:9865

NodeManager1: http://localhost:8042
NodeManager2: http://localhost:8043

### Spark
master: http://localhost:8080

worker1: http://localhost:8081
worker2: http://localhost:8082

history: http://localhost:18080

### Hive
URI: jdbc:hive2://localhost:10000

- jupyter
- jupyter
### Jupyter Notebook
URL: http://localhost:8888?token=yiyishiwushiyiyijiuyijiubayiling

如果出现文件权限问题，将文件夹的权限转交docker（常见于非root用于运行docker）：

``` bash
sudo chown -R 1000.1000 ./jupyter/notebook
```

example: [jupyter/notebook/pyspark.ipynb](jupyter/notebook/pyspark.ipynb)


## If you need a WebUI view in a remote server

### Docker + VNC

``` bash 
docker run -d -p 3389:3389 -p 5901:5901 -p 6901:6901 --name ubuntu-xfce-vnc --net host soff/ubuntu-xfce-vnc
```
- 浏览器，直接访问：http://ip:6901，密码 vncpassword
- VNC：ip:5901，密码 vncpassword
- RDP：ip，用户名 user 密码 password
