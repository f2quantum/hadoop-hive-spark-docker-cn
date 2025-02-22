FROM tensorflow/tensorflow:2.13.0
# aliyun mirror
RUN  sed -i 's@//.*archive.ubuntu.com@//mirrors.ustc.edu.cn@g' /etc/apt/sources.list


RUN apt-get update  -y \
 && DEBIAN_FRONTEND=noninteractive apt-get install --no-install-recommends \
      sudo \
      openjdk-8-jdk \
      curl \
      gnupg \
      procps \
      python3.8 \
      python3-pip \
      python-is-python3 \
      coreutils \
      libc6-dev \
      wget -y\
 && rm -rf /var/lib/apt/lists/*

ARG USERNAME=jupyter
ARG GROUPNAME=jupyter
ARG UID=1001
ARG GID=1001

RUN echo $USERNAME ALL=\(root\) NOPASSWD:ALL > /etc/sudoers.d/$USERNAME \
 && chmod 0440 /etc/sudoers.d/$USERNAME \
 && groupadd -g $GID $GROUPNAME \
 && useradd -m -s /bin/bash -u $UID -g $GID $USERNAME

USER $USERNAME

ENV JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64/

# Hadoop
ARG HADOOP_VERSION=3.3.6
ARG HADOOP_URL=https://mirrors.huaweicloud.com/apache/hadoop/common/hadoop-$HADOOP_VERSION/hadoop-$HADOOP_VERSION.tar.gz
ENV HADOOP_HOME=/opt/hadoop

# && curl -fsSL https://dlcdn.apache.org/hadoop/common/KEYS -o /tmp/hadoop-KEYS \
# && gpg --import /tmp/hadoop-KEYS \
# && curl -fsSL $HADOOP_URL -o /tmp/hadoop.tar.gz \
# && curl -fsSL $HADOOP_URL.asc -o /tmp/hadoop.tar.gz.asc \
# && gpg --verify /tmp/hadoop.tar.gz.asc \
RUN set -x \
 && sudo mkdir $HADOOP_HOME  \
 && sudo chown $USERNAME:$GROUPNAME -R $HADOOP_HOME \
 && wget -P /tmp $HADOOP_URL \
 && mv /tmp/hadoop-$HADOOP_VERSION.tar.gz /tmp/hadoop.tar.gz \
 && tar -xf /tmp/hadoop.tar.gz -C $HADOOP_HOME --strip-components 1 \
 && mkdir $HADOOP_HOME/logs \
 && rm /tmp/hadoop*

ENV HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
ENV PATH=$HADOOP_HOME/sbin:$HADOOP_HOME/bin:$PATH
ENV LD_LIBRARY_PATH=$HADOOP_HOME/lib/native:$LD_LIBRARY_PATH

# Spark
ARG SPARK_VERSION=3.3.3
ARG SPARK_URL=https://mirrors.huaweicloud.com/apache/spark/spark-$SPARK_VERSION/spark-$SPARK_VERSION-bin-hadoop3.tgz
ENV SPARK_HOME=/opt/spark

RUN set -x \
 && sudo mkdir $SPARK_HOME \
 && sudo chown $USERNAME:$GROUPNAME -R $SPARK_HOME \
 && wget -P /tmp $SPARK_URL \
 && mv /tmp/spark-$SPARK_VERSION-bin-hadoop3.tgz  /tmp/spark.tgz\
 && tar -xf /tmp/spark.tgz -C $SPARK_HOME --strip-components 1 \
 && rm /tmp/spark* \
 && wget -P $SPARK_HOME/jars https://jdbc.postgresql.org/download/postgresql-42.3.2.jar 

ENV PYTHONHASHSEED=1
ENV PYSPARK_PYTHON=python3
ENV SPARK_CONF_DIR=$SPARK_HOME/conf
ENV PATH=$SPARK_HOME/sbin:$SPARK_HOME/bin:$PATH

# Hive
ARG HIVE_VERSION=3.1.3
ARG HIVE_URL=https://mirrors.huaweicloud.com/apache/hive/hive-$HIVE_VERSION/apache-hive-$HIVE_VERSION-bin.tar.gz
ENV HIVE_HOME=/opt/hive

RUN set -x \
 && sudo mkdir $HIVE_HOME \
 && sudo chown $USERNAME:$GROUPNAME -R $HIVE_HOME \
 && wget -P /tmp $HIVE_URL \
 && mv /tmp/apache-hive-$HIVE_VERSION-bin.tar.gz /tmp/hive.tar.gz \
 && tar -xf /tmp/hive.tar.gz -C $HIVE_HOME --strip-components 1 \
 && rm /tmp/hive*

#RUN sudo rm $HIVE_HOME/lib/guava-*.jar \
# && sudo cp $HADOOP_HOME/share/hadoop/hdfs/lib/guava-*.jar $HIVE_HOME/lib/
#RUN mv /opt/hive/lib/log4j-slf4j-impl-2.6.2.jar /opt/hive/lib/log4j-slf4j-impl-2.6.2.jar.bak

ENV HIVE_CONF_DIR=$HIVE_HOME/conf
ENV PATH=$HIVE_HOME/sbin:$HIVE_HOME/bin:$PATH

# Config
COPY --chown=$USERNAME:$GROUPNAME conf/core-site.xml $HADOOP_CONF_DIR/
COPY --chown=$USERNAME:$GROUPNAME conf/hdfs-site.xml $HADOOP_CONF_DIR/
COPY --chown=$USERNAME:$GROUPNAME conf/yarn-site.xml $HADOOP_CONF_DIR/
COPY --chown=$USERNAME:$GROUPNAME conf/mapred-site.xml $HADOOP_CONF_DIR/
COPY --chown=$USERNAME:$GROUPNAME conf/workers $HADOOP_CONF_DIR/
COPY --chown=$USERNAME:$GROUPNAME conf/spark-defaults.conf $SPARK_CONF_DIR/
COPY --chown=$USERNAME:$GROUPNAME conf/log4j.properties $SPARK_CONF_DIR/
COPY --chown=$USERNAME:$GROUPNAME conf/hive-site.xml $HIVE_CONF_DIR/


# Hivemall plugin
COPY --chown=$USERNAME:$GROUPNAME conf/.hiverc $HOME
COPY --chown=$USERNAME:$GROUPNAME resource/ddl/define-all.hive $HIVE_CONF_DIR/
COPY --chown=$USERNAME:$GROUPNAME lib/hivemall-all-0.6.0-incubating.jar $HIVE_HOME/lib

RUN ln $HADOOP_CONF_DIR/workers $SPARK_CONF_DIR/ \
 && ln $HIVE_CONF_DIR/hive-site.xml $SPARK_CONF_DIR/

# Entry point
COPY entrypoint.sh /usr/local/sbin/entrypoint.sh
RUN sudo chmod a+x /usr/local/sbin/entrypoint.sh
ENTRYPOINT ["entrypoint.sh"]