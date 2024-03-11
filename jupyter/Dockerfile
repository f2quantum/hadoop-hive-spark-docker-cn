FROM hadoop-hive-spark-base AS base
FROM tensorflow/tensorflow:2.13.0-gpu-jupyter

RUN apt-key del 7fa2af80
RUN curl -fsSL https://developer.download.nvidia.com/compute/cuda/repos/ubuntu2004/x86_64/cuda-keyring_1.0-1_all.deb -o /tmp/cuda-keyring_1.0-1_all.deb 
RUN dpkg -i /tmp/cuda-keyring_1.0-1_all.deb
RUN rm -rf /etc/apt/sources.list.d/cuda.list

RUN  sed -i 's@//.*archive.ubuntu.com@//mirrors.ustc.edu.cn@g' /etc/apt/sources.list

RUN apt-get update -y \
 && DEBIAN_FRONTEND=noninteractive apt-get install --no-install-recommends \
      sudo \
      openjdk-8-jdk \
      curl \
      coreutils \
      libc6-dev -y\
 && rm -rf /var/lib/apt/lists/*

ARG USERNAME=jupyter
ARG GROUPNAME=jupyter
ARG UID=1000
ARG GID=1000

RUN echo $USERNAME ALL=\(root\) NOPASSWD:ALL > /etc/sudoers.d/$USERNAME \
 && chmod 0440 /etc/sudoers.d/$USERNAME \
 && groupadd -g $GID $GROUPNAME \
 && useradd -m -s /bin/bash -u $UID -g $GID $USERNAME

USER $USERNAME

ENV JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64/

# Hadoop
COPY --from=base --chown=$USERNAME:$GROUPNAME /opt/hadoop /opt/hadoop
ENV HADOOP_HOME=/opt/hadoop
ENV HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
ENV PATH=$HADOOP_HOME/sbin:$HADOOP_HOME/bin:$PATH
ENV LD_LIBRARY_PATH=$HADOOP_HOME/lib/native:$LD_LIBRARY_PATH

# Spark
COPY --from=base --chown=$USERNAME:$GROUPNAME /opt/spark /opt/spark
ENV SPARK_HOME=/opt/spark
ENV PYTHONHASHSEED=1
ENV PYSPARK_PYTHON=python3
ENV SPARK_CONF_DIR=$SPARK_HOME/conf
ENV PATH=$SPARK_HOME/sbin:$SPARK_HOME/bin:$PATH

# Hive
COPY --from=base --chown=$USERNAME:$GROUPNAME /opt/hive /opt/hive
ENV HIVE_HOME=/opt/hive
ENV HIVE_CONF_DIR=$HIVE_HOME/conf
ENV PATH=$HIVE_HOME/sbin:$HIVE_HOME/bin:$PATH
# Custom Jars
COPY --chown=$USERNAME:$GROUPNAME /jars /opt/spark/jars

# Python 
COPY --chown=$USERNAME:$GROUPNAME ./requirements.txt /home/$USERNAME

RUN sudo pip install -U pip -i https://pypi.tuna.tsinghua.edu.cn/simple &&\
    sudo pip install  -i https://pypi.tuna.tsinghua.edu.cn/simple -r $HOME/requirements.txt
    
WORKDIR /home/$USERNAME

COPY run.sh /usr/local/sbin/run.sh
RUN sudo chmod a+x /usr/local/sbin/run.sh
CMD ["run.sh"]