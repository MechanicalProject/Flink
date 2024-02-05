FROM flink:1.18.0-java8

ENV FLINK_VERSION="1.18.0"
ENV PYTHON_VERSION="3.10.11"
ENV BASE_FILE_PATH="./base_files"
ENV TEMP_WORK_PATH="/tmp/work"
ENV VIRTUAL_ENV=${FLINK_HOME}/venv


# Install dependencies
RUN set -ex \
&& apt update \
&& apt -y install libsnappy1v5 build-essential zlib1g-dev libncurses5-dev libgdbm-dev liblzma-dev libnss3-dev \
libssl-dev libreadline-dev libffi-dev curl libbz2-dev git vim procps telnet zip htop net-tools gdb autoconf \
&& rm -rf /var/lib/apt/lists/*


# Install python
RUN mkdir -p ${TEMP_WORK_PATH}

RUN cd ${TEMP_WORK_PATH} \
&& wget https://www.python.org/ftp/python/${PYTHON_VERSION}/Python-${PYTHON_VERSION}.tar.xz \
&& tar xvf Python-${PYTHON_VERSION}.tar.xz

RUN cd ${TEMP_WORK_PATH}/Python-${PYTHON_VERSION} \
&& ./configure --enable-optimizations \
&& make -j 8 \
&& make altinstall \
&& ln -snf /usr/local/bin/python3.10 /usr/bin/python \
&& ln -snf /usr/local/bin/python3.10 /usr/bin/python3 \
&& ln -snf /usr/local/bin/pip3.10 /usr/bin/pip \
&& ln -snf /usr/local/bin/pip3.10 /usr/bin/pip3


RUN cd ${TEMP_WORK_PATH} \
&& mkdir -p /opt/flink/jemalloc/memory-ag \
&& mkdir -p /opt/flink/jemalloc/prof \
&& git clone https://github.com/jemalloc/jemalloc.git \
&& cd jemalloc \
&& git checkout master \
&& autoconf \
&& ./configure --enable-prof --enable-stats --enable-debug --enable-fill --prefix=/opt/flink/jemalloc/memory-ag/ \
&& make \
&& make install


# Install dependencies of python
COPY ${BASE_FILE_PATH}/openjdk/include ${JAVA_HOME}/include
COPY ${BASE_FILE_PATH}/requirements.txt ${TEMP_WORK_PATH}
RUN python -m pip install --upgrade pip && pip install -r ${TEMP_WORK_PATH}/requirements.txt


# Flink
#COPY ${BASE_FILE_PATH}/lib/*.jar ${FLINK_HOME}/lib/

RUN mv ${FLINK_HOME}/examples/* ${FLINK_HOME}/examples/java | mkdir ${FLINK_HOME}/examples/java

RUN cd ${TEMP_WORK_PATH} \
&& wget https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/1.17.1/flink-sql-connector-kafka-1.17.1.jar \
&& cp ./flink-sql-connector-kafka-1.17.1.jar ${FLINK_HOME}/lib \
&& wget https://archive.apache.org/dist/flink/flink-${FLINK_VERSION}/python/apache-flink-${FLINK_VERSION}.tar.gz \
&& tar -zxvf apache-flink-${FLINK_VERSION}.tar.gz \
&& rm -rf ${FLINK_HOME}/examples/python \
&& mv ${TEMP_WORK_PATH}/apache-flink-${FLINK_VERSION}/deps/examples/* ${FLINK_HOME}/examples/python | mkdir ${FLINK_HOME}/examples/python
RUN chown -R flink:flink ${FLINK_HOME}

USER flink

RUN cd ${FLINK_HOME} \
&& python -m venv ${VIRTUAL_ENV} \
&& chmod +x "${VIRTUAL_ENV}/bin/activate" \
&& echo ". ${VIRTUAL_ENV}/bin/activate" >> .bashrc \
&& echo "alias ll='ls -al'" >> .bashrc \
&& echo "export LD_PRELOAD='/opt/flink/jemalloc/memory-ag/lib/libjemalloc.so.2'" >> .bashrc \
&& echo "export MALLOC_ARENA_MAX=4" >> .bashrc \
&& echo "export MALLOC_CONF='prof:true,prof_prefix:/opt/flink/jemalloc/prof/jeprof'" >> .bashrc

RUN . ${VIRTUAL_ENV}/bin/activate \
&& python -m pip install --upgrade pip && pip install -r ${TEMP_WORK_PATH}/requirements.txt

WORKDIR ${FLINK_HOME}

# docker build -t flink:1.18.0 .