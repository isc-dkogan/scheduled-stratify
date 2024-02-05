FROM ubuntu:22.04

ARG DEBIAN_FRONTEND=noninteractive

RUN apt-get update && \
    apt-get -y install gcc mono-mcs && \
    rm -rf /var/lib/apt/lists/* 

#RUN apt-get install libboost-all-dev
RUN apt-get update && \
    apt-get -y install software-properties-common

RUN  add-apt-repository ppa:deadsnakes/ppa && \
     apt-get update

RUN apt-get -y install unixodbc unixodbc-dev

# TODO - link python to python3 command
RUN apt-get -y install python3.10
RUN apt-get -y install python3-pip && \
    pip install --upgrade pip && \
    pip install --upgrade setuptools

# TODO - test just installing JRE
RUN add-apt-repository ppa:openjdk-r/ppa
RUN apt-get -y install openjdk-11-jdk

RUN apt-get install -y sqlite3 libsqlite3-dev
RUN mkdir /db
RUN /usr/bin/sqlite3 /db/test.db

RUN apt-get -y install curl

RUN apt-get clean

ADD python/requirements.txt /tmp/requirements.txt
RUN pip install --requirement /tmp/requirements.txt

COPY  ./ /home/

WORKDIR "/home"

RUN cp conf/odbcinst.ini /etc/odbcinst.ini

RUN pip install bin/intersystems_irispython-3.2.0-py3-none-any.whl

RUN python3 scripts/install_sqllite.py 

WORKDIR "/home/python"

RUN pip install -e .

RUN apt-get -y install wget

RUN wget https://github.com/sevdokimov/log-viewer/releases/download/v1.0.5/log-viewer-1.0.5.tar.gz && \
    tar -xvf log-viewer-1.0.5.tar.gz 

WORKDIR "/home"

CMD ["scripts/start_services.sh"]

