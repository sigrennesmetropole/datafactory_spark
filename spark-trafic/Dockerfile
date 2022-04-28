FROM docker.io/bitnami/spark:2.4.3-r3

USER root
RUN apt-get update
RUN apt-get install nano
COPY ./deleteJARS.sh /
COPY ./log4j.properties  /opt/bitnami/spark/conf
WORKDIR /opt/bitnami/spark/
RUN /deleteJARS.sh
