FROM maven:3.8.6-openjdk-11-slim AS build-trafic
RUN mkdir -p /workspace-trafic
WORKDIR /workspace-trafic
COPY spark-trafic/pom.xml /workspace-trafic
COPY spark-trafic/src /workspace-trafic/src
RUN mvn -B package --file pom.xml -DskipTests

FROM maven:3.8.6-openjdk-11-slim AS build-lora
RUN mkdir -p /workspace-lora
WORKDIR /workspace-lora
COPY spark-lora/pom.xml /workspace-lora
COPY spark-lora/src /workspace-lora/src
RUN mvn -B package --file pom.xml -DskipTests

FROM maven:3.8.6-openjdk-11-slim AS build-dechets
RUN mkdir -p /workspace-dechets
WORKDIR /workspace-dechets
COPY spark-dechet /workspace-dechets
RUN mvn -B package --file dechet-preparation/pom.xml -DskipTests
RUN mvn -B package --file dechet-analysis/pom.xml -DskipTests


FROM docker.io/bitnami/spark:3.3.0

USER root

RUN rm -f /opt/bitnami/spark/jars/spark-hive_2.12-3.3.0.jar
RUN rm -f /opt/bitnami/spark/jars/spark-mllib_2.12-3.3.0.jar
COPY log4j2.properties  /opt/bitnami/spark/conf/
COPY spark.conf /opt/bitnami/spark/conf/spark-defaults.conf

COPY --from=build-trafic /workspace-trafic/target/rm-traffic-analysis-*.jar /app-trafic/target/rm-traffic-analysis.jar
COPY --from=build-lora /workspace-lora/target/rm-lora-analysis-*.jar /app-lora/target/rm-lora-analysis.jar
COPY --from=build-dechets /workspace-dechets/dechet-preparation/target/rm-dechet-preparation-*.jar /app-dechet/dechet-preparation/target/rm-dechet-preparation.jar
COPY --from=build-dechets /workspace-dechets/dechet-analysis/target/rm-dechet-analysis-*.jar /app-dechet/dechet-analysis/target/rm-dechet-analysis.jar

WORKDIR /opt/bitnami/spark/
