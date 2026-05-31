FROM maven:3.9.9-eclipse-temurin-17 AS build

WORKDIR /build
COPY pom.xml .
RUN mvn -B -DskipTests dependency:go-offline
COPY src ./src
RUN mvn -B -DskipTests package

FROM flink:1.19.1-java17

COPY --from=build /build/target/bigdata-flink-lab-1.0.0.jar /opt/flink/usrlib/bigdata-flink-lab.jar
