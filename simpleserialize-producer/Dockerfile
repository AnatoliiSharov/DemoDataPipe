FROM openjdk:11-jdk
#adoptopenjdk:11-jre-hotspot-bionic
#FROM alpine:latest RUN apk add netcat-openbsd

RUN apt-get update && apt-get install -y netcat-openbsd

ARG JAR_FILE=build/libs/*all*.jar

WORKDIR /app

COPY ${JAR_FILE} /app/app.jar

ENV KAFKA_TOPIC=default-topic
ENV KAFKA_BROKER_HOST=localhost
ENV KAFKA_BROKER_PORT=9092

ENTRYPOINT ["java", "-jar", "/app/app.jar"]
