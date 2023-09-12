FROM openjdk:11
#oracle/openjdk:11 adoptopenjdk/openjdk11 

WORKDIR /app

COPY ./build/libs/kafka-producer-simulator-all-0.0.1.jar /app/app.jar

CMD ["java", "-jar", "/app/app.jar"]
