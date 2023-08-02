FROM openjdk:11
#oracle/openjdk:11 adoptopenjdk/openjdk11 

WORKDIR /app

COPY ./build/libs/kafka-producer-all-0.0.1.jar /app/app.jar

EXPOSE 9092

CMD ["java", "-jar", "/app/app.jar"]
