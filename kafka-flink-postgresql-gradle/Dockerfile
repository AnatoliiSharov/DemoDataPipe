FROM flink:1.17.1-scala_2.12-java11

# Копируем ваш JAR-файл с приложением в директорию /opt/flink/usrlib
COPY ./build/libs/kafka-flink-postgresql-gradle-0.1-SNAPSHOT-all.jar /opt/flink/usrlib/api.jar

# Можно опционально установить дополнительные зависимости для вашего приложения
# Например, для Python можно установить необходимые пакеты через pip
# RUN pip install pandas

# Копируем конфигурационный файл Flink, если он необходим
# COPY /home/anatolii/opt/flink/conf/flink-conf.yaml /opt/flink/conf/flink-config.yaml

