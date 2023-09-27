#!/bin/bash
kafka-console-consumer --topic ${KAFKA_TOPIC} --from-beginning --bootstrap-server ${KAFKA_BROKER_HOST}:${KAFKA_BROKER_PORT}
