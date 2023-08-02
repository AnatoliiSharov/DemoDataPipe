#https://medium.com/@katyagorshkova/docker-compose-for-running-kafka-in-kraft-mode-20c535c48b1a
#!/bin/sh

# Docker workaround: Remove check for KAFKA_ZOOKEEPER_CONNECT parameter
sed -i '/KAFKA_ZOOKEEPER_CONNECT/d' /etc/confluent/docker/configure

# Docker workaround: Remove check for KAFKA_ADVERTISED_LISTENERS parameter
sed -i '/dub ensure KAFKA_ADVERTISED_LISTENERS/d' /etc/confluent/docker/configure

# Docker workaround: Ignore cub zk-ready
sed -i 's/cub zk-ready/echo ignore zk-ready/' /etc/confluent/docker/ensure

