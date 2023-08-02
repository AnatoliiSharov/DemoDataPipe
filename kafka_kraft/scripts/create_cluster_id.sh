#https://medium.com/@katyagorshkova/docker-compose-for-running-kafka-in-kraft-mode-20c535c48b1a
/bin/bash

file_path="/tmp/clusterID/clusterID"

if [ ! -f "$file_path" ]; then
  /bin/kafka-storage random-uuid > /tmp/clusterID/clusterID
  echo "Cluster id has been  created..."
fi
