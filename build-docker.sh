#!/bin/sh

# Arguments to the spark submit job
dockerName=$1
hostMountpath=$2

# Setting up the container where Kafka is running 
docker-compose -f ./kafka/docker-compose.yml up -d

# Setting up the container where Spark Batch and Real time pipeline runs
docker build . -f Dockerfile -t $dockerName
docker run -v $hostMountpath:/app/xapo/data/output --network=rmoff_kafka -it $dockerName /bin/bash