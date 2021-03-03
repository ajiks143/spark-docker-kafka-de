#!/bin/sh

# Arguments to the spark submit job
dockerName=$1
hostMountpath=$2

docker build . -f Dockerfile -t $dockerName
docker run -v $hostMountpath:/app/xapo/data/output --network host -it $dockerName /bin/bash