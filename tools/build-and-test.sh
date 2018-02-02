#!/bin/bash

set -euo pipefail
IFS=$'\n\t'

clear

# This ensure that we're at top level of the repo
cd $(git rev-parse --show-toplevel)

if [ ! -f "tools/config" ]; then
    echo "config file not found!"
    exit 1
fi

source tools/config

if [ ! -f "tools/spark_jaas.conf" ]; then
    echo "spark_jaas.conf not found!"
    exit 1
fi

if [ ! -f "tools/log4j.properties" ]; then
    echo "log4j.properties not found!"
    exit 1
fi

if [ ! -f "src/${APP_DRIVER_PATH}" ]; then
    echo "src/${APP_DRIVER_PATH} not found!"
    exit 1
fi

if [ ! -f "configuration/${CONFIG_PATH}" ]; then
    echo "configuration/${CONFIG_PATH} not found!"
    exit 1
fi

DOCKER_IMAGE="lgi/odh-python-pipelines:${DOCKER_TAG}"
docker build . -t ${DOCKER_IMAGE}

docker run -it --net=host --rm --privileged=true --env SPARK_LOCAL_IP=127.0.0.1 \
        -v `pwd`/configuration:/spark/processing-conf:ro \
        -v `pwd`/tools/spark_jaas.conf:/spark/conf/spark_jaas.conf \
        -v `pwd`/tools/log4j.properties:/spark/conf/log4j.properties \
        -v `pwd`/checkpoints:/spark/checkpoints:rw \
        ${DOCKER_IMAGE}  \
        /spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.2.1 \
        --conf spark.executor.extraJavaOptions="-Djava.security.auth.login.config=/spark/conf/spark_jaas.conf" \
        --conf spark.driver.extraJavaOptions="-Djava.security.auth.login.config=/spark/conf/spark_jaas.conf" \
        /odh/python/${APP_DRIVER_PATH} /spark/processing-conf/${CONFIG_PATH}
