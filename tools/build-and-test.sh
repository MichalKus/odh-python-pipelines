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


DOCKER_IMAGE="lgi/odh-python-pipelines:${DOCKER_TAG}"
docker build . -t ${DOCKER_IMAGE}

# docker run --rm -it --net=host --rm --privileged=true --env SPARK_LOCAL_IP=127.0.0.1 \
#         -v `pwd`/configuration:/spark/processing-conf:ro \
#         -v `pwd`/tools/spark_jaas.conf:/spark/conf/spark_jaas.conf \
#         -v `pwd`/tools/log4j.properties:/spark/conf/log4j.properties \
#         -v `pwd`/checkpoints:/spark/checkpoints:rw \
#         ${DOCKER_IMAGE}  \
#         /spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.2.1 \
#         --conf spark.executor.extraJavaOptions="-Djava.security.auth.login.config=/spark/conf/spark_jaas.conf" \
#         --conf spark.driver.extraJavaOptions="-Djava.security.auth.login.config=/spark/conf/spark_jaas.conf" \
#         /odh/python/${APP_DRIVER_PATH} /spark/processing-conf/${CONFIG_PATH}

rm -fr `pwd`/reports/*
mkdir -p `pwd`/reports/xunit-reports
mkdir -p `pwd`/reports/coverage-reports


# docker run --rm -v `pwd`/reports/xunit-reports:/odh/python/.xunit-reports\
#         -v `pwd`/reports/coverage-reports:/odh/python/.coverage-reports ${DOCKER_IMAGE} \
#         bash -c 'cd /odh/python && nosetests --exclude-dir=test/it \
#         --with-xunit --xunit-file=.xunit-reports/nosetests-ut.xml --with-coverage \
#         --cover-erase --cover-xml --cover-xml-file=.coverage-reports/coverage-ut.xml'

docker run --rm -it -v `pwd`/reports/xunit-reports:/odh/python/.xunit-reports \
                -v `pwd`/reports/coverage-reports:/odh/python/.coverage-reports ${DOCKER_IMAGE} \
                bash -c 'cd /odh/python && nosetests --nologcapture --exclude-dir=test/unit --with-xunit \
                --xunit-file=.xunit-reports/nosetests-it.xml --with-coverage --cover-erase --cover-xml \
                --cover-xml-file=.coverage-reports/coverage-it.xml test/it/applications/basic_analytics/eos_stb_reports/test_usage_collector_dropped_events_report_driver.py'
