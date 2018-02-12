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


DOCKER_IMAGE="lgi/odh-python-pipelines:${DOCKER_TAG}"
docker build . -t ${DOCKER_IMAGE}


rm -fr `pwd`/reports/*
mkdir -p `pwd`/reports/xunit-reports
mkdir -p `pwd`/reports/coverage-reports


docker run --rm -it -v `pwd`/reports/xunit-reports:/odh/python/.xunit-reports \
                -v `pwd`/reports/coverage-reports:/odh/python/.coverage-reports ${DOCKER_IMAGE} \
                bash -c "cd /odh/python && nosetests --nologcapture --exclude-dir=test/unit --with-xunit \
                --xunit-file=.xunit-reports/nosetests-it.xml --with-coverage --cover-erase --cover-xml \
                --cover-xml-file=.coverage-reports/coverage-it.xml ${TEST_APP_DRIVER_PATH}"
