#!/bin/bash

# data_generator.sh
# How to use it:
#  - Copy this file to one host
#  - Set BROKER_LIST and TOPIC to the wanted values
#  - Set MSG to the format you want, use variables to modify data
#  - Run chmod +x ./data_generator.sh && ./data_generator.sh

set -euo pipefail
IFS=$'\n\t'
clear

BROKER_LIST='172.31.101.138:9093'
TOPIC='odhdev_test_in_bdebotte_01'


i=0
while true
do

    MSG="{\
\"@timestamp\": \"$(date --utc +%FT%T.%3NZ)\",\
\"hardwareVersion\": \"ARRIS-DCX960-MPA+\",\
\"firmwareVersion\": \"DCX960__-mon-prd-00.01-033-ai-AL-20180124205202-na004\",\
\"asVersion\": \"135_20180124205202_master_eos_sprint33\",\
\"appVersion\": \"135_20180124205202_master_eos_sprint33\",\
\"UsageCollectorReport_missed_events\": \"${i}\"\
}"

    ((i+=10))
    docker exec -it kafka sh -c "unset JMX_PORT; echo '${MSG}' | kafka-console-producer --broker-list ${BROKER_LIST} --topic ${TOPIC}"
    sleep 1
done
