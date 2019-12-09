#!/bin/bash
# check dmesg io & memory error , if any post falcon
# This script is called by ExternalScriptHealthChecker

if [ $# -ne 2 ];then
  echo "Argument size is not 2"
  exit 1
fi

HOST=`hostname`

if [[ -z "$HOST" ]];then
   echo "Hostname is empty"
   exit 2
fi

INTERVAL=$1
CLUSTER_NAME=$2

if [[ -z "$CLUSTER_NAME" ]];then
  echo "Cluster name is empty"
  exit 3
fi

NOW=`date +%s`
STEP=$[INTERVAL / 1000]
START_TIME=$[NOW - INTERVAL / 1000]

ERRORS=""
LATEST_OFFSET=0
LATEST_UNIXTIMESTAMP=0
METRIC=""

function set_io_error() {
  ERRORS=`dmesg | egrep -i 'error|hang'|egrep -v 'ioapic|ACPI|ERST'|egrep 'EXT4-fs error|I/O error'`
}

function set_mem_error() {
  ERRORS=`dmesg | grep 'MCE MEMORY ERROR'`
}

function set_latest_offset() {
  LATEST_OFFSET=`echo "${ERRORS}" | awk -F '[][]' 'BEGIN {max=0} { if($2 + 0 > max + 0) max=$2 } END { print max }'`
}


function set_latest_unixtimestamp() {
  LATEST_UNIXTIMESTAMP=$(date -d "1970-01-01 UTC `echo "$(date +%s)-$(cat /proc/uptime|cut -f 1 -d' ') + $LATEST_OFFSET"|bc ` seconds" +'%s')
}

function post_falcon() {
curl -i -X POST -H "'Content-type':'application/json'" -d  '[{ "endpoint" : "hbase-health-checker", "metric": "'$METRIC'","tags": "host=$HOST,cluster=$CLUSTER_NAME","timestamp": '$NOW', "step": '$STEP', "value": 1, "counterType": "GAUGE" }]' http://127.0.0.1:1988/v1/push
}

function process_error() {
  if [[ -z "$ERRORS" ]];then
    echo "empty string"
    return
  fi
  set_latest_offset
  set_latest_unixtimestamp
  if [ $LATEST_UNIXTIMESTAMP -ge $START_TIME ]
  then
    echo "ERROR: $ERRORS"
    post_falcon
  fi
}

function check_io_error() {
  set_io_error
  METRIC='dmesg.error.io'
  process_error
}

function check_mem_error() {
  set_mem_error
  METRIC='dmesg.error.memory'
  process_error
}

check_io_error
check_mem_error
echo "Finished to check io & memory status."
