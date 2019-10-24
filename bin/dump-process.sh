#!/bin/bash

pid=$1

if [ ! -n "$pid" ]
then
  echo "Usage: ./dump-process.sh pid"
  exit 0
fi

jps_result=$(jps | grep $pid | awk -F' ' '{print $1}')

if [ -n "$jps_result" ]
then
  dir="$pid-$(date +%s)"
  mkdir $dir

  echo -e "Find process $pid, will dump jstack/threads info to $dir\n"
  for i in {0..9}
  do
    echo "Start jstack $i times"
    jstack $pid > $dir/$pid.jstack.$i
  done
  echo -e "\nDump ps with threads info to $dir/$pid.ps"
  ps -mp $pid -o THREAD,tid,time | awk '{if(NR>1)print}' | sort -k2 -n > $dir/$pid.ps
  echo -e "Find the top 10 cpu threads:\n"
  awk -F' ' '{print $8}' $dir/$pid.ps | tail -n 11 | head -n 10 | xargs printf "%x\n" > $dir/top10.threads
  cat $dir/top10.threads | while read line
  do
    grep $line $dir/$pid.jstack.0
  done
else
  echo "There are no process $pid"
fi
