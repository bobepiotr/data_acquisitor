#!/bin/bash

timestamp=$(date +%s)
mkdir -p /tmp/reports
touch /tmp/reports/ls_report_${timestamp}.txt

hdfs dfs -mkdir -p /all_data/

while read DATA; do
  echo
  hdfs dfs -test -e /all_data/${DATA}
  EXIT_STATUS=$?
  echo "Processing ${DATA}"

  if [ $EXIT_STATUS -eq 0 ]; then
    echo "File already exists. Removing data associated with ${DATA} ..."
    hdfs dfs -rm -R /all_data/${DATA}
    rm /tmp/reports/fsck_report_${DATA}.txt
    rm /tmp/reports/dfsadmin_report_${DATA}.txt
  fi

  touch /tmp/reports/fsck_report_${DATA}.txt
  touch /tmp/reports/dfsadmin_report_${DATA}.txt
  hdfs dfs -put /data/hadoop/data/${DATA} /all_data
  # hdfs dfs -setrep -w 3 /all_data/${DATA}
  hdfs fsck /all_data/${DATA} >> /tmp/reports/fsck_report_${DATA}.txt
  hdfs dfsadmin -report >> /tmp/reports/dfsadmin_report_${DATA}.txt
  echo "Finished file ${DATA}"
done <"/data/hadoop/download_info.txt"


hdfs dfs -ls /all_data > /data/reports/ls_report_${timestamp}.txt
echo "Finished all files"

echo "Clearing download info log."
>"/data/hadoop/download_info.txt"
