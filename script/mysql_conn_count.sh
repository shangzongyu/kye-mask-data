#!/bin/bash

while true; do
  echo $(date -u +"%Y-%m-%dT%H:%M:%SZ")
  mysql -uroot -proot  -e "SHOW FULL processlist" | wc -l
  sleep 1
done
