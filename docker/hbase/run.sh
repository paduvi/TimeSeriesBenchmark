#!/bin/bash
docker run --rm -d -h "$(hostname)" \
  -p 2181:2181 -p 60000:60000 -p 60010:60010 -p 60020:60020 -p 60030:60030 \
  --name hbase debian-hbase
