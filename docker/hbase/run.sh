#!/bin/bash
docker build -t debian-hbase .
docker run --rm -d --network host --name hbase debian-hbase
