#!/bin/bash
docker run -d -p 8086:8086 --rm --name influxdb \
  -v "$PWD/data:/var/lib/influxdb" \
  influxdb
