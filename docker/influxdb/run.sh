#!/bin/bash
docker run -d -p 9999:9999 --rm --name influxdb \
  quay.io/influxdb/influxdb:2.0.0-beta --reporting-disabled
