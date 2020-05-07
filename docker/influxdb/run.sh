#!/bin/bash
docker run -d --network host --rm --name influxdb quay.io/influxdb/influxdb:2.0.0-beta --reporting-disabled
