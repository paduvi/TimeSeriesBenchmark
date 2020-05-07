#!/bin/bash
# Read Password
read -s -p "password: " -r PASSWORD
echo

# Run Command
docker run -d --rm --network host \
  -e POSTGRES_PASSWORD="$PASSWORD" \
  --name timescaledb timescale/timescaledb:latest-pg12
