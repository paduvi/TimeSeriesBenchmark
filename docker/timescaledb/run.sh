#!/bin/bash
# Read Password
read -s -p "password: " -r PASSWORD
echo

# Run Command
docker run -d --rm --name timescaledb \
  -p 5432:5432 -e POSTGRES_PASSWORD="$PASSWORD" \
  timescale/timescaledb:latest-pg12
