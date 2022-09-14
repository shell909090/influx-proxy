#!/bin/bash

if [[ $# -gt 1 ]] || [[ "$1" != "" && "$1" != "flux" && "$1" != "auth" && "$1" != "https" ]]; then
    echo "Usage: $0 [flux|auth|https]"
    exit 1
fi

if [[ "$1" == "flux" ]]; then
    echo "Mode: $1"
    OPTIONS="-e INFLUXDB_HTTP_FLUX_ENABLED=true"
elif [[ "$1" == "auth" ]]; then
    echo "Mode: $1"
    OPTIONS="-e INFLUXDB_HTTP_AUTH_ENABLED=true -e INFLUXDB_ADMIN_USER=admin -e INFLUXDB_ADMIN_PASSWORD=admin"
elif [[ "$1" == "https" ]]; then
    echo "Mode: $1"
    OPTIONS="-v ${PWD}/cert:/cert -e INFLUXDB_HTTP_HTTPS_ENABLED=true -e INFLUXDB_HTTP_HTTPS_CERTIFICATE=/cert/tls.crt -e INFLUXDB_HTTP_HTTPS_PRIVATE_KEY=/cert/tls.key"
else
    echo "Mode: default"
fi

docker run -d --name influxdb-1 -p 8086:8086 ${OPTIONS} influxdb:1.8
docker run -d --name influxdb-2 -p 8087:8086 ${OPTIONS} influxdb:1.8
docker run -d --name influxdb-3 -p 8088:8086 ${OPTIONS} influxdb:1.8
docker run -d --name influxdb-4 -p 8089:8086 ${OPTIONS} influxdb:1.8
