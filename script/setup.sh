#!/bin/bash

docker run -d --name influxdb-1 -p 8086:8086 influxdb:1.7
docker run -d --name influxdb-2 -p 8087:8086 influxdb:1.7
docker run -d --name influxdb-3 -p 8088:8086 influxdb:1.7
docker run -d --name influxdb-4 -p 8089:8086 influxdb:1.7


# docker run -d --name influxdb-1 -p 8086:8086 -v ${PWD}/cert:/cert -e INFLUXDB_HTTP_HTTPS_ENABLED=true -e INFLUXDB_HTTP_HTTPS_CERTIFICATE=/cert/tls.crt -e INFLUXDB_HTTP_HTTPS_PRIVATE_KEY=/cert/tls.key influxdb:1.7
# docker run -d --name influxdb-2 -p 8087:8086 -v ${PWD}/cert:/cert -e INFLUXDB_HTTP_HTTPS_ENABLED=true -e INFLUXDB_HTTP_HTTPS_CERTIFICATE=/cert/tls.crt -e INFLUXDB_HTTP_HTTPS_PRIVATE_KEY=/cert/tls.key influxdb:1.7
# docker run -d --name influxdb-3 -p 8088:8086 -v ${PWD}/cert:/cert -e INFLUXDB_HTTP_HTTPS_ENABLED=true -e INFLUXDB_HTTP_HTTPS_CERTIFICATE=/cert/tls.crt -e INFLUXDB_HTTP_HTTPS_PRIVATE_KEY=/cert/tls.key influxdb:1.7
# docker run -d --name influxdb-4 -p 8089:8086 -v ${PWD}/cert:/cert -e INFLUXDB_HTTP_HTTPS_ENABLED=true -e INFLUXDB_HTTP_HTTPS_CERTIFICATE=/cert/tls.crt -e INFLUXDB_HTTP_HTTPS_PRIVATE_KEY=/cert/tls.key influxdb:1.7
