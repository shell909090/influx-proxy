InfluxDB Proxy
======

This project adds a basic high availability layer to InfluxDB.

NOTE: influx-proxy must be built with Go 1.5+

Why
---

We used [InfluxDB Relay](https://github.com/influxdata/influxdb-relay) before, but it doesn't  support some demands.
We use grafana for visualizing time series data,so we need add datasource for grafana. We need change the datasource config when influxdb is down.
We need transfer data across idc,but Relay doesn't support gzip.Also we need analyse data,client need connect different influxdb.
Therefore,we made InfluxDB Proxy. 

Features
--------

* Support gzip.
* Support query.
* Filter some dangerous influxql.
* Transparent for client,like cluster for client.
* Cache data to file when write failed,then rewrite.

Requirements
-----------

* Redis-server
* Python >= 2.7

Usage
------------

```sh
$ # install redis-server
$ yum install redis
$ # start redis-server on 6379 port
$ redis-server --port 6379 &
$ # Install influxdb-proxy to your $GOPATH/bin
$ go get -u github.com/shell909090/influx-proxy
$ # Edit config.py and execute it
$ python config.py
$ # Start influx-proxy!
$ $GOPATH/bin/influxdb-proxy -redis localhost:6379
```

Configuration
-------------

Example configuration file is at [config.py](config.py).
We use config.py to genrate config to redis.

Commands
--------

#### Unsupported commands

The following commands are not available.

* `SELECT *` statement
* `SELECT INTO`  statement
* `SELECT cpu_load from cpu` statement,must need time ranges.

License
-------

MIT. Copyright (c) 2017 Eleme Inc.

See [LICENSE](LICENSE) for details.
