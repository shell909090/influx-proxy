InfluxDB Proxy
======

This project adds a basic high availability layer to InfluxDB.

NOTE: influx-proxy must be built with Go 1.7+, don't implement udp.

Why
---

We used [InfluxDB Relay](https://github.com/influxdata/influxdb-relay) before, but it doesn't support some demands.
We use grafana for visualizing time series data, so we need add datasource for grafana. We need change the datasource config when influxdb is down.
We need transfer data across idc, but Relay doesn't support gzip.
It's inconvenient to analyse data with connecting different influxdb.
Therefore, we made InfluxDB Proxy.

Features
--------

* Support gzip.
* Support query.
* Filter some dangerous influxql.
* Transparent for client, like cluster for client.
* Cache data to file when write failed, then rewrite.
* Support sharding with consistent hash.

Requirements
-----------

* Golang >= 1.7

Usage
------------

```sh
$ # Install influx-proxy to your $GOPATH/bin
$ go get -u github.com/chengshiwen/influx-proxy
$ go install github.com/chengshiwen/influx-proxy
$ # Start influx-proxy!
$ $GOPATH/bin/influx-proxy -config proxy.json
```

Description
-----------

The architecture is fairly simple, one InfluxDB Proxy process and two or more InfluxDB processes. The Proxy should point HTTP requests with db and measurement to the two InfluxDB servers.

The setup should look like this:

```
        ┌─────────────────┐
        │writes & queries │
        └─────────────────┘
                 │
                 ▼
         ┌───────────────┐
         │               │
         │InfluxDB Proxy │
         |  (only http)  |
         │               │
         └───────────────┘
                 │
                 ▼
        ┌─────────────────┐
        │  db,measurement │
        │ consistent hash │
        └─────────────────┘
          |              |
        ┌─┼──────────────┘
        │ └──────────────┐
        ▼                ▼
  ┌──────────┐      ┌──────────┐
  │          │      │          │
  │ InfluxDB │      │ InfluxDB │
  │          │      │          │
  └──────────┘      └──────────┘
```

Query Commands
--------

#### Unsupported commands

The following commands are forbid.

* `GRANT`
* `REVOKE`

#### Supported commands

Only support match the following commands.

* `.*from.*`
* `drop measurement.*`
* `show.*measurements`

License
-------

MIT.
