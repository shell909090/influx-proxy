InfluxDB Proxy
======

This project adds a basic high availability and consistent hash layer to InfluxDB.

NOTE: influx-proxy must be built with Go 1.14+ with Go module support, don't implement udp.

Why
---

We used [InfluxDB Relay](https://github.com/influxdata/influxdb-relay) before, but it doesn't support some demands.
We use grafana for visualizing time series data, so we need add datasource for grafana. We need change the datasource config when influxdb is down.
We need transfer data across idc, but Relay doesn't support gzip.
It's inconvenient to analyse data with connecting different influxdb.
Therefore, we made [InfluxDB Proxy](https://github.com/shell909090/influx-proxy). More details please visit [https://github.com/shell909090/influx-proxy](https://github.com/shell909090/influx-proxy).

Forked from the above InfluxDB Proxy, after many improvements and optimizations, [InfluxDB Proxy v1](https://github.com/chengshiwen/influx-proxy/tree/branch-1.x) has released, which no longer depends on python and redis, and supports more features.

Since the InfluxDB Proxy v1 version is limited by the `KEYMAPS` configuration, we refactored [InfluxDB Proxy v2](https://github.com/chengshiwen/influx-proxy) with high availability and consistent hash, which supports tools to rebalance, recovery, resync and clear.

Features
--------

* Support gzip.
* Support query.
* Support some cluster influxql.
* Filter some dangerous influxql.
* Transparent for client, like cluster for client.
* Cache data to file when write failed, then rewrite.
* Support database sharding with consistent hash.
* Support tools to rebalance, recovery, resync and clear.
* Load config file and no longer depend on python and redis.
* Support precision query parameter when writing data.
* Support influxdb-java, influxdb shell and grafana.
* Support authentication and https.
* Support health status query.
* Support database whitelist.
* Support version display.

Requirements
-----------

* Golang >= 1.14 with Go module support

Usage
------------

#### Quickstart

```sh
$ go get -u github.com/chengshiwen/influx-proxy
$ $GOPATH/bin/influx-proxy -config /path/to/proxy.json
```

#### Build Release

```sh
$ # build current platform
$ make build
$ # build linux amd64
$ make linux
```

Tutorial
-----------

[Chinese](https://git.io/influx-proxy-wiki)

Description
-----------

The architecture is fairly simple, one InfluxDB Proxy instance and two consistent hash circles with two InfluxDB instances respectively.
The Proxy should point HTTP requests with db and measurement to the two circles and the four InfluxDB servers.

The setup should look like this:

```
        ┌──────────────────┐
        │ writes & queries │
        └──────────────────┘
                 │
                 ▼
        ┌──────────────────┐
        │                  │
        │  InfluxDB Proxy  │
        │   (only http)    │
        │                  │
        └──────────────────┘
                 │
                 ▼
        ┌──────────────────┐
        │  db,measurement  │
        │ consistent hash  │
        └──────────────────┘
          |              |
        ┌─┼──────────────┘
        │ └────────────────┐
        ▼                  ▼
     Circle 1          Circle 2
  ┌────────────┐    ┌────────────┐
  │            │    │            │
  │ InfluxDB 1 │    │ InfluxDB 3 │
  │ InfluxDB 2 │    │ InfluxDB 4 │
  │            │    │            │
  └────────────┘    └────────────┘
```

Proxy Configuration
--------

The configurations in `proxy.json` are the following:

* `circles`: circle list
  * `name`: circle name, `required`
  * `backends`: backend list belong to the circle, `required`
    * `name`: backend name, `required`
    * `url`: influxdb addr or other http backend which supports influxdb line protocol, `required`
    * `username`: influxdb username, with encryption if proxy auth_secure is enabled, default is `empty` which means no auth
    * `password`: influxdb password, with encryption if proxy auth_secure is enabled, default is `empty` which means no auth
* `listen_addr`: proxy listen addr, default is `:7076`
* `db_list`: database list permitted to access, default is `[]`
* `data_dir`: data dir to save .dat .rec, default is `data`
* `mlog_dir`: log dir to save rebalance, recovery, resync or clear operation, default is `log`
* `hash_key`: backend key for consistent hash, including "idx", "name" or "url", default is `idx`, once changed rebalance operation is necessary
* `vnode_size`: the size of virtual nodes for consistent hash, default is `256`
* `flush_size`: default is `10000`, wait 10000 points write
* `flush_time`: default is `1`, wait 1 second write whether point count has bigger than flush_size config
* `check_interval`: default is `1`, check backend active every 1 second
* `rewrite_interval`: default is `10`, rewrite every 10 seconds
* `write_timeout`: default is `10`, write timeout until 10 seconds
* `idle_timeout`: default is `10`, keep-alives wait time until 10 seconds
* `conn_pool_size`: default is 20, create a connection pool which size is 20
* `log_enabled`: enable the logging of debug messages for troubleshooting, default is `false`
* `username`: proxy username, with encryption if auth_secure is enabled, default is `empty` which means no auth
* `password`: proxy password, with encryption if auth_secure is enabled, default is `empty` which means no auth
* `auth_secure`: secure auth with encryption, default is `false`
* `https_enabled`: enable https, default is `false`
* `https_cert`: the ssl certificate to use when https is enabled
* `https_key`: use a separate private key location

Query Commands
--------

### Unsupported commands

The following commands are forbid.

* `ALTER`
* `GRANT`
* `REVOKE`
* `KILL`
* `SELECT INTO`
* `Multiple queries` delimited by semicolon `;`

### Supported commands

Only support match the following commands.

* `select from`
* `show from`
* `show measurements`
* `show series`
* `show field keys`
* `show tag keys`
* `show tag values`
* `show retention policies`
* `show stats`
* `show databases`
* `create database`
* `drop database`
* `delete from`
* `drop series from`
* `drop measurement`
* `on clause` (the `db` parameter takes precedence when the parameter is set in `/query` http endpoint)

HTTP Endpoints
--------

[HTTP Endpoints](https://github.com/chengshiwen/influx-proxy/wiki/HTTP-Endpoints)

License
-------

MIT.
