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

Proxy Configuration
--------

The configurations in `proxy.json` are the following:

#### circles
* `circles`: circle list
  * `name`: circle name
  * `backends`: backend list belong to the circle
    * `name`: backend name
    * `url`: influxdb addr or other http backend which supports influxdb line protocol
    * `username`: influxdb username, with encryption if proxy auth_secure is enabled
    * `password`: influxdb password, with encryption if proxy auth_secure is enabled
* `listen_addr`: proxy listen addr
* `db_list`: database list permitted to access
* `data_dir`: data dir to save .dat .rec, default is "data"
* `mlog_dir`: log dir to save rebalance, recovery, resync or clear operation
* `hash_key`: backend key for consistent hash, including "idx", "name" or "url", default is "idx", once changed rebalance operation is necessary
* `vnode_size`: the size of virtual nodes for consistent hash
* `flush_size`: default config is 10000, wait 10000 points write
* `flush_time`: default config is 1s, wait 1 second write whether point count has bigger than flush_size config
* `username`: proxy username, with encryption if auth_secure is enabled
* `password`: proxy password, with encryption if auth_secure is enabled
* `auth_secure`: secure auth with encryption, default is false
* `https_enabled`: enable https, default is false
* `https_cert`: the ssl certificate to use when https is enabled
* `https_key`: use a separate private key location

Query Commands
--------

#### Unsupported commands

The following commands are forbid.

* `GRANT`
* `REVOKE`
* `SELECT INTO`

#### Supported commands

Only support match the following commands.

* `select from`
* `show from`
* `show measurements`
* `show series`
* `show field keys`
* `show tag keys`
* `show tag values`
* `show stats`
* `show retention policies`
* `show databases`
* `create database`
* `delete from`
* `drop measurement`

License
-------

MIT.
