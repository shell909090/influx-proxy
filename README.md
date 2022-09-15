# InfluxDB Proxy

[![CN doc](https://img.shields.io/badge/文档-中文版-blue.svg)](https://github.com/chengshiwen/influx-proxy/wiki)
[![EN doc](https://img.shields.io/badge/document-English-blue.svg)](https://github.com/chengshiwen/influx-proxy/blob/master/README.md)
[![Go Report Card](https://goreportcard.com/badge/chengshiwen/influx-proxy)](https://goreportcard.com/report/chengshiwen/influx-proxy)
[![LICENSE](https://img.shields.io/github/license/chengshiwen/influx-proxy.svg)](https://github.com/chengshiwen/influx-proxy/blob/master/LICENSE)
[![Releases](https://img.shields.io/github/v/release/chengshiwen/influx-proxy.svg)](https://github.com/chengshiwen/influx-proxy/releases)
![GitHub stars](https://img.shields.io/github/stars/chengshiwen/influx-proxy.svg?label=github%20stars&logo=github)
[![Docker pulls](https://img.shields.io/docker/pulls/chengshiwen/influx-proxy.svg)](https://hub.docker.com/r/chengshiwen/influx-proxy)

This project adds a basic high availability and consistent hash layer to InfluxDB.

NOTE: influx-proxy must be built with Go 1.16+ with Go module support, don't implement udp.

NOTE: [InfluxDB Cluster](https://github.com/chengshiwen/influxdb-cluster) - open source alternative to [InfluxDB Enterprise](https://docs.influxdata.com/enterprise_influxdb/v1.8/) has been released, which is better than InfluxDB Proxy.

## Why

We used [InfluxDB Relay](https://github.com/influxdata/influxdb-relay) before, but it doesn't support some demands.
We use grafana for visualizing time series data, so we need add datasource for grafana. We need change the datasource config when influxdb is down.
We need transfer data across idc, but Relay doesn't support gzip.
It's inconvenient to analyse data with connecting different influxdb.
Therefore, we made [InfluxDB Proxy](https://github.com/shell909090/influx-proxy). More details please visit [https://github.com/shell909090/influx-proxy](https://github.com/shell909090/influx-proxy).

Forked from the above InfluxDB Proxy, after many improvements and optimizations, [InfluxDB Proxy v1](https://github.com/chengshiwen/influx-proxy/tree/branch-1.x) has released, which no longer depends on python and redis, and supports more features.

Since the InfluxDB Proxy v1 is limited by the only `ONE` database and the `KEYMAPS` configuration, we refactored [InfluxDB Proxy v2](https://github.com/chengshiwen/influx-proxy) with high availability and consistent hash, which supports multiple databases and tools to rebalance, recovery, resync and cleanup.

## Features

* Support query and write.
* Support /api/v2 endpoints.
* Support flux language query.
* Support some cluster influxql.
* Filter some dangerous influxql.
* Transparent for client, like cluster for client.
* Cache data to file when write failed, then rewrite.
* Support multiple databases to create and store.
* Support database sharding with consistent hash.
* Support tools to rebalance, recovery, resync and cleanup.
* Load config file and no longer depend on python and redis.
* Support both rp and precision parameter when writing data.
* Support influxdb-java, influxdb shell and grafana.
* Support prometheus remote read and write.
* Support authentication and https.
* Support authentication encryption.
* Support health status check.
* Support database whitelist.
* Support version display.
* Support gzip.

## Requirements

* Golang >= 1.16 with Go module support
* InfluxDB 1.2 - 1.8 (For InfluxDB 2.x, please visit branch [influxdb-v2](https://github.com/chengshiwen/influx-proxy/tree/influxdb-v2))

## Usage

#### Quickstart by Docker

Download `docker-compose.yml` and `proxy.json` from [docker/quick](https://github.com/chengshiwen/influx-proxy/tree/master/docker/quick)

```sh
$ docker-compose up -d
```

An influx-proxy container (port: 7076) and 4 influxdb containers will start.

#### Quickstart

```sh
$ git clone https://github.com/chengshiwen/influx-proxy.git
$ cd influx-proxy
$ make
$ ./bin/influx-proxy -config proxy.json
```

#### Usage

```sh
$ ./bin/influx-proxy -h
Usage of ./bin/influx-proxy:
  -config string
        proxy config file with json/yaml/toml format (default "proxy.json")
  -version
        proxy version
```

#### Build Release

```sh
$ # build current platform
$ make build
$ # build linux amd64
$ make linux
$ # cross-build all platforms
$ make release
```

## Development

Before developing, you need to install and run [Docker](https://docs.docker.com/get-docker/)

```sh
$ ./script/setup.sh  # start 4 influxdb instances by docker
$ make run
$ ./script/write.sh  # write data
$ ./script/query.sh  # query data
$ ./script/remove.sh # remove 4 influxdb instances
```

## Description

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

## Proxy Configuration

The configuration file supports format `json`, `yaml` and `toml`, such as [proxy.json](proxy.json), [proxy.yaml](conf/proxy.yaml) and [proxy.toml](conf/proxy.toml).

The configuration settings are as follows:

* `circles`: circle list
  * `name`: circle name, `required`
  * `backends`: backend list belong to the circle, `required`
    * `name`: backend name, `required`
    * `url`: influxdb addr or other http backend which supports influxdb line protocol, `required`
    * `username`: influxdb username, with encryption if auth_encrypt is enabled, default is `empty` which means no auth
    * `password`: influxdb password, with encryption if auth_encrypt is enabled, default is `empty` which means no auth
    * `auth_encrypt`: whether to encrypt auth (username/password), default is `false`
    * `write_only`: whether to write only on the influxdb, default is `false`
* `listen_addr`: proxy listen addr, default is `:7076`
* `db_list`: database list permitted to access, default is `[]`
* `data_dir`: data dir to save .dat .rec, default is `data`
* `tlog_dir`: transfer log dir to rebalance, recovery, resync or cleanup, default is `log`
* `hash_key`: backend key for consistent hash, including "idx", "exi", "name" or "url", default is `idx`, once changed rebalance operation is necessary
* `flush_size`: default is `10000`, wait 10000 points write
* `flush_time`: default is `1`, wait 1 second write whether point count has bigger than flush_size config
* `check_interval`: default is `1`, check backend active every 1 second
* `rewrite_interval`: default is `10`, rewrite every 10 seconds
* `conn_pool_size`: default is `20`, create a connection pool which size is 20
* `write_timeout`: default is `10`, write timeout until 10 seconds
* `idle_timeout`: default is `10`, keep-alives wait time until 10 seconds
* `username`: proxy username, with encryption if auth_encrypt is enabled, default is `empty` which means no auth
* `password`: proxy password, with encryption if auth_encrypt is enabled, default is `empty` which means no auth
* `auth_encrypt`: whether to encrypt auth (username/password), default is `false`
* `write_tracing`: enable logging for the write, default is `false`
* `query_tracing`: enable logging for the query, default is `false`
* `pprof_enabled`: enable `/debug/pprof` HTTP endpoint, default is `false`
* `https_enabled`: enable https, default is `false`
* `https_cert`: the ssl certificate to use when https is enabled, default is `empty`
* `https_key`: use a separate private key location, default is `empty`

## Query Commands

### Unsupported commands

The following commands are forbid.

* `GRANT`
* `REVOKE`
* `KILL`
* `EXPLAIN`
* `SELECT INTO`
* `CONTINUOUS QUERY`
* `Multiple queries` delimited by semicolon `;`
* `Multiple measurements` delimited by comma `,`
* `Regexp measurement`

### Supported commands

Only support match the following commands.

* `select from`
* `show from`
* `show measurements`
* `show series`
* `show field keys`
* `show tag keys`
* `show tag values`
* `show stats`
* `show databases`
* `create database`
* `drop database`
* `show retention policies`
* `create retention policy`
* `alter retention policy`
* `drop retention policy`
* `delete from`
* `drop series from`
* `drop measurement`
* `on clause`
* `from clause` like `from <db>.<rp>.<measurement>`

## HTTP Endpoints

[HTTP Endpoints](https://github.com/chengshiwen/influx-proxy/wiki/HTTP-Endpoints)

## Benchmark

There are three tools for benchmarking InfluxDB, which can also be applied to InfluxDB Proxy:

* [influx-stress](https://github.com/chengshiwen/influx-stress) is a stress tool for generating artificial load on InfluxDB.
* [influxdb-comparisons](https://github.com/influxdata/influxdb-comparisons) contains code for benchmarking InfluxDB against other databases and time series solutions.
* [tsbs](https://github.com/timescale/tsbs) (Time Series Benchmark Suite) is a tool for comparing and evaluating databases for time series data.

## Tool

There is a tool for InfluxDB and InfluxDB Proxy:

* [influx-tool](https://github.com/chengshiwen/influx-tool): high performance tool to rebalance, recovery, resync, cleanup and compact. most commands do not require InfluxDB to start

## License

MIT.
