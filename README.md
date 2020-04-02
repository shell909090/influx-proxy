InfluxDB Proxy
======

This project adds a basic high availability and consistent hash layer to InfluxDB.

NOTE: influx-proxy must be built with Go 1.8+, don't implement udp.

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

* Golang >= 1.8

Usage
------------

#### Quickstart

```sh
$ go get -u github.com/chengshiwen/influx-proxy
$ cd $GOPATH/src/github.com/chengshiwen/influx-proxy
$ $GOPATH/bin/influx-proxy -config proxy.json
```

#### Build Release

```sh
$ cd $GOPATH/src/github.com/chengshiwen/influx-proxy
$ # build current platform
$ make build
$ # build linux amd64
$ make linux
```

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
* `vnode_size`: the size of virtual nodes for consistent hash, default is 256
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

### Unsupported commands

The following commands are forbid.

* `GRANT`
* `REVOKE`
* `SELECT INTO`

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
* `show retention policies`
* `show databases`
* `create database`
* `delete from`
* `drop measurement`

HTTP Endpoints
--------

| Endpoint    | Description |
| :---------- | :---------- |
| [/ping](#ping-http-endpoint) | Use `/ping` to check the status of your InfluxDB Proxy instance and your version of InfluxDB Proxy. |
| [/query](#query-http-endpoint) | Use `/query` to query data and manage databases and measurements. |
| [/write](#write-http-endpoint) | Use `/write` to write data to a pre-existing database. |
| [/health](#health-http-endpoint) | Use `/health` to view health status of all influxdb backends. |
| [/replica](#replica-http-endpoint) | Use `/replica` to query replica backends with database and measurement. |
| [/encrypt](#encrypt-http-endpoint) | Use `/encrypt` to encrypt plain username and password. |
| [/decrypt](#decrypt-http-endpoint) | Use `/decrypt` to decrypt ciphered username and password. |
| [/migrate/state](#migratestate-http-endpoint) | Use `/migrate/state` to get or set migration state. |
| [/migrate/stats](#migratestats-http-endpoint) | Use `/migrate/stats` to query migration stats when migrating. |
| [/rebalance](#rebalance-http-endpoint) | Use `/rebalance` to rebalance measurements of specific circle. |
| [/recovery](#recovery-http-endpoint) | Use `/recovery` to recovery measurements from one circle to another circle. |
| [/resync](#resync-http-endpoint) | Use `/resync` to resync measurements with each other. |
| [/clear](#clear-http-endpoint) | Use `/clear` to clear misplaced measurements of specific circle. |
| [/debug/pprof](#debugpprof-http-endpoint)   | Use `/debug/pprof` to generate profiles for troubleshooting.  |

The following sections assume your InfluxDB Proxy instance is running on `127.0.0.1:7076` and HTTPS is not enabled.

### `/ping` HTTP endpoint

official reference:
* [/ping HTTP endpoint](https://docs.influxdata.com/influxdb/v1.7/tools/api/#ping-http-endpoint)

### `/query` HTTP endpoint

official reference:
* [/query HTTP endpoint](https://docs.influxdata.com/influxdb/v1.7/tools/api/#query-http-endpoint)
* [Querying data with the InfluxDB API](https://docs.influxdata.com/influxdb/v1.7/guides/querying_data/)

### `/write` HTTP endpoint

official reference:
* [/write HTTP endpoint](https://docs.influxdata.com/influxdb/v1.7/tools/api/#write-http-endpoint)
* [Writing data with the InfluxDB API](https://docs.influxdata.com/influxdb/v1.7/guides/writing_data/)

### `/health` HTTP endpoint

#### Definition

```
GET http://127.0.0.1:7076/health
```

#### Query string parameters

| Query String Parameter | Optional/Required | Description |
| :--------------------- | :---------------- |:---------- |
| pretty=true | Optional | Enables pretty-printed JSON output. |
| u=\<username> | Optional if you haven't enabled authentication | Set the username for authentication if you've enabled authentication. |
| p=\<password> | Optional if you haven't enabled authentication | Set the password for authentication if you've enabled authentication. |

#### Example

```bash
$ curl 'http://127.0.0.1:7076/health?pretty=true'

[
    {
        "backends": [
            {
                "active": true,
                "backlog": false,
                "load": {
                    "db1": {
                        "incorrect": 0,
                        "inplace": 2,
                        "measurements": 2
                    },
                    "db2": {
                        "incorrect": 0,
                        "inplace": 1,
                        "measurements": 1
                    }
                },
                "name": "influxdb-1-1",
                "rewrite": false,
                "url": "http://127.0.0.1:8086"
            }
        ],
        "circle": "circle-1"
    }
]
```

Field description:

- name: influxdb name
- url: influxdb url
- active: whether the influxdb is running
- backlog: is there failed data in the cache file to influxdb
- rewrite: whether the cache data is being rewritten to influxdb
- load: influxdb load status
  - db: influxdb database
    - incorrect: number of measurements which should not be stored on this influxdb
      - it's healthy when incorrect equals 0
    - inplace: number of measurements which should be stored on this influxdb
      - it's healthy when inplace equals the number of measurements in this database
    - measurements: number of measurements in this database
      - `measurements = incorrect + inplace`
      - the number of measurements in each influxdb instance is roughly the same, indicating that the data store is load balanced

### `/replica` HTTP endpoint

#### Definition

```
GET http://127.0.0.1:7076/replica
```

#### Query string parameters

| Query String Parameter | Optional/Required | Description |
| :--------------------- | :---------------- |:---------- |
| db=\<database> | Required | Database name for replica backends query. |
| meas=\<measurement> | Required | Measurement name for replica backends query. |
| pretty=true | Optional | Enables pretty-printed JSON output. |
| u=\<username> | Optional if you haven't enabled authentication | Set the username for authentication if you've enabled authentication. |
| p=\<password> | Optional if you haven't enabled authentication | Set the password for authentication if you've enabled authentication. |

#### Example

```bash
$ curl 'http://127.0.0.1:7076/replica?db=db1&meas=cpu1&pretty=true'

[
    {
        "circle": "circle-1",
        "name": "influxdb-1-1",
        "url": "http://127.0.0.1:8086"
    },
    {
        "circle": "circle-2",
        "name": "influxdb-2-2",
        "url": "http://127.0.0.1:8089"
    }
]
```

### `/encrypt` HTTP endpoint

#### Definition

```
GET http://127.0.0.1:7076/encrypt
```

#### Query string parameters

| Query String Parameter | Optional/Required | Description |
| :--------------------- | :---------------- |:---------- |
| msg=\<message> | Required | The plain message to encrypt. |

#### Example

```bash
$ curl 'http://127.0.0.1:7076/encrypt?msg=admin'

YgvEyuPZlDYAH8sGDkC!Ag
```

### `/decrypt` HTTP endpoint

#### Definition

```
GET http://127.0.0.1:7076/decrypt
```

#### Query string parameters

| Query String Parameter | Optional/Required | Description |
| :--------------------- | :---------------- |:---------- |
| key=\<key> | Required | The key to encrypt and decrypt, default is consistentcipher. |
| msg=\<message> | Required | The ciphered message to decrypt. |

#### Example

```bash
$ curl 'http://127.0.0.1:7076/decrypt?key=consistentcipher&msg=YgvEyuPZlDYAH8sGDkC!Ag'

admin
```

### `/migrate/state` HTTP endpoint

#### Definition

```
GET http://127.0.0.1:7076/migrate/state
```

```
POST http://127.0.0.1:7076/migrate/state
```

#### Query string parameters

| Query String Parameter | Optional/Required | Description |
| :--------------------- | :---------------- |:---------- |
| resyncing=true | Optional | Set resyncing state of proxy. |
| circle_id=0 | Optional | Circle index to set migrating state, started from 0. |
| migrating=false | Optional | Set migrating state of circle specified by circle_id. |
| pretty=true | Optional | Enables pretty-printed JSON output. |
| u=\<username> | Optional if you haven't enabled authentication | Set the username for authentication if you've enabled authentication. |
| p=\<password> | Optional if you haven't enabled authentication | Set the password for authentication if you've enabled authentication. |

#### Example

Get migration state

```bash
$ curl 'http://127.0.0.1:7076/migrate/state?pretty=true'

{
    "circles": [
        {
            "circle_id": 0,
            "circle_name": "circle-1",
            "is_migrating": false
        },
        {
            "circle_id": 1,
            "circle_name": "circle-2",
            "is_migrating": false
        }
    ],
    "is_resyncing": false
}
```

Set resyncing state of proxy

```bash
$ curl -X POST 'http://127.0.0.1:7076/migrate/state?resyncing=true&pretty=true'

{
    "resyncing": true
}
```

Set migrating state of circle 0

```bash
$ curl -X POST 'http://127.0.0.1:7076/migrate/state?circle_id=0&migrating=true&pretty=true'

{
    "circle": {
        "circle_id": 0,
        "circle_name": "circle-1",
        "is_migrating": true
    }
}
```

### `/migrate/stats` HTTP endpoint

#### Definition

```
GET http://127.0.0.1:7076/migrate/stats
```

#### Query string parameters

| Query String Parameter | Optional/Required | Description |
| :--------------------- | :---------------- |:---------- |
| circle_id=0 | Required | Circle index started from 0. |
| type=\<type> | Required | Stats type, valid options are rebalance, recovery, resync or clear. |
| pretty=true | Optional | Enables pretty-printed JSON output. |
| u=\<username> | Optional if you haven't enabled authentication | Set the username for authentication if you've enabled authentication. |
| p=\<password> | Optional if you haven't enabled authentication | Set the password for authentication if you've enabled authentication. |

#### Example

```bash
$ curl 'http://127.0.0.1:7076/migrate/stats?circle_id=0&type=rebalance&pretty=true'

{
    "http://127.0.0.1:8086": {
        "database_total": 0,
        "database_done": 0,
        "measurement_total": 0,
        "measurement_done": 0,
        "migrate_count": 0,
        "inplace_count": 0
    },
    "http://127.0.0.1:8087": {
        "database_total": 0,
        "database_done": 0,
        "measurement_total": 0,
        "measurement_done": 0,
        "migrate_count": 0,
        "inplace_count": 0
    }
}
```

### `/rebalance` HTTP endpoint

#### Definition

```
POST http://127.0.0.1:7076/rebalance
```

#### Query string parameters

| Query String Parameter | Optional/Required | Description |
| :--------------------- | :---------------- |:---------- |
| circle_id=0 | Required | Circle index started from 0. |
| operation=\<operation> | Required | Operation type, valid options are add or rm. |
| db=\<db_list> | Optional | database list split by ',' while using all databases if empty. |
| cpus=1 | Optional | Migration max cpus, default is 1. |
| ha_addrs=\<ha_addrs> | Optional if only one InfluxDB Proxy instance is running | High available addrs of all running InfluxDB Proxy for two instances at least. |
| u=\<username> | Optional if you haven't enabled authentication | Set the username for authentication if you've enabled authentication. |
| p=\<password> | Optional if you haven't enabled authentication | Set the password for authentication if you've enabled authentication. |

#### Request body

Json body for describing backends to remove, required when operation is rm

```
{
    "backends": [
        {
            "name": "influxdb-1-2",
            "url": "http://127.0.0.1:8087",
            "username": "",
            "password": ""
        }
    ]
}
```

#### Example

Rebalance measurements of circle specified by circle_id after adding backends, while there are two running InfluxDB Proxy instances

```bash
$ curl -X POST 'http://127.0.0.1:7076/rebalance?circle_id=0&operation=add&ha_addrs=127.0.0.1:7076,127.0.0.1:7077'

accepted
```

Rebalance measurements of circle specified by circle_id after removing backends, if there is only one running InfluxDB Proxy instances

```bash
$ curl -X POST 'http://127.0.0.1:7076/rebalance?circle_id=0&operation=rm&cpus=2' -H 'Content-Type: application/json' -d \
'{
    "backends": [
        {
            "name": "influxdb-1-2",
            "url": "http://127.0.0.1:8087",
            "username": "",
            "password": ""
        }
    ]
}'

accepted
```

### `/recovery` HTTP endpoint

#### Definition

```
POST http://127.0.0.1:7076/recovery
```

#### Query string parameters

| Query String Parameter | Optional/Required | Description |
| :--------------------- | :---------------- |:---------- |
| from_circle_id=0 | Required | From circle index started from 0. |
| to_circle_id=1 | Required | To circle index started from 0. |
| backend_urls=\<backend_urls> | Optional | backend urls to recovery split by ',' while using all backends of circle specified by to_circle_id if empty. |
| db=\<db_list> | Optional | database list split by ',' while using all databases if empty. |
| cpus=1 | Optional | Migration max cpus, default is 1. |
| ha_addrs=\<ha_addrs> | Optional if only one InfluxDB Proxy instance is running | High available addrs of all running InfluxDB Proxy for two instances at least. |
| u=\<username> | Optional if you haven't enabled authentication | Set the username for authentication if you've enabled authentication. |
| p=\<password> | Optional if you haven't enabled authentication | Set the password for authentication if you've enabled authentication. |

#### Example

Recovery all backends of circle 1 from circle 0

```bash
$ curl -X POST 'http://127.0.0.1:7076/recovery?from_circle_id=0&to_circle_id=1'

accepted
```

Only recovery backend http://127.0.0.1:8089 in circle 1 from circle 0

```bash
$ curl -X POST 'http://127.0.0.1:7076/recovery?from_circle_id=0&to_circle_id=1&backend_urls=http://127.0.0.1:8089'

accepted
```

### `/resync` HTTP endpoint

#### Definition

```
POST http://127.0.0.1:7076/resync
```

#### Query string parameters

| Query String Parameter | Optional/Required | Description |
| :--------------------- | :---------------- |:---------- |
| seconds=\<seconds> | Optional | Resync data for last seconds, default is 0 for all data. |
| db=\<db_list> | Optional | database list split by ',' while using all databases if empty. |
| cpus=1 | Optional | Migration max cpus, default is 1. |
| ha_addrs=\<ha_addrs> | Optional if only one InfluxDB Proxy instance is running | High available addrs of all running InfluxDB Proxy for two instances at least. |
| u=\<username> | Optional if you haven't enabled authentication | Set the username for authentication if you've enabled authentication. |
| p=\<password> | Optional if you haven't enabled authentication | Set the password for authentication if you've enabled authentication. |

#### Example

```bash
$ curl -X POST 'http://127.0.0.1:7076/resync?cpus=2'

accepted
```

### `/clear` HTTP endpoint

#### Definition

```
POST http://127.0.0.1:7076/clear
```

#### Query string parameters

| Query String Parameter | Optional/Required | Description |
| :--------------------- | :---------------- |:---------- |
| circle_id=0 | Required | Circle index started from 0. |
| cpus=1 | Optional | Migration max cpus, default is 1. |
| ha_addrs=\<ha_addrs> | Optional if only one InfluxDB Proxy instance is running | High available addrs of all running InfluxDB Proxy for two instances at least. |
| u=\<username> | Optional if you haven't enabled authentication | Set the username for authentication if you've enabled authentication. |
| p=\<password> | Optional if you haven't enabled authentication | Set the password for authentication if you've enabled authentication. |

#### Example

```bash
$ curl -X POST 'http://127.0.0.1:7076/clear?circle_id=0&cpus=2'

accepted
```

### `/debug/pprof` HTTP endpoint

official reference:
* [/debug/pprof HTTP endpoint](https://docs.influxdata.com/influxdb/v1.7/tools/api/#debug-pprof-http-endpoint)

License
-------

MIT.
