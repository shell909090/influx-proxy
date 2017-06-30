#!/usr/bin/python
# -*- coding: utf-8 -*-
'''
@date: 2017-01-24
@author: Shell.Xu
@copyright: 2017, Eleme <zhixiang.xu@ele.me>
@license: MIT
'''
from __future__ import absolute_import, division,\
    print_function, unicode_literals
import sys
import getopt
import redis


# backends key use for KEYMAPS, NODES, cache file
# url: influxdb addr or other http backend which supports influxdb line protocol
# db: influxdb db 
# zone: same zone first query
# interval: default config is 1000ms, wait 1 second write whether point count has bigger than maxrowlimit config
# timeout: default config is 10000ms, write timeout until 10 seconds
# timeoutquery: default config is 600000ms, query timeout until 600 seconds
# maxrowlimit: default config is 10000, wait 10000 points write 
# checkinterval: default config is 1000ms, check backend active every 1 second
# rewriteinterval: default config is 10000ms, rewrite every 10 seconds
BACKENDS = {
    'local': {
        'url': 'http://localhost:8086', 
        'db': 'test', 
        'zone':'local', 
        'interval': 1000,
        'timeout': 10000, 
        'timeoutquery':600000, 
        'maxrowlimit':10000,  
        'checkinterval':1000, 
        'rewriteinterval':10000,
    },
    'local2': {
        'url': 'http://influxdb-test:8086',
        'db': 'test2',
        'interval': 200,
    },
}

# measurement:[backends keys], the key must be in the BACKENDS
# data with the measurement will write to the backends
KEYMAPS = {
    'cpu': ['local'],
    'temperature': ['local2']
}

# this config will cover default_node config
# listenaddr: proxy listen addr                
# db: proxy db, client's db must be same with it
# zone: use for query
# nexts: the backends keys, will accept all data, split with ','
# interval: collect Statistics
# idletimeout: keep-alives wait time 
NODES = {
    'l1': { 
        'listenaddr': ':6666',
        'db': 'test',
        'zone': 'local',
        'interval':10,
        'idletimeout':10,
    }
}

# the influxdb default cluster node 
DEFAULT_NODE = {
    'listenaddr': ':6666'
}


def cleanups(client, parttens):
    for p in parttens:
        for key in client.keys(p):
            client.delete(key)


def write_configs(client, o, prefix):
    for k, l in o.items():
        if hasattr(l, 'items'):
            for f, v in l.items():
                client.hset(prefix+k, f, v)
        elif hasattr(l, '__iter__'):
            for i in l:
                client.rpush(prefix+k, i)


def write_config(client, d, name):
    for k, v in d.items():
        client.hset(name, k, v)


def main():
    optlist, args = getopt.getopt('d:hH:p:', sys.argv[1:])
    optdict = dict(optlist)
    if '-h' in optdict:
        print(main.__doc__)
        return

    client = redis.StrictRedis(
        host=optdict.get('-H', 'localhost'),
        port=int(optdict.get('-p', '6379')),
        db=int(optdict.get('-d', '0')))

    cleanups(client, ['default_node', 'b:*', 'm:*', 'n:*'])

    write_config(client, DEFAULT_NODE, "default_node")
    write_configs(client, BACKENDS, 'b:')
    write_configs(client, NODES, 'n:')
    write_configs(client, KEYMAPS, 'm:')


if __name__ == '__main__':
    main()
