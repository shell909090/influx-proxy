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


BACKENDS = {
    'local': {
        'url': 'http://localhost:8086',
        'db': 'test',
        'interval': 200
    },
    'local2': {
        'url': 'http://localhost:8086',
        'db': 'test2',
        'interval': 200
    },
}

KEYMAPS = {
    'cpu': ['local'],
    'temperature': ['local2']
}

NODES = {
    'l1': {
        'db': 'test',
        'zone': 'local'
    }
}

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
