#!/usr/bin/python
# -*- coding: utf-8 -*-
'''
@date: 2017-01-18
@author: Shell.Xu
@copyright: 2017, Shell.Xu <shell909090@gmail.com>
@license: BSD-3-clause
'''
from __future__ import absolute_import, division,\
    print_function, unicode_literals
import requests

BASEURL = 'http://localhost:6666'


def main():
    s = '''cpu,host=server01,region=uswest value=1 1434055562000000000
cpu value=3,value2=4 1434055562000010000
temperature,machine=unit42,type=assembly internal=32,external=100 1434055562000000035
temperature,machine=unit143,type=assembly internal=22,external=130 1434055562005000035'''
    resp = requests.post(
        BASEURL+'/write', params={'db': 'test'}, data=s)
    print(resp.status_code)
    print(resp.content)


if __name__ == '__main__':
    main()
