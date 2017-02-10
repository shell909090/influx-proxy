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
import sys
import requests

BASEURL = 'http://localhost:6666'


def query(q, print_result=False):
    resp = requests.post(BASEURL+'/query', params={'db': 'test', 'q': q})
    return resp.status_code, resp.content


def main():
    if len(sys.argv) == 1:
        assert query('select * from cpu')[0] == 400
        assert query('select value from cpu')[0] == 400
        assert query('select value from cpu where time < now()')[0] == 200
    else:
        print(query(sys.argv[1])[1])


if __name__ == '__main__':
    main()
