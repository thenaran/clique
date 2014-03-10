#!/usr/bin/env python
# coding: utf-8

from nose import main
from nose.tools import *
from rpclib.msgpack import packb, unpackb


def test_unpack_buffer():
    from array import array
    buf = array('b')
    buf.fromstring(packb(('foo', 'bar')))
    obj = unpackb(buf, use_list=1)
    assert_equal([b'foo', b'bar'], obj)

if __name__ == '__main__':
    main()
