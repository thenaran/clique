# coding: utf-8
from rpclib.msgpack._version import version
from rpclib.msgpack._msgpack import *

# alias for compatibility to simplejson/marshal/pickle.
load = unpack
loads = unpackb

dump = pack
dumps = packb
