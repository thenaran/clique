# -*- coding: utf-8 -*-
#
# Copyright 2012-2013 Narantech Inc. All rights reserved.
#  __    _ _______ ______   _______ __    _ _______ _______ _______ __   __
# |  |  | |   _   |    _ | |   _   |  |  | |       |       |       |  | |  |
# |   |_| |  |_|  |   | || |  |_|  |   |_| |_     _|    ___|       |  |_|  |
# |       |       |   |_||_|       |       | |   | |   |___|       |       |
# |  _    |       |    __  |       |  _    | |   | |    ___|      _|       |
# | | |   |   _   |   |  | |   _   | | |   | |   | |   |___|     |_|   _   |
# |_|  |__|__| |__|___|  |_|__| |__|_|  |__| |___| |_______|_______|__| |__|


""" RPC library.
"""

from rpclib.tornado.ioloop import IOLoop
from contextlib import contextmanager
import threading
import logging


__IOLOOP__ = None
__ALL_LOOPS__ = []
__CONTEXTS__ = threading.local()


def _get_io_loop():
  global __IOLOOP__
  if not __IOLOOP__:
    __IOLOOP__ = new_ioloop()
  return __IOLOOP__


def start():
  """Starts the RPC IO loop.
  """
  __IOLOOP__ = _get_io_loop()
  __IOLOOP__.start()


def stop():
  """Stops the RPC IO loop.
  """
  __IOLOOP__.stop()


def ioloop():
  """Gets the main RPC IO loop.
  """
  return _get_io_loop()


def new_ioloop():
  """Creates a new RPC IO loop.
  """
  #TODO: implement ioloop adapter to close ioloop and handle resources in close method like close_ioloop method so that we shouldn't call close_ioloop to close ioloop, it make it possible just to call ioloop.close() and others
  newloop = IOLoop()
  __ALL_LOOPS__.append(newloop)
  return newloop


def close_ioloop(ioloop):
  """ Closes a ioloop and remove from list
  """
  try:
    __ALL_LOOPS__.remove(ioloop)
    ioloop.close()
  except:
    logging.exception("Error while closeing the IO loop.")


def terminate():
  """Terminate all instantiated IO loops.
  """
  while __ALL_LOOPS__:
    loop = __ALL_LOOPS__.pop()
    try:
      loop.close()
    except:
      logging.exception("Error while closing the IO loop.")


def _set_context(context):
  __CONTEXTS__.context = context


@contextmanager
def context():
  yield __CONTEXTS__.context
