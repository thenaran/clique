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


"""RPC testing tools.
"""

import sys
import logging
import re
import gc
from unittest import TestCase
import rpclib
import socket
import gc
from contextlib import contextmanager
from rpclib.stackcontext import raise_exc_info
from rpclib.stackcontext import StackContext
from rpclib.stackcontext import wrap
from rpclib.tornado import netutil
from threading import Timer


def bind_unused_port():
    """Binds a server socket to an available port on localhost.

    Returns a tuple (socket, port).
    """
    [sock] = netutil.bind_sockets(0, 'localhost', family=socket.AF_INET)
    port = sock.getsockname()[1]
    return sock, port


class RpcTestCase(TestCase):
  """Base test class for all tests using rpclib underneath.
  """
  DEFAULT_MAX_RUNNING_TIME = 20  # Maximum test running time.

  def __init__(self, *args, **kwargs):
    super(RpcTestCase, self).__init__(*args, **kwargs)
    self._stopped = False
    self._started = False
    self._failure = None
    self._stop_args = None
    self._timeout = None

  @contextmanager
  def _cleanup(self):
    try:
      yield
    except:
      self.set_error()

  def set_error(self):
    self._exc_info = sys.exc_info()
    self.stop()

  def timedout(self):
    raise Exception("Test timeout!")

  def setUp(self):
    TestCase.setUp(self)
    gc.set_threshold(500, 10, 10)
    self._started = False
    self._ioloop = rpclib.ioloop()
    rpclib.__IOLOOP__ = self._ioloop
    self._exc_info = None

  def tearDown(self):
    TestCase.tearDown(self)
    gc.collect()
    self.stop()

  def start(self, timeout=DEFAULT_MAX_RUNNING_TIME, condition=None):
    timer = Timer(timeout, wrap(self.timedout))
    timer.start()
    while True:
      self._started = True
      self._ioloop.start()
      if (self._exc_info is not None or condition is None or condition()):
        break

    self._started = False
    timer.cancel()
    if self._exc_info:
      raise_exc_info(self._exc_info)

    result = self._stop_args
    self._stop_args = None
    return result

  def stop(self, _arg=None, **kwargs):
    if self._started:
      self._stop_args = kwargs or _arg
      try:
        self._ioloop.add_callback(self._ioloop.stop)
        self._started = False
      except:
        pass

  def ioloop(self):
    return self._ioloop

  def run(self, result=None):
    with StackContext(self._cleanup):
      TestCase.run(self, result)


class ExpectLog(logging.Filter):
    """Context manager to capture and suppress expected log output.

    Useful to make tests of error conditions less noisy, while still
    leaving unexpected log entries visible.  *Not thread safe.*

    Usage::

        with ExpectLog('application', "Uncaught exception"):
            error_response = self.fetch("/some_page")
    """
    def __init__(self, logger, regex, required=True):
        """Constructs an ExpectLog context manager.

        :param logger: Logger object (or name of logger) to watch.  Pass
            an empty string to watch the root logger.
        :param regex: Regular expression to match.  Any log entries on
            the specified logger that match this regex will be suppressed.
        :param required: If true, an exeption will be raised if the end of
            the ``with`` statement is reached without matching any log entries.
        """
        if isinstance(logger, basestring):
            logger = logging.getLogger(logger)
        self.logger = logger
        self.regex = re.compile(regex)
        self.required = required
        self.matched = False

    def filter(self, record):
        message = record.getMessage()
        if self.regex.match(message):
            self.matched = True
            return False
        return True

    def __enter__(self):
        self.logger.addFilter(self)

    def __exit__(self, typ, value, tb):
        self.logger.removeFilter(self)
        if not typ and self.required and not self.matched:
            raise Exception("did not get expected log message")

