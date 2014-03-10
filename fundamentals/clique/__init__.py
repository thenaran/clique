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


VERSION = "0.19.140103"
# More detailed version data (major, minor, release timestamp).
VERSION_INFO = tuple(map(int, VERSION.split('.')))


import logging
import os
from adt.concurrency import Lazy
from adt.concurrency import Future
import rpclib


LOG_PATH = '''/var/log/clique'''
LOG_LEVEL = '''debug'''
LOG_MAX_BYTES = 10485760   # 10MB
LOG_BACKUP_COUNT = 1
__DATA__ = Lazy()


def _setup_logging(log_dirpath, log_filename):
  """Setup logging.
  """
  import logging.handlers
  log_levels = {'debug': logging.DEBUG,
                'info': logging.INFO,
                'warning': logging.WARNING,
                'error': logging.ERROR,
                'critical': logging.CRITICAL}
  level = log_levels[LOG_LEVEL]
  formatter = logging.Formatter("%(created)f\t%(levelname)5s\t"
                                "%(thread)d\t%(lineno)d\t%(module)s\t"
                                "%(funcName)s\t%(message)s")
  logging.getLogger().setLevel(level)

  if not os.path.exists(log_dirpath):
    os.makedirs(log_dirpath)
  log_filepath = os.path.join(log_dirpath, log_filename)
  handler = logging.handlers.RotatingFileHandler(log_filepath,
                                                 maxBytes=LOG_MAX_BYTES,
                                                 backupCount=LOG_BACKUP_COUNT)
  handler.setFormatter(formatter)
  logging.getLogger().addHandler(handler)


def _config_changed_callback(topic, msg):
  import clique.isc
  import clique.runtime
  logging.debug("CC config changed event %s", msg)
  app_name = clique.runtime.app_name()
  setting_values = msg.get(app_name)
  setting_values and clique.runtime.config().call_changed_handler(
      setting_values)


def configure(blocking=False):
  """Configure the clique APIs to use. It must be called at the application
  startup if desired to use the clique APIs (mq, endpoints, etc.).
  """
  import clique.runtime
  try:
    _setup_logging(LOG_PATH, clique.runtime.app_name())
  except:
    import traceback
    traceback.print_exc()

  import clique.isc
  clique.isc._restore = None
  import clique.mq
  import atexit
  import signal

  def register_cb(results):
    if results and len(results) == 2:
      _, event_ep = results
      ep = clique.iscEndpoint('mq', 'subscribe',
                              node_id=clique.runtime.node_id())
      ep('app.settings.changed', event_ep)
    else:
      logging.debug("fail to start events result:%s", str(results))

  def start():
    clique.isc.start()
    clique.mq.start()
    clique.isc.register_endpoint(_config_changed_callback,
                                 namespace='config.changed').then(
                                     register_cb)
    logging.info("clique application %s started.", clique.runtime.app_name())
    rpclib.start()
    logging.info("clique application %s terminated.", clique.runtime.app_name())

  @atexit.register
  def terminate():
    logging.info("clique application %s terminating...",
                 clique.runtime.app_name())
    do(clique.isc.stop)
    do(clique.mq.stop)
    rpclib.stop()

  signal.signal(signal.SIGINT, lambda x, y: do(terminate))
  signal.signal(signal.SIGTERM, lambda x, y: do(terminate))

  if blocking:
    start()
  else:
    from threading import Thread
    __DATA__.io_thread = Thread(target=start)
    __DATA__.io_thread.start()


def wait():
  """Wait until the main IO loop thread finishes. It works only if the clique
  library has been configured in a non-blocking way (blocking=False)
  """
  __DATA__.io_thread.join()
  del __DATA__.io_thread


def do(callback, *args, **kwargs):
  """Convenient function to execute the `callback` in the main IO loop.
  It returns a `Future` if the return value of the `callback` is expected::

    clique.do(some_work).then(work2).then(work3)

  The `callback` accepts no argument.
  """
  f = Future()

  def do_callback():
    try:
      value = callback(*args, **kwargs)
      if isinstance(value, Future):
        value.then(f.set_value)
      else:
        f.set_value(value)
    except Exception, e:
      f.set_value(e)
  ioloop().add_callback(do_callback)
  return f


def ioloop():
  """Main IO loop.
  """
  return rpclib.ioloop()
