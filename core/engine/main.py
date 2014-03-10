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

""" The clique main process module.

* Loads up all necessary engine modules on startup.
"""

import os
import sys
import logging


def _setuids(clique_user):
  import pwd
  u = pwd.getpwnam(clique_user)
  if os.getuid() != u.pw_uid:
    os.setresgid(u.pw_gid, u.pw_gid, u.pw_gid)
    os.setresuid(u.pw_uid, u.pw_uid, u.pw_uid)


def _setup_fundamentals():
  """Setup the fundamentals library suited for the engine.
  """
  # TODO: disable all app-only library APIs.
  import clique.isc
  clique.isc.Endpoint.__call__ = engine.isc.__call__


def _exec_or_exit(callback):
  """Helper to execute the callback. If fails, it will terminate the whole
  program.
  """
  try:
    callback()
  except:
    import sys
    logging.critical("Failed.", exc_info=True)
    sys.exit(1)


def _run_module(mod):
  """Helper to run the module. If fail to validate, it will be ignored.

  Module has to have essential funtions.

    validate(): Validating a system before running a module.
    start(): Doing a preparation before using a module.
  """
  try:
    mod.validate()
  except:
    logging.exception("Error while validating. '%s' operation.", mod.__name__)
  _exec_or_exit(mod.start)


if __name__ == '__main__':
  import engine.runtime
  _setuids(engine.runtime.CLIQUE_USER)
  import clique
  clique._setup_logging(engine.runtime.LOG_PATH,
                        engine.runtime.MAIN_LOG_FILENAME)
  # Validating pathes
  os.path.exists(engine.runtime.RECOVERY_PATH) or os.makedirs(
      engine.runtime.RECOVERY_PATH)
  # TODO: check if there's an update file.
  # TODO: check if there's anything to do with an update.

  import rpclib
  import engine.isc
  import engine.app
  import engine.mq
  import engine.identity
  import engine.network
  import engine.node
  import engine.system
  import engine.storage
  import engine.display
  import engine.led
  import engine.progress
  import engine.backup
  import signal
  import gc
  from adt.funcs import try_execute

  logging.info("Engine starting.")
  #TODO(hdkim): do profiling cpu and memory
  gc.set_threshold(400, 10, 10)
  _setup_fundamentals()

  # The secret token for communicating with the daemon process.
  engine.aengel.TOKEN = sys.argv[1]

  def on_signal():
    try:
      try_execute(engine.app.stop)
      try_execute(engine.progress.stop)
      try_execute(engine.display.stop)
      try_execute(engine.node.stop)
      try_execute(engine.storage.stop)
      engine.runtimeis_no_display() or try_execute(engine.network.stop)
      try_execute(engine.mq.stop)
      try_execute(engine.isc.stop)
      try_execute(engine.backup.stop)
      engine.runtime.engine_server().stop()
    finally:
      rpclib.stop()

  signal.signal(signal.SIGINT, lambda x, y: clique.do(on_signal))
  signal.signal(signal.SIGTERM, lambda x, y: clique.do(on_signal))

  """
  ISC:
    Registers endpoint. - handle, register_app_endpoint, find, dump
  MQ:
    Registers endpoint. - subscribe
  NETWORK:
    Restore network connection
  STRAGE:
    Creates topic - mount.add, mount.remove
    Mounts already inserted storage.
    Starts to monitoring what mount.
  NODE:
    Registers endpoint. - hello, let_me_join, welcome, do_you_know, disconnect,
                          handshake
    Stand by to handle discorver.
    Handshakes to already handshaked nodes
    Runs a hello timer
    Creates topic - node.added, node.removed, node.connected, node.upgraded,
                    node.upgrade.failed, CONNECTION_FAILED, CONNECTION_TIMEOUT
  DISPLAY:
    Creates topic - display.connected, display.disconnected
    Runs display servers
  PROGRESS:
    Creates topic - progress.updated
  APP:
    Creates topic - app.executed, app.execute.failed, app.terminated,
                    app.terminate.failed, app.installed, app.install.failed,
                    app.uninstalled, app.uninstall.failed, app.upgraded,
                    app.upgrade.failed
  """
  _run_module(engine.isc)
  _run_module(engine.mq)
  _run_module(engine.network)
  _run_module(engine.storage)
  _run_module(engine.node)
  engine.runtime.is_no_display() or _run_module(engine.display)
  _run_module(engine.backup)
  _run_module(engine.progress)
  _run_module(engine.app)
  clique.do(lambda: logging.info("Engine started."))
  # TODO: add_callback to stop breathe!

  # TODO: fire the engine-ready event.
  startups = engine.runtime.context().engine_settings.get('common.startups')
  logging.info("Start-up service list : %s", startups)
  if startups:
    def do_startup(app_name):
      try:
        engine.app.execute(app_name)
      except:
        logging.exception("Failed to execute a '%s' app", app_name)

    clique.do(lambda: map(do_startup, startups))

  def send_msg(msg):
    engine.aengel.talk(msg)
  engine.isc.register_engine_endpoint(send_msg,
                                      namespace='engine',
                                      name='send_msg')
  try:
    # Hold the main thread until SIGINT or SIGTERM is received.
    rpclib.start()
  except:
    logging.exception("The main thread is terminating unexpectedly.")
  else:
    logging.debug("The main thread is terminating...")
  logging.info("Engine terminated.")
  sys.exit(0)
