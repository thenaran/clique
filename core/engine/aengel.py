#!/usr/bin/env python
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


"""Daemon(or angel) process watching the main process and help execute
root-privileged commands. It must be executed as root.

It must have a permission set of 4511, owner=root, group=clique.

TODO
1. start clique process if not started yet.
1. watch if the clique process terminates, and start it again if it terminates.
1. update clique API.
1. execute remote function as root.
"""

from engine.runtime import ROOT_PATH
from engine.runtime import BASE_PATH
from engine.runtime import DATA_PATH
import rpclib.proxy
from rpclib.proxy import SingleRpcProxy


__PATH__ = DATA_PATH + '''/.aengel'''
__AENGEL_VAR_PATH__ = ROOT_PATH + '''var/aengel'''
__PRERUN_PATH__ = DATA_PATH + '''/prerun'''
__NO_PRERUN_PATH__ = __PRERUN_PATH__ + '''/.no_prerun'''
__RECOVERY_PATH__ = __AENGEL_VAR_PATH__ + '''/recovery'''
__SIGN_PATH__ = __AENGEL_VAR_PATH__ + '''/clique.run'''
__BACKUP_PREFIX__ = '''.clique.bak'''
TOKEN = None
__LONG_INTERVAL__ = 1800


def execute_cmd(cmd, timeout=None):
  """Execute the given command and return the result.
  """
  if timeout:
    p = SingleRpcProxy(__PATH__, timeout=timeout)
  else:
    p = SingleRpcProxy(__PATH__)
  return rpclib.proxy.sync(p.execute_cmd)(TOKEN, cmd)


def execute_cmd_async(cmd, timeout=None):
  """Execute the given command and return the future of an asynchronous result.
  """
  if timeout:
    p = SingleRpcProxy(__PATH__, timeout=timeout)
  else:
    p = SingleRpcProxy(__PATH__)
  return p.execute_cmd(TOKEN, cmd)


def reset(factory=False):
  """If `factory` is False, it only deletes applications and contents (no FTL
  is shown up). If True, it will default to the factory settings and display
  the FTL as if it's first time configuring the system.
  """
  p = SingleRpcProxy(__PATH__)
  return rpclib.proxy.sync(p.reset)(TOKEN, factory)


def reboot():
  """Reboot the clique system
  """
  p = SingleRpcProxy(__PATH__)
  return rpclib.proxy.sync(p.reboot)(TOKEN)


def restart():
  """Restart the clique process.
  """
  p = SingleRpcProxy(__PATH__)
  return rpclib.proxy.sync(p.restart)(TOKEN)


def upgrade(upgrade_path, progress_ident):
  """upgrade a clique
  """
  p = SingleRpcProxy(__PATH__, timeout=__LONG_INTERVAL__)
  return rpclib.proxy.sync(p.upgrade)(TOKEN, upgrade_path, progress_ident)


def talk(message):
  """Send a logging message to aengle
  """
  p = SingleRpcProxy(__PATH__)
  return rpclib.proxy.sync(p.talk)(TOKEN, message)


def restore(version):
  """Restore a clique
  """
  p = SingleRpcProxy(__PATH__)
  return rpclib.proxy.sync(p.restore)(TOKEN, version)


def remove_files(files_path):
  """Removes files
  """
  p = SingleRpcProxy(__PATH__)
  return rpclib.proxy.sync(p.remove)(TOKEN, files_path)


if __name__ == '__main__':
  import engine.led
  engine.led.setup()

  # Default python libraries
  import os
  import sys
  import shutil
  import random
  import subprocess
  import logging
  import logging.handlers
  import time
  import psutil
  import signal
  from subprocess import Popen
  from subprocess import PIPE
  from threading import Thread
  from StringIO import StringIO

  # Clique libraries
  import engine
  import engine.appinstall
  import rpclib
  from rpclib.server import RpcServerOverUnixSocket
  from adt.concurrency import Lazy
  from adt.concurrency import Future
  from adt.concurrency import HotPotato
  from adt.concurrency import CommandExecuter
  from engine.runtime import LOG_PATH
  from engine.runtime import CLIQUE_USER

  def _do_execute_cmd(cmd):
    try:
      logging.info("executing the command as root: %s", cmd)
      return subprocess.check_output(cmd, shell=True)
    except:
      logging.warn("error while executing the command.", exc_info=True)
      raise

  def _set_recovery(is_active=True):
    check_it_script = '00checkit'
    force_file_system_check_file = '/forcefsck'
    try:
      cmd_io = StringIO()
      if is_active:
        cmd_io.write('update-rc.d %s defaults;' % check_it_script)
        cmd_io.write('touch %s;' % __NO_PRERUN_PATH__)
        cmd_io.write('echo $(date -u +%s) > ')
        cmd_io.write(force_file_system_check_file)
      else:
        cmd_io.write('update-rc.d -f %s remove;' % check_it_script)
        cmd_io.write('rm -f %s;' % __NO_PRERUN_PATH__)
        cmd_io.write('rm -f %s' % force_file_system_check_file)
      _do_execute_cmd(cmd_io.getvalue())
      cmd_io.close()
    except:
      logging.warn("error while set recovery scripts.", exc_info=True)

  if os.path.exists(__PRERUN_PATH__):
    _do_execute_cmd('rm -rf {prerun_path}'.format(prerun_path=__PRERUN_PATH__))
  os.makedirs(__PRERUN_PATH__)
  os.path.exists(__RECOVERY_PATH__) or os.makedirs(__RECOVERY_PATH__)
  _set_recovery()

  # Globals
  restart_interval = 5
  terminate_max_wait_time = 10

  # Standby log
  level = (len(sys.argv) >= 2 and sys.argv[1] == '--debug') and \
      logging.DEBUG or logging.INFO
  formatter = logging.Formatter(
      "%(created)f\t%(levelname)5s\t"
      "%(thread)d\t%(lineno)d\t%(module)s\t%(funcName)s\t%(message)s")
  logging.getLogger().setLevel(level)
  handler = logging.handlers.RotatingFileHandler(ROOT_PATH + 'var/log/'
                                                 'aengel.log',
                                                 maxBytes=10485760,
                                                 backupCount=1)
  handler.setFormatter(formatter)
  logging.getLogger().addHandler(handler)

  if not os.path.exists(LOG_PATH):
    os.makedirs(LOG_PATH)
    _do_execute_cmd('chown %s:%s %s' % (CLIQUE_USER, CLIQUE_USER, LOG_PATH))
    logging.info("Created the log directory for the engine.")

  def led_watch(p):
    ret = p.wait()
    logging.warn("led process returned! : %s", str(ret))
    try:
      time.sleep(restart_interval)
    except:
      logging.warn("error while preparing for another startup.", exc_info=True)
    finally:
      led_startup()

  def led_startup():
    """Start up the led process.
    """
    token = str(random.random())
    p = Popen([sys.executable, os.path.join(BASE_PATH, 'engine', 'led.pyc'),
              token])
    logging.info("started up the led process. pid=%s", str(p.pid))
    Thread(target=led_watch, args=(p,)).start()

  # start led
  led_startup()

  if os.geteuid() != 0:
    print >> sys.stderr, "Aengel must be running as root."
    engine.led.fatal_error()
    sys.exit(1)

  first_py_path = os.path.join(BASE_PATH, 'engine', 'first.pyc')
  # Execute the first.py if exists.
  if os.path.exists(first_py_path):
    logging.info("Executing the first.py module.")
    try:
      cmd = "%s %s" % (sys.executable, first_py_path)
      output = _do_execute_cmd(cmd)
      logging.info(output)
    except:
      logging.exception("Error while executing the first module.")
      engine.led.fatal_error()
      raise

  context = Lazy()
  context.current_process = None
  context.upgrader = None
  context.token = None
  context.hotpotato = None
  context.processes = set(psutil.get_pid_list())
  context.upgrading = False

  def check_token(token):
    """Helper to check the token validity.
    """
    if context.token and context.token != token:
      raise Exception("Token '%s' is mismatch!", token)

  def ensure_no_leftovers():
    """Helper to ensure there are no leftover processes.
    """
    procs = set(psutil.get_pid_list())
    leftovers = procs - context.processes
    failures = set()
    if len(leftovers) > 0:
      logging.info("killing all leftover processes... %s", str(leftovers))
      for pid in leftovers:
        try:
          _do_execute_cmd('kill -9 %d' % pid)
        except:
          failures.add(pid)
          logging.warn("error while killing the leftover with pid=%d", pid,
                       exc_info=True)
      if len(failures) > 0:
        logging.error("failed to kill all leftover processes... failures=%s",
                      str(failures))
    else:
      logging.info("no leftover processes.")

  class StoreKeeper(object):

      includes = [
          'bin/',
          'boot/',
          'core/',
          'data/',
          'etc/',
          'fundamentals/',
          'lib/',
          'opt/',
          'sbin/',
          'selinux/',
          'srv/',
          'usr/',
          'var/',
      ]

      excludes = [
          __RECOVERY_PATH__[1:] + '/',
          'var/clique/',
          'var/store/',
          'var/shell/',
          'var/run/',
          'var/log/',
          'etc/fake-hwclock.data',
      ]

      @classmethod
      def build_backup_filename(cls, version):
        return version + '.clique.bak'

      @classmethod
      def build_backup_path(cls, version):
        return os.path.join(__RECOVERY_PATH__,
                            cls.build_backup_filename(version))

      def __init__(self, version, progress_ident='clique_backup'):
        self._version = version
        self._progress_ident = progress_ident
        self._executers = set()

      def start(self):
        """ Validates back-up.
        Makes a back-up of the current clique version, if not exists.
        Old version back-up will be removed.
        """
        from glob import glob
        # Ready to create backup file for clique
        data = Lazy()
        data.cur_bak = self.build_backup_filename(self._version)
        data.bak_path = os.path.join(__RECOVERY_PATH__, data.cur_bak)
        data.work_path = os.path.join(__RECOVERY_PATH__, '_' + data.cur_bak)
        data.baks = glob(os.path.join(__RECOVERY_PATH__,
                                      self.build_backup_filename('*')))
        if data.bak_path not in data.baks:
          cmd_io = StringIO()
          cmd_io.write('tar cvzfp ')
          cmd_io.write(data.work_path)
          for path in self.includes:
            cmd_io.write(' ')
            cmd_io.write(ROOT_PATH)
            cmd_io.write(path)
          for path in self.excludes:
            cmd_io.write(' --exclude=')
            cmd_io.write(ROOT_PATH)
            cmd_io.write(path)
            cmd_io.write('*')

          cmd = cmd_io.getvalue()
          cmd_io.close()
          # compress backup file for clique

          def out_cb(msg):
            pass

          def ret_cb(v):
            if isinstance(v, Exception) or v not in [0, None]:
              logging.warn("Failed to create a back-up for '%s' version...",
                           self._version, exc_info=True)
            else:
              os.rename(data.work_path, data.bak_path)
              logging.warn("Completed to create a back-up for '%s' version...",
                           self._version)
              for path in data.baks:
                os.remove(path)

          return CommandExecuter(self._executers).do(
              cmd, out_cb, 20).then(ret_cb)

      def restore(self):
        engine.led.breathe(speed=8, count=1000)
        bak_path = self.build_backup_path(self._version)
        if not os.path.exists(bak_path):
          logging.info("Cannot recovery a clique because a back-up file not "
                       "exists. : %s", bak_path, exc_info=True)
          return Future(False)

        def out_cb(msg):
          pass

        def ret_cb(v):
          if isinstance(v, Exception) or v not in [0, None]:
            logging.warn("Failed to restore a back-up for '%s' version...",
                         self.version, exc_info=True)

        cmd = 'tar xvzfp %s -C %s' % (bak_path, ROOT_PATH)
        return CommandExecuter(self._executers).do(
            cmd, out_cb).then(ret_cb)

  def _do_restore(version):
    storekeeper = StoreKeeper(version)
    return storekeeper.restore()

  def _validate():
    if os.path.exists(Upgrader.flag_path):
      with open(Upgrader.flag_path, 'r') as rf:
        version = rf.readline().strip()
        logging.info("Restoring a clique because it is shutdowned while"
                     " upgrading for '%s'.", version)
        upgrade_path = rf.readline().strip()

      def ret_cb(v):
        if v is True:
          _do_reboot()
        return v

      return _do_upgrade(upgrade_path).then(ret_cb)
    else:
      return Future(True)

  def _sign():
    with open(__SIGN_PATH__, 'w') as wf:
      wf.write('{pid},{version},{timestamp}'.format(
          pid=context.current_process.pid, version=engine.VERSION,
          timestamp=time.time()))

  def do_startup(v):
    token = str(random.random())
    p = Popen([sys.executable, os.path.join(BASE_PATH, 'engine', 'main.pyc'),
              token], stdin=PIPE)
    logging.info("started up the clique process. pid=%s", str(p.pid))
    context.current_process = p
    context.token = token
    _sign()
    Thread(target=watch, args=(p,)).start()

  def startup():
    """Start up the clique process.
    """
    try:
      _validate().then(do_startup)
    except:
      logging.exception("Failed to validate a clique.")
      do_startup(True)

  def watch(p):
    storekeeper = StoreKeeper(engine.VERSION)
    storekeeper.start()
    ret = p.wait()
    logging.warn("clique process returned! : %s", str(ret))
    if context.hotpotato:
      context.hotpotato.stop()
      context.hotpotato = None
    context.current_process = None
    try:
      time.sleep(restart_interval)
      ensure_no_leftovers()
    except:
      logging.warn("error while preparing for another startup.", exc_info=True)
    finally:
      startup()

  def send_msg(msg):
    if context.current_process:
      context.current_process.stdin.write("%s\n" % msg)
    else:
      logging.warn("Process is not exists.", exc_info=True)

  def do_execute_cmd(token, cmd):
    check_token(token)
    return _do_execute_cmd(cmd)

  class Upgrader(object):
    flag_path = os.path.join(__RECOVERY_PATH__, 'upgrade_clique')
    post_script_path = os.path.join(ROOT_PATH, '/post_upgrade.sh')

    def __init__(self, upgrade_path, progress_ident):
      logging.debug("[s]init:\n%s", upgrade_path)
      try:
        self._upgrade_path = os.path.join(__RECOVERY_PATH__,
                                          os.path.basename(upgrade_path))
        if upgrade_path != self._upgrade_path:
          os.rename(upgrade_path, self._upgrade_path)
        self._deleted_files = set()
        self._progress_ident = progress_ident
        self._executers = set()
        self._ongoing = True
      except:
        _do_execute_cmd('rm -rf %s' % self._upgrade_path)
        raise
      logging.debug("[c]init:\n%s", upgrade_path)

    def _prepare(self):
      ret = True
      try:
        if not self._ongoing:
          logging.info("Stoped a upgrade before preparing.")
          return Future(True)
        logging.debug("[s]prepare:\n%s\n%s\n%s", self.flag_path,
                      engine.VERSION, self._upgrade_path)
        flag_io = StringIO()
        flag_io.write(engine.VERSION)
        flag_io.write('\n')
        flag_io.write(self._upgrade_path)
        flag_io.write('\n')
        flag_io.close()
        logging.debug("[c]prepare:\n%s\n%s\n%s", self.flag_path,
                      engine.VERSION, self._upgrade_path)
      except Exception as e:
        logging.exception("Failed to prepare while upgrading.")
        ret = e
      finally:
        return Future(ret)

    def _uncompress(self, v):
      if isinstance(v, Exception):
        return v
      if not self._ongoing:
        logging.info("Stoped a upgrade before uncompressing.")
        return Future(True)
      logging.debug("[s]uncompres:\n%s", self._upgrade_path)

      def out_cb(msg):
        if isinstance(msg, Exception):
          logging.warn("Failed to uncompress while upgrading, %s", msg,
                       exc_info=True)
          return
        logging.info(msg)

      def ret_cb(v):
        if isinstance(v, Exception):
          logging.warn("Failed to uncompress while upgrading, %s", v,
                       exc_info=True)
          return v
        else:
          logging.debug("[c]uncompress:\n%s", self._upgrade_path)

      if self._upgrade_path.endswith('/'):
        name = os.path.basename(self._upgrade_path[:-1])
      else:
        name = os.path.basename(self._upgrade_path)
      cmd = 'cd {path};cat {name}.tgz* | tar xvzfp - -C /'.format(
          path=self._upgrade_path, name=name)
      return CommandExecuter(self._executers).do(cmd, out_cb).then(
          ret_cb)

    def _del_files(self, v):
      if isinstance(v, Exception):
        return v
      try:
        if self._deleted_files:
          logging.debug("[s]delete:\n%d", len(self._deleted_files))
          for deleted_file in self._deleted_files:
            try:
              if not self._ongoing:
                logging.info("Stoped a upgrade.")
                return Future(True)
              _do_execute_cmd('rm -rf %s' % deleted_file)
            except Exception as e:
              logging.warn(e, exc_info=True)
          logging.debug("[c]delete:\n%d", len(self._deleted_files))
        else:
          logging.debug("[c]delete:\n%d", len(self._deleted_files))
      except Exception as e:
        logging.exception("Failed to delete while upgrading")
        return e

    def _run_post_script(self, v):
      if isinstance(v, Exception):
        return v
      try:
        if os.path.exists(self.post_script_path):
          cmd = '/bin/sh {script_path} {version}'.format(
              script_path=self.post_script_path,
              version=engine.VERSION)
          _do_execute_cmd(cmd)
          os.remove(self.post_script_path)
      except Exception as e:
        logging.exception("Failed to run a post script while upgrading")
        return e

    def _clean(self, v):
      os.path.exists(self._upgrade_path) and shutil.rmtree(self._upgrade_path)
      os.path.exists(self.flag_path) and os.remove(self.flag_path)

    def _finish(self, v):
      # clean upgrade data
      if isinstance(v, Exception):
        logging.warn("Failed to upgrade a clique.", exc_info=True)
        ret = v
      else:
        logging.info("Completed to upgrade a clique.")
        ret = True
      return ret

    def start(self):
      logging.info("Starting upgrade a clique...")
      return self._prepare().then(self._uncompress).then(self._del_files).then(
          self._run_post_script).then(self._clean).then(self._finish)

    def cancel(self):
      for execute in self._executers:
        execute.process.kill()
      self._ongoing = False

  def do_cancel_upgrade(token):
    check_token(token)
    if context.upgrader:
      return context.upgrader.cancel()
    else:
      logging.info("Do not upgrading a clique now.")

  def _do_upgrade(upgrade_path, progress_ident='upgrade_clique'):
    """Update the clique process.
    1. back up the current version.
    1. copy the updated version.
    """
    logging.debug("[s]upgrade:\n%s\n%s", upgrade_path, progress_ident)

    def ret_cb(v):
      logging.debug("[f]upgrade:\n%s\n%s\n%s", upgrade_path,
                    progress_ident, v)
      context.upgrader = None
      return v
    if os.path.exists(upgrade_path):
      context.upgrader = Upgrader(upgrade_path, progress_ident)
      return context.upgrader.start().then(ret_cb)
    else:
      logging.info("clique upgrade requested but no upgrade is available.")
      return Future(False)

  def do_upgrade(token, upgrade_path, progress_ident):
    check_token(token)
    """Update the clique process.
    1. back up the current version.
    1. copy the updated version.
    """
    return _do_upgrade(upgrade_path, progress_ident)

  def do_reset(token, factory):
    check_token(token)
    logging.info("Start setting... factory=%s", str(factory))
    pass  # TODO: impl.

  def do_restart(token):
    check_token(token)
    logging.info("Restarting the clique...")
    context.current_process.terminate()
    context.hotpotato = HotPotato(rpclib.ioloop(), terminate_max_wait_time,
                                  context.current_process.kill)
    context.hotpotato.start()

  def _do_reboot():
    logging.info("Reboot the clique system...")
    try:
      _set_recovery(False)
      # -r: reboot after shutdown
      # -F: Force fsck on reboot
      _do_execute_cmd('halt;shutdown -rF now')
    except:
      logging.exception("Failed to reboot.")
      raise

  def do_reboot(token):
    check_token(token)
    _do_reboot()

  def do_restore(token, version):
    check_token(token)
    logging.info("restore the clique system...")
    return _do_restore(version)

  def do_talk(token, message):
    check_token(token)
    logging.info(message)

  def do_remove_files(token, files_path):
    check_token(token)
    for path in files_path:
      _do_execute_cmd(path)
    return True

  signal.signal(signal.SIGINT, lambda x, y: do_restart(context.token))
  signal.signal(signal.SIGTERM, lambda x, y: do_restart(context.token))

  server = RpcServerOverUnixSocket(path=__PATH__)
  server.register_function(do_execute_cmd, name='execute_cmd')
  server.register_function(do_upgrade, name='upgrade')
  server.register_function(do_reset, name='reset')
  server.register_function(do_reboot, name='reboot')
  server.register_function(do_restart, name='restart')
  server.register_function(do_talk, name='talk')
  server.register_function(do_restore, name='restore')
  server.register_function(do_remove_files, name='remove_files')
  server.serve()
  _do_execute_cmd('chown %s:%s %s' % ('root', CLIQUE_USER, __PATH__))
  startup()
  rpclib.start()  # blocking
