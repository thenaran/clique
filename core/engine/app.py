# -*- coding: utf-8 -*-
# Copyright 2012-2013 Narantech Inc. All rights reserved.
#  __    _ _______ ______   _______ __    _ _______ _______ _______ __   __
# |  |  | |   _   |    _ | |   _   |  |  | |       |       |       |  | |  |
# |   |_| |  |_|  |   | || |  |_|  |   |_| |_     _|    ___|       |  |_|  |
# |       |       |   |_||_|       |       | |   | |   |___|       |       |
# |  _    |       |    __  |       |  _    | |   | |    ___|      _|       |
# | | |   |   _   |   |  | |   _   | | |   | |   | |   |___|     |_|   _   |
# |_|  |__|__| |__|___|  |_|__| |__|_|  |__| |___| |_______|_______|__| |__|


""" Application management module.
Clique application includes service and library.
A service application is a running program just like any other server
applications on other platforms. A library application does not have an entry
executable file (therefore cannot be executed).
"""


# Python Default Libraries
import psutil
import logging
import time
import os
from threading import Thread
from subprocess import PIPE
from cStringIO import StringIO

# Clique related Libraries
import clique
import engine
import engine.appinstall
import engine.runtime
import engine.gpio as gpio
import engine.mq
import engine.isc
import engine.network
import engine.aengel
import engine.progress
import engine.led as led
from engine.runtime import APPS_PATH
from engine.runtime import FUNDAMENTALS_PATH
from engine.runtime import ROOT_PATH
from engine.runtime import LOG_PATH
from engine.runtime import CLIQUE_USER
from engine.runtime import APP_INFO_FILE_NAME
from engine.runtime import APP_CONFIG_FILE_NAME
from engine.isc import endpoint
from engine.isc import PRIVILEGED
from rpclib.proxy import Base
from rpclib.proxy import FileToBinaryCopier
from rpclib.proxy import Binary
from clique.runtime import CONTEXT_FILEPATH
from adt.concurrency import Lazy
from adt.concurrency import Future
from adt.concurrency import Timer


# application configure
__APP_ENV_PACKAGE_FILENAME__ = '''appbase'''
__APP_INFO_SUBPATH__ = '''app/%s''' % APP_INFO_FILE_NAME
__APP_ICON_SUBPATH__ = '''app.ico'''

# app runtime data
__DATA__ = Lazy()
__DATA__.add_initializer('sandbox_path',
                         lambda: engine.runtime.context().rebase('sandbox'))
__DATA__.add_initializer('appbase',
                         lambda: engine.runtime.context().rebase_to_data(
                         'appbase'))
# map: app_name -> app object
__DATA__.add_initializer('apps', lambda: _collect_installed_apps())
__DATA__.add_initializer('recovery_dir', lambda: _init_recovery_dir())


# App state
READY_STATE = 0
START_STATE = 1
RUNNING_STATE = 2
TERMINATING_STATE = 3
INSTALLING_STATE = 4
UNINSTALLING_STATE = 5
UPGRADING_STATE = 6
DOWNLOADING_STATE = 10


# App Topics
APP_EXECUTED_TOPIC = '''app.executed'''
APP_EXECUTE_FAILED_TOPIC = '''app.execute.failed'''
APP_TERMINATED_TOPIC = '''app.terminated'''
APP_TERMINATE_FAILED_TOPIC = '''app.terminate.failed'''
APP_INSTALLED_TOPIC = '''app.installed'''
APP_INSTALL_FAILED_TOPIC = '''app.install.failed'''
APP_UNINSTALLED_TOPIC = '''app.uninstalled'''
APP_UNINSTALL_FAILED_TOPIC = '''app.uninstall.failed'''
APP_UPGRADED_TOPIC = '''app.upgraded'''
APP_UPGRADE_FAILED_TOPIC = '''app.upgrade.failed'''
APP_SETTINGS_CHANGED_TOPIC = '''app.settings.changed'''


__LONG_TIMEOUT_INTERVAL__ = 3600


# Terminate configure
__TIMEOUT_INTERVAL__ = 10


# Resources
SCREEN_RESOURCE = '''display'''
STORAGE_RESOURCE = '''storage'''
GPIO_RESOURCE = '''gpio'''
RESOURCE_SEPARATOR = ''':'''

# mount list in app
__REQUIRED_MOUNT_PATHS__ = ['proc', 'dev', FUNDAMENTALS_PATH[1:]]
__OPTIONAL_MOUNT_PATHS__ = {'display': ['opt/vc'],
                            'gpio': ['sys/class/gpio',
                                     'sys/devices/virtual/gpio']}


__LIMIT_COMMAND_SIZE__ = 100000


def validate():
  pass


def start():
  """
  Note:
    Edits comment of start function in main.py, if edit it.
  """
  engine.mq.create_topic(APP_EXECUTED_TOPIC)
  engine.mq.create_topic(APP_EXECUTE_FAILED_TOPIC)
  engine.mq.create_topic(APP_TERMINATED_TOPIC)
  engine.mq.create_topic(APP_TERMINATE_FAILED_TOPIC)
  engine.mq.create_topic(APP_INSTALLED_TOPIC)
  engine.mq.create_topic(APP_INSTALL_FAILED_TOPIC)
  engine.mq.create_topic(APP_UNINSTALLED_TOPIC)
  engine.mq.create_topic(APP_UNINSTALL_FAILED_TOPIC)
  engine.mq.create_topic(APP_UPGRADED_TOPIC)
  engine.mq.create_topic(APP_UPGRADE_FAILED_TOPIC)
  engine.mq.create_topic(APP_SETTINGS_CHANGED_TOPIC)

  try:
    for app_info in __DATA__.apps.values():
      _set_up_with_app_info(app_info)

    flags_path = _get_upgrade_flags()
    for path in flags_path:
      app_name, upgrade_path = _get_upgrade_flag_info(path)
      upgrade(upgrade_path, app_name)
  except:
    logging.exception("Failed to recovery a app with upgrade fetch")


def stop():
  engine.mq.delete_topic(APP_EXECUTED_TOPIC)
  engine.mq.delete_topic(APP_EXECUTE_FAILED_TOPIC)
  engine.mq.delete_topic(APP_TERMINATED_TOPIC)
  engine.mq.delete_topic(APP_TERMINATE_FAILED_TOPIC)
  engine.mq.delete_topic(APP_INSTALLED_TOPIC)
  engine.mq.delete_topic(APP_INSTALL_FAILED_TOPIC)
  engine.mq.delete_topic(APP_UNINSTALLED_TOPIC)
  engine.mq.delete_topic(APP_UNINSTALL_FAILED_TOPIC)
  engine.mq.delete_topic(APP_UPGRADED_TOPIC)
  engine.mq.delete_topic(APP_UPGRADE_FAILED_TOPIC)
  engine.mq.delete_topic(APP_SETTINGS_CHANGED_TOPIC)


class App(Base):
  """Clique application abstraction.
  """
  def __init__(self, name, display, version=None, engine_version=None,
               author=None, description=None, entry=None,
               last_upgraded_time=None, required_apps=None,
               devices=None, view=None, priority=None, forwarding=[],
               shares=None):
    """ Inits App class with app config data"""
    self.name = name
    self.display = display
    self.version = version
    self.engine_version = engine_version
    self.author = author
    self.description = description
    self._entry = entry
    self.last_upgraded_time = last_upgraded_time
    self.required_apps = required_apps or []
    self.devices = devices or []
    self.view = view or []
    self.priority = priority
    self.forwarding = forwarding   # [[from,to], [from,to] ...]
    self.error = None
    self.state = READY_STATE
    self.service = None
    self.progress_ident = None
    self.shares = shares or []

  @classmethod
  def build(cls, config):
    ports = _parse_ports(config.get('application.forwarding', []))
    shares = _parse_shares(config.get('application.shares', []))

    return App(config.get('application.name'),
               config.get('application.display'),
               config.get('application.version'),
               config.get('application.sdk-version'),
               config.get('application.author'),
               config.get('application.description'),
               config.get('application.entry'),
               0,   # TODO: get upgraded date
               config.get('application.requires', []),
               config.get('application.devices', []),
               config.get('application.view', []),
               config.get('application.priority'),
               ports,
               shares)

  def __setattr__(self, name, value):
    if name == 'state':
      # Clear error record if state is changed
      self.error = None
    return object.__setattr__(self, name, value)

  def set_error(self, code, msg):
    self.error = (code, msg)

  @property
  def entry(self):
    return self._entry

  def update(self, config):
    self.display = config.get('application.display')
    self.version = config.get('application.version')
    self.engine_version = config.get('application.sdk-version')
    self.author = config.get('application.author')
    self.description = config.get('application.description')
    self._entry = config.get('application.entry')
    self.requires = config.get('application.requires', [])
    self.devices = config.get('application.devices', [])
    self.view = config.get('application.view', [])
    self.priority = config.get('application.priority')
    self.ports = _parse_ports(config.get('application.forwarding', []))
    self.shares = _parse_shares(config.get('application.shares', []))


def _parse_ports(forwarding_infos):
  ports = []
  for direction in forwarding_infos:
    ports.append(direction.split('->'))
  return ports


def _parse_shares(share_infos):
  shares = []
  for share_info in share_infos:
    able_apps, permits, target = share_info.split(':')
    shares.append([able_apps.split(','), permits, target])
  return shares


class Service(Base):
  """Running service application abstraction.
  """
  def __init__(self, started_time, version=None, pid=None, monitor=None):
    self.started_time = started_time
    self.pid = pid
    self.version = version
    self._monitor = monitor

  @property
  def monitor(self):
    return self._monitor


class Monitor(object):
  """Service monitor for watching until the end of the service.
  """
  def __init__(self, app_name, proc):
    self.app_name = app_name
    self.proc = proc
    self.timeout = None
    self.future = None
    Thread(target=self._run).start()

  def _run(self):
    stdout, stderr = self.proc.communicate()
    # Clean timeout
    if self.timeout:
      self.timeout.cancel()
      self.timeout = None
    # Clean resources related service
    appinfo = __DATA__.apps[self.app_name]
    appinfo.service = None
    appinfo.state = READY_STATE
    self.future and self.future.set_value(appinfo)
    engine.progress.update_progress(appinfo.progress_ident, 100,
                                    response=True)
    engine.mq.publish(APP_TERMINATED_TOPIC, self.app_name)
    clique.do(engine.isc.remove_service_route, self.app_name)
    dispose_devices(appinfo)
    _umount(self.app_name)
    logging.info("App %s terminated with output: %s\n, error: %s",
                 self.app_name, stdout, stderr)


def init_devices(appinfo):
  """Initialize any required devices"""
  for device in appinfo.devices:
    if device.startswith(GPIO_RESOURCE):
      edge = None
      if device.count(RESOURCE_SEPARATOR) == 2:
        _, pin, direction = device.split(RESOURCE_SEPARATOR)
      elif device.count(RESOURCE_SEPARATOR) == 3:
        _, pin, direction, edge = device.split(RESOURCE_SEPARATOR)
      gpio.export(int(pin), direction, edge)
    elif device.startswith(STORAGE_RESOURCE):
      engine.storage.register(appinfo.name)


def dispose_devices(appinfo):
  """Dispose required devices"""
  for device in appinfo.devices:
    if device.startswith(GPIO_RESOURCE):
      _, pin, direction = device.split(RESOURCE_SEPARATOR)
      gpio.unexport(int(pin))
    elif device.startswith(STORAGE_RESOURCE):
      engine.storage.unregister(appinfo.name)


def _set_up_with_app_info(app_info):
  try:
    _mount_share_app_dir(app_info)
  except:
    logging.exception("Failed to set up a %s", app_info.name)


def _tear_down_with_app_info(app_info):
  try:
    _unmount_share_app_dir(app_info)
  except:
    logging.exception("Failed to tear down a %s", app_info.name)


def _mount_share_app_dir(app_info):
  for share_dir_info in app_info.shares:
    able_apps, permits, target = share_dir_info
    engine.storage.mount_share_app_dir(app_info.name, target, permits,
                                       able_apps)


def _unmount_share_app_dir(app_info):
  for share_dir_info in app_info.shares:
    _, _, target = share_dir_info
    engine.storage.unmount_share_app_dir(app_info.name, target)


def _extract_tar(src_path, app_name, dst_path):
  os.path.exists(dst_path) or os.makedirs(dst_path)
  cmd = 'cd {src_path};cat {name}.tgz* | tar xvzfp - -C {dst_path}'.format(
      src_path=src_path, name=app_name, dst_path=dst_path)
  engine.aengel.execute_cmd(cmd, timeout=__LONG_TIMEOUT_INTERVAL__)


def _remove_dir(dir_path):
  engine.aengel.execute_cmd('rm -rf %s' % dir_path)


def _init_recovery_dir():
  path = os.path.join(engine.runtime.HOME_PATH, 'recovery')
  os.path.exists(path) or os.makedirs(path)
  return path


def _get_upgrade_flags():
  from glob import glob
  pattern = _build_upgrade_flag_path('*')
  return glob(pattern)


def _build_upgrade_flag_path(app_name):
  file_name = 'upgrade_{name}.flag'.format(name=app_name)
  return os.path.join(__DATA__.recovery_dir, file_name)


def _is_exists_upgrade_flag(app_name):
  return os.path.exists(os.path.join(_build_upgrade_flag_path(app_name)))


def _remove_upgrade_data(app_name):
  cmd = 'rm -rf {base}/upgrade_{name}*'.format(
      base=__DATA__.recovery_dir, name=app_name)
  engine.aengel.execute_cmd(cmd)


def _set_upgrade_flag_info(app_name, src_path):
  path = _build_upgrade_flag_path(app_name)
  flag_io = StringIO()
  flag_io.write(app_name)
  flag_io.write('\n')
  flag_io.write(src_path)
  flag_io.write('\n')
  with open(path, 'w') as wf:
    wf.write(flag_io.getvalue())
  flag_io.close()
  return path


def _backup_package_files(src_path, name, dst_path):
  cmd = 'mkdir -p {dst_path};cp -f {src_path}/{name}.tgz* {dst_path}/'.format(
      src_path=src_path, name=name, dst_path=dst_path)
  engine.aengel.execute_cmd(cmd)


def _get_upgrade_flag_info(flag_path):
  with open(flag_path) as rf:
    rf.readline()  # No need app name sector
    app_name = rf.readline().strip()
    src_path = rf.readline().strip()
  return app_name, src_path


def _parsing_app_path(path, app_name):
  return os.path.join(_get_app_dir(app_name), path[1:])


@endpoint(name='install',
          elevated=True,
          protection=PRIVILEGED,
          arg_descriptions={'data_path': 'App files local path',
                            'app': 'App informanton to install'}
          )
def install(data_path, app, owner, progress_ident=None):
  """receive apack
  decompress app env and apack
  create user(username is app_name)
  change authority onwer(user) and group(clique) for app directory
  set facl and quata
  """
  led.breathe(count=1000)
  logging.info("Try to install a App '%s'." % app.name)
  appinfo = get_app(app.name)
  ident = progress_ident or engine.progress.build_ident('install', app.name)

  # PRE CONDITION
  if appinfo:
    if appinfo.state in [INSTALLING_STATE, DOWNLOADING_STATE]:
      raise Exception(0, "App '%s' is installing now." % app.name)
    elif appinfo.state == UNINSTALLING_STATE:
      raise Exception(0, "App '%s' is uninstalling now." % app.name)
    elif appinfo.state == UPGRADING_STATE:
      raise Exception(0, "App '%s' is updating now." % app.name)
    elif appinfo.state in [RUNNING_STATE, START_STATE]:
      raise Exception(0, "App '%s' is already installed." % app.name)
    #TODO: If terminating. need to subscribe

  def do_install():
    try:
      #TODO: publish event install start
      appinfo.state = INSTALLING_STATE
      logging.info("Installing a App '%s'...", appinfo.name)
      dst_path = _parsing_app_path(data_path, owner)
      # app install start
      engine.appinstall.appinstall(dst_path, appinfo.name, __DATA__.appbase)
      # Confirm app.config and read it
      appinfo.update(engine.runtime.context().get_app_info(appinfo.name))
      appinfo.state = READY_STATE
      __DATA__.apps[appinfo.name] = appinfo
      engine.progress.update_progress(ident, 100, response=True)
      logging.info("Complete to install a app '%s' install." % appinfo.name)
    except Exception as e:
      logging.exception("Failed to install a '%s'", appinfo.name)
      return e
    finally:
      led.stop()

  def on_finish(value):
    # Rollback function and error handle
    if isinstance(value, Exception):
      logging.error("Error while install a application. :'%s'" % str(value))
      del __DATA__.apps[appinfo.name]
      engine.progress.update_progress(progress_ident, response=value)
      engine.mq.publish(APP_INSTALL_FAILED_TOPIC, appinfo.name)
    else:
      _set_up_with_app_info(appinfo)
      engine.mq.publish(APP_INSTALLED_TOPIC, appinfo.name)

  appinfo = App(app.name, app.display)
  appinfo.progress_ident = ident
  __DATA__.apps[app.name] = appinfo
  appinfo.state = INSTALLING_STATE
  engine.runtime.worker().do(do_install).then(on_finish)
  return ident


def _get_app_dir(app_name):
  return os.path.join(APPS_PATH, app_name)


def _parse_required_apps_info(required_apps):
  """Helper to parse the required applications.
  It returns the list of (app_name, version, sign-of-inequality).
  """
  appinfos = []
  for app_name in required_apps:
    index = app_name.rfind('>')
    index = index > 0 and index or app_name.rfind('<')
    separator = None
    if index > 0:
      separator = app_name[index]
      if app_name[index + 1] == '=':
        separator += '='
    else:
      index = app_name.rfind('=')
      if index > 0:
        separator = '='
    if separator:
      appinfos.append(tuple(app_name.split(separator) + [separator]))
    else:
      appinfos.append((app_name, None, None))
  return appinfos


def _check_required_app(app_info, dest_list):
  """ Return True, if required applications are sufficient
  But not, raise ServiceRequestError
  """

  appinfos = _parse_required_apps_info(app_info.required_apps)

  for name, ver, sign in appinfos:
    cmp = dest_list.get(name)
    if cmp:
      if sign == '>=' and ver >= cmp.version:
        pass
      elif sign == '<=' and ver <= cmp.version:
        pass
      elif sign == '<' and ver < cmp.version:
        pass
      elif sign == '>' and ver > cmp.version:
        pass
      elif sign == '=' and ver == cmp.version:
        pass
      else:
        raise ServiceRequestError(1003, "Required application '%s' version"
                                  " is '%f'. Required version is '%f'" %
                                  (name, cmp.version, ver))
    else:
      raise ServiceRequestError(1001, "Application '%s' is required" %
                                app_info.name)
    logging.debug("Required application '%s' is sufficient with version '%s'",
                  name, ver)


@endpoint(elevated=True,
          protection=PRIVILEGED)
def uninstall(app_name, progress_ident=None):
  """ Uninstalls a application

  post-condition:
  remove user from userlist
  remove app directory
  check dependency
  """
  led.breathe(count=3)
  ident = progress_ident or engine.progress.build_ident('uninstall', app_name)
  # PRECONDITION CHECK START
  if engine.runtime.context().engine_policy.is_immortal(app_name):
    # Ignore uninstall request
    raise Exception("Cannot uninstall a app '%s', it is refused by policy.",
                    app_name)

  appinfo = __DATA__.apps.get(app_name)

  # PRECONDITION
  if not appinfo:
    raise Exception("Fail to uninstall App '%s', it is not exists." % app_name)
  elif appinfo.state == UNINSTALLING_STATE:
    raise Exception("Fail to uninstall App '%s', Already uninstalling."
                    % app_name)
  appinfo.progress_ident = ident
  #TODO: Handles TERMINATING STATE
  #TODO: Handles INSTALLING STATE

  @Future.throw_if_error
  def do_uninstall(v=None):
    appinfo.state = UNINSTALLING_STATE
    #TODO: Handles exception if ret is exception
    if os.path.exists(os.path.join(APPS_PATH, app_name)):
      _tear_down_with_app_info(__DATA__.apps[app_name])
      _remove_dependency(app_name)
      _remove_app_directory(app_name)
      del __DATA__.apps[app_name]
      engine.progress.update_progress(ident, 100, response=True)
      engine.mq.publish(APP_UNINSTALLED_TOPIC, app_name)
    else:
      raise Exception("Fail to uninstall, %s is not installed." % app_name)

  def on_finish(value):
    if isinstance(value, Exception):
      logging.error("Occurs error while uninstall '%s': %s", app_name,
                    str(value))
      appinfo.state = READY_STATE
      engine.progress.update_progress(ident, response=value)
      engine.mq.publish(APP_UNINSTALL_FAILED_TOPIC, app_name)
    return value

  engine.progress.update_progress(ident, 50)
  if appinfo.state in [RUNNING_STATE, START_STATE]:
    terminate(app_name).then(do_uninstall).then(on_finish)
  else:
    engine.runtime.worker().do(do_uninstall).then(on_finish)
  return ident


def _remove_app_directory(app_name):
  """remove app directory
  """
  app_path = os.path.join(APPS_PATH, app_name)
  engine.aengel.execute_cmd('rm -rf %s' % app_path)


def _remove_dependency(app_name):
  """check dependency and confirm remove to user
  """
  _umount(app_name)


@endpoint(description="Start the service application.",
          arg_descriptions={'app_name': "Service application name."},
          elevated=True,
          protection=PRIVILEGED
          )
def execute(app_name, progress_ident=None):
  """Executes the given application as a service.
  Note that only one service instance can be running at any given time.
  It returns the associated `Service` object if successful.

  Flow:
  1. add route named app_name
  1. execute app with sandbox
  1.1 if Failed, remove route name app_name
  """
  led.breathe(speed=8, count=3)
  logging.debug("Try to execute a Application '%s'.", app_name)
  ident = progress_ident or engine.progress.build_ident('execute', app_name)
  appinfo = __DATA__.apps.get(app_name)

  # PRECONDITION
  if not appinfo:
    raise Exception("Fail to execute App '%s', it is not exists." % app_name)
  elif appinfo.state in [START_STATE, RUNNING_STATE]:
    raise Exception("Fail to execeute App '%s', already running." % app_name)
  elif appinfo.state != READY_STATE:
    # Checks what application is already executed.
    raise Exception("Fail to execute, App '%s' is working." % app_name)
    #TODO: Handles UNINTSALL STATE
    #TODO: Handles INTSALL STATE
    #TODO: Handles TERMINATE STATE
  appinfo.progress_ident = ident

  # Starts app
  engine.isc.add_service_route(app_name)
  appinfo.state = START_STATE
  service = Service(time.time(), version=appinfo.version)

  # TODO: Handle error if ret is exception
  # Checks engine version
  # TODO: Need to check version
  """
  # Checks prerequsite
  _check_required_app(app, get_services())
  """
  # Write the current engine settings to the app directory for its use.
  engine.aengel.execute_cmd('rm -rf %s/run/* %s/tmp/*' % (
      os.path.join(APPS_PATH, appinfo.name),
      os.path.join(APPS_PATH, appinfo.name)))
  context_filepath = os.path.join(APPS_PATH, appinfo.name,
                                  CONTEXT_FILEPATH[1:])
  logging.debug("Context file path : %s", context_filepath)
  engine.runtime.context().engine_info.flush(context_filepath)
  _create_log(appinfo.name)
  _mount(appinfo)

  engine.progress.update_progress(ident, 50)
  # Execute a application
  execute_args = []
  stderr = None
  try:
    execute_args = [__DATA__.sandbox_path, appinfo.name, appinfo.entry]
    logging.debug("Execute command: '%s'.", execute_args)
    init_devices(appinfo)
    p = psutil.Popen(execute_args, env=_create_app_env_vars(appinfo),
                     stdout=PIPE, stderr=PIPE)
    if p.is_running():
      monitor = Monitor(appinfo.name, p)
      # Creates and register a service
      service._monitor = monitor
      appinfo.state = RUNNING_STATE
      service.pid = p.pid
      appinfo.service = service

      if appinfo.forwarding:
        ports_map = engine.network.list_mapped_ports()
        for d_p, to_p in appinfo.forwarding:
          if d_p in ports_map:
            engine.network.unmap_network_port(ports_map[d_p][0], d_p)
          engine.network.map_network_port(to_p, d_p)

      if appinfo.priority:
        engine.aengel.execute_cmd('renice %s %s' % (str(appinfo.priority),
                                                    str(p.pid)))

      # TODO : Need to check about children
      logging.info("Successfully started a Application '%s'", appinfo.name)
      logging.debug("Children num %d", len(p.get_children()))
      engine.progress.update_progress(ident, 100, response=True)
      engine.mq.publish(APP_EXECUTED_TOPIC, appinfo.name)
    else:
      stdout, stderr = p.communicate()
      raise Exception("Fail to start a Application '%s'. status='%s',"
                      "\noutput: %s\nerror: %s" %
                      (appinfo.name, p.status, stdout, stderr))
  except:
    logging.exception("Error while running the application.\nCommand : %s",
                      execute_args)
    _umount(app_name)
    engine.isc.remove_service_route(app_name)
    engine.progress.update_progress(ident, response=False)
    engine.mq.publish(APP_EXECUTE_FAILED_TOPIC, app_name)
    appinfo.state = READY_STATE
  return ident


@endpoint(description='Terminate the running service application.',
          arg_descriptions={
          'app_name': 'Service application name.',
          },
          name='terminate',
          elevated=True,
          protection=PRIVILEGED
          )
def ep_terminate(app_name, progress_ident=None):
  ident = progress_ident or engine.progress.build_ident('terminate', app_name)
  engine.runtime.worker().do(terminate, app_name, ident)
  return ident


def terminate(app_name, progress_ident=None):
  """Terminates the running service application.
  """
  led.breathe(count=3)
  logging.info("Terminating a '%s' app.", app_name)
  ident = progress_ident or engine.progress.build_ident('terminate', app_name)
  if engine.runtime.context().engine_policy.is_immortal(app_name):
    # Ignore terminate request
    logging.warn("Cannot terminate a app '%s', it is refused by policy.",
                 app_name)
    engine.progress.update_progress(ident, response=False)
    engine.mq.publish(APP_TERMINATE_FAILED_TOPIC, app_name)
    return Future(value=False)

  try:
    app = __DATA__.apps.get(app_name)
    app.progress_ident = ident
    if not app:
      engine.mq.publish(APP_TERMINATE_FAILED_TOPIC, app_name)
      raise Exception("App '%s' not exsits." % app_name)
    if not app.service:
      engine.mq.publish(APP_TERMINATE_FAILED_TOPIC, app_name)
      raise Exception("App '%s' is not running." % app_name)
    elif not psutil.pid_exists(app.service.pid):
      pid = app.service.pid
      logging.info("Clean '%s' Service data, pid '%d' is already terminated.",
                   app_name, pid)
      app.service = None
      if app.state in [START_STATE, RUNNING_STATE, TERMINATING_STATE]:
        app.state = READY_STATE
      engine.progress.update_progress(ident, 100, response=True)
      engine.mq.publish(APP_TERMINATED_TOPIC, app_name)
      return Future(value=ident)

    app.state = TERMINATING_STATE
    clique.do(engine.isc.remove_service_route, app_name)
    logging.debug("Register task for terminating a pid '%d'.", app.service.pid)

    def do_terminate():
      pid = app.service.pid
      p = psutil.Process(pid)

      cpid_list = p.get_children(recursive=True)
      terminate_pid_cmd = StringIO()
      while cpid_list:
        terminate_pid_cmd.write("%s " % cpid_list.pop().pid)

      terminate_pid_list = terminate_pid_cmd.getvalue()
      terminate_pid_cmd.close()
      service = get_service(app_name)

      def on_timeout():
        logging.warning("Fail to terminate an application %s, pid '%s'.",
                        app_name, terminate_pid_list)
        logging.warning("Try to kill an application %s, pid '%s'.",
                        app_name, terminate_pid_list)
        engine.aengel.execute_cmd('kill -9 %s' % terminate_pid_list)

      service.monitor.future = Future()
      logging.info("Try to terminate a pid '%s'.", terminate_pid_list)
      engine.progress.update_progress(ident, 60)
      engine.aengel.execute_cmd('kill %s' % terminate_pid_list)
      service.monitor.timeout = Timer(clique.ioloop(), __TIMEOUT_INTERVAL__,
                                      on_timeout)

      for d_p, to_p in app.forwarding:
        engine.network.unmap_network_port(to_p, d_p)

      return service.monitor.future

    def on_finish(value):
      if isinstance(value, Exception):
        logging.error("Error is occured while terminating service.:%s",
                      str(value))
        engine.progress.update_progress(ident, response=False)
        engine.mq.publish(APP_TERMINATE_FAILED_TOPIC, app_name)
      return value

    engine.progress.update_progress(ident, 50)
    return do_terminate().then(on_finish)
  except Exception as e:
    logging.exception('Fail to terminate: ')
    engine.update_progress(ident, response=e)
    raise


@endpoint(description='Upgrade the app',
          name='upgrade',
          elevated=True,
          protection=PRIVILEGED,
          )
def upgrade(data_path, app_name, owner=None, progress_ident=None):
  """Update app.
  """
  led.breathe(count=1000)
  logging.info("Upgrading a App '%s'..." % app_name)
  appinfo = get_app(app_name)
  ident = progress_ident or engine.progress.build_ident('upgrade', app_name)

  #PRECONDITION
  if appinfo:
    if appinfo.state in [INSTALLING_STATE, DOWNLOADING_STATE]:
      raise Exception(0, "App '%s' is installing now." % app_name)
    elif appinfo.state == UNINSTALLING_STATE:
      raise Exception(0, "App '%s' is uninstalling now." % app_name)
    elif appinfo.state == UPGRADING_STATE:
      raise Exception(0, "App '%s' is updating now." % app_name)
    #TODO: If terminating. need to subscribe
  else:
    raise Exception(0, "App '%s' is not installed yet." % app_name)
  appinfo.progress_ident = ident
  data = Lazy()

  @Future.throw_if_error
  def do_prepare(v=None):
    engine.progress.update_progress(ident, 70)
    appinfo.state = UPGRADING_STATE
    logging.info("Preparing to upgrade '%s' app data...", appinfo.name)
    if owner:
      dst_path = os.path.join(__DATA__.recovery_dir, 'upgrade_{name}'.format(
          name=app_name))
      src_path = _parsing_app_path(data_path, owner)
      _backup_package_files(src_path, app_name, dst_path)
      _set_upgrade_flag_info(app_name, dst_path)
      data.path = dst_path
    else:
      data.path = data_path

  @Future.throw_if_error
  def do_upgrade(v=None):
    engine.progress.update_progress(ident, 75)
    logging.info("Upgrading '%s' app data...", appinfo.name)
    app_dir = os.path.join(APPS_PATH, appinfo.name)

    logging.debug("Uncompress updated file: %s", data.path)
    _extract_tar(data.path, app_name, app_dir)
    logging.debug("Completed to uncompress file")
    engine.progress.update_progress(ident, 85)

    engine.progress.update_progress(ident, 90)
    engine.appinstall.set_app_core_permision(os.path.join(app_dir, 'app'),
                                             os.path.join(app_dir, 'data'))
    logging.info("Updates config of '%s'.", appinfo.name)
    engine.progress.update_progress(ident, 98)
    __DATA__.apps[appinfo.name] = App.build(
        engine.runtime.context().get_app_info(appinfo.name))

    logging.info("Completed to Upgrading '%s' app data...", appinfo.name)
    engine.progress.update_progress(ident, 99, response=v)

  def do_finish(v):
    led.stop()
    # Rollback function and error handle
    if isinstance(v, Exception):
      logging.error("Error while upgrade a application. :'%s'" % str(v))
      engine.progress.update_progress(ident, response=v)
      engine.mq.publish(APP_UPGRADE_FAILED_TOPIC, appinfo.name)
    else:
      _remove_upgrade_data(appinfo.name)
      engine.progress.update_progress(ident, 100, response=True)
      engine.mq.publish(APP_UPGRADED_TOPIC, appinfo.name)
      logging.info("Completed to Upgrade a '%s' app.", appinfo.name)
    __DATA__.apps[appinfo.name].state = READY_STATE

  is_imm = engine.runtime.context().engine_policy.is_immortal(appinfo.name)

  engine.progress.update_progress(ident, 70)
  if appinfo.state in [RUNNING_STATE, START_STATE] and not is_imm:
    terminate(appinfo.name).then(do_prepare).then(do_upgrade).then(do_finish)
  else:
    engine.runtime.worker().do(do_prepare).then(do_upgrade).then(do_finish)
  return ident


def _mount(appinfo):
  # mount for required mount path
  for dirname in __REQUIRED_MOUNT_PATHS__:
    try:
      src_path = os.path.join(ROOT_PATH, dirname)
      dst_path = os.path.join(APPS_PATH, appinfo.name, dirname)
      if not os.path.exists(dst_path):
        engine.aengel.execute_cmd('mkdir -p %s' % dst_path)
      if not os.path.ismount(dst_path):
        engine.aengel.execute_cmd('mount --rbind %s %s' % (src_path,
                                                           dst_path))
    except:
      logging.warn("%s failed to %s mount", appinfo.name, dirname,
                   exc_info=True)

  # mount for optional mount path
  for device in appinfo.devices:
    #device has ':' for example gpio:23:out
    if device.find(':') > -1:
      #the device name should be placed first
      device = device.split(':')[0]
    dirlist = __OPTIONAL_MOUNT_PATHS__.get(device)
    if dirlist:
      for dirname in dirlist:
        try:
          src_path = os.path.join(ROOT_PATH, dirname)
          dst_path = os.path.join(APPS_PATH, appinfo.name, dirname)
          if os.path.exists(src_path):
            if not os.path.exists(dst_path):
              engine.aengel.execute_cmd('mkdir -p %s' % dst_path)
            if not os.path.ismount(dst_path):
              engine.aengel.execute_cmd('mount --rbind %s %s' % (src_path,
                                                                 dst_path))
          else:
            logging.warn("Failed %s mounted. %s is not exist.", src_path,
                         src_path)
        except:
          logging.warn("%s failed to %s mount", appinfo.name, dirname,
                       exc_info=True)
    else:
      logging.warn("Failed %s mounted. %s is not includes path.", device,
                   device, exc_info=True)


def _umount(app_name):
  # umount for required mount path
  logging.debug("Umount %s.", app_name)
  for dirname in __REQUIRED_MOUNT_PATHS__:
    dst_path = os.path.join(APPS_PATH, app_name, dirname)
    try:
      engine.aengel.execute_cmd('umount -l %s' % dst_path)
    except:
      logging.warn("%s failed to %s umount.", app_name, dirname, exc_info=True)

  # umount for optional mount paths
  for dirlist in __OPTIONAL_MOUNT_PATHS__.values():
    for dirname in dirlist:
      dst_path = os.path.join(APPS_PATH, app_name, dirname)
      try:
        engine.aengel.execute_cmd('umount -l %s' % dst_path)
      except:
        logging.warn("%s failed to %s umount.", app_name, dirname,
                     exc_info=True)


def _create_log(app_name):
  log_path = os.path.join(APPS_PATH, app_name, LOG_PATH[1:])
  if not os.path.exists(log_path):
    engine.aengel.execute_cmd('mkdir -p %s' % log_path)
    engine.aengel.execute_cmd('chown %s:%s %s' % (CLIQUE_USER, CLIQUE_USER,
                                                  log_path))
    logging.info("Created the log directory for the %s.", app_name)


@endpoint(description='Gets the specific installed application.',
          arg_descriptions={
          'app_name': 'Application name.',
          },
          )
def get_app(app_name):
  """Gets the :class:`App` data of the specified app.
  """
  return __DATA__.apps.get(app_name)


@endpoint(description='List all installed applications.',
          )
def get_apps():
  """Gets all :class:`App`s
  """
  return __DATA__.apps


@endpoint(description='List all running applications.',
          )
def get_services():
  """Gets the :class:'Service' data of all running apps
  """
  services = {}
  for name, app in __DATA__.apps.items():
    if app.service:
      services[name] = app.service
  return services


@endpoint(description='Get a running service application.',
          )
def get_service(app_name):
  """Gets a :class:`Service` for the given `app_name`.
  """
  return __DATA__.apps[app_name].service if app_name in __DATA__.apps else None


@endpoint(description='Gets app icon.',
          )
def get_icon(app_name):
  """ Gets app icon

  Args:
    app_name: specified app name to get a icon

  Returns:
    Binary object
  """
  if app_name not in __DATA__.apps:
    logging.warning("Application '%s' not exsits.", app_name)
    return Future(None)

  appbase = engine.runtime.context().get_app_base_dir(app_name)
  # Icon file is located in data directory
  iconpath = os.path.join(appbase, __APP_ICON_SUBPATH__)

  if not os.path.exists(iconpath):
    logging.warn("The icon file not exists, App: %s", app_name)
    return Future(None)

  def finish_cb(copier):
    if isinstance(copier.error, Exception):
      logging.warn("Failed to send a icon : %s", copier.error)

  def ready_callback(binary):
    logging.debug("Start to transfer a icon, %s", iconpath)
    FileToBinaryCopier(open(iconpath), binary, binary.size, finish_cb).start()

  return Future(Binary(os.path.getsize(iconpath), ready_callback))


@endpoint(description='Sets start apps.',
          protection=PRIVILEGED,
          )
def set_starts_app(app_names):
  engine.runtime.context().engine_settings.set('common.startups', app_names)
  engine.runtime.context().engine_settings.flush()


@endpoint(description='Gets start apps.',
          protection=PRIVILEGED,
          )
def get_starts_app():
  return engine.runtime.context().engine_settings.get('common.startups')


def _create_app_env_vars(appinfo):
  """Helper for creating app environment variables.
  """
  vars = {'PYTHONPATH': FUNDAMENTALS_PATH,
          'PATH': '/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin'
                  ':/bin',
          'HOME': '/home/' + appinfo.name,
          'USER': appinfo.name,
          }
  lang = os.environ.get('LANG')
  if lang:
    vars['LANG'] = lang
  app_settings = engine.runtime.context().get_app_info(appinfo.name)
  env_vars = app_settings.get('environments', [])
  for key, value in env_vars:
    vars[key] = value + vars[key] if key.startswith(':') else value
  if SCREEN_RESOURCE in appinfo.devices:
    # TODO: if multiple applications acquire the screen, provide a means to
    # switch between them (an Apple's expose-like UI would be nice...)
    vars['DISPLAY'] = engine.node.name() + ':0'
  return vars


def _collect_installed_apps():
  """Gets the `App` data of all installed apps.
  1. Get directory list of a app base
  2. Matching directory list with user
  3. Check /[appbase]/[user]/app/.info file
  4. Create specified app data
  """
  apps = {}
  for app_name in os.listdir(APPS_PATH):
    try:
      appdir = os.path.join(APPS_PATH, app_name)
      if not os.path.isdir(appdir):
        logging.info("Skipping non-directory...")
        continue
      apps[app_name] = App.build(engine.runtime.context().get_app_info(app_name))
    except:
      logging.exception("Error while collecting apps.")
  logging.debug("Collected app list : number of %d, %s", len(apps), str(apps))
  return apps


@endpoint(description='Gets start apps.',
          protection=PRIVILEGED,
          )
def copy_appbase(data_path, owner):
  dst_path = _parsing_app_path(data_path, owner)
  logging.info("Copy appbase files to %s", dst_path)
  cmd = 'rsync -ar --progress %s/* %s/' % (__DATA__.appbase, dst_path)
  engine.aengel.execute_cmd(cmd)
  logging.info("Completed to copy")
  return True


@endpoint(description='Get app setting values',
          name='get_app_settings',
          elevated=True,
          protection=PRIVILEGED,
          )
def get_app_settings(app_name):
  """ Gets app setting values

  Args:
    app_name: Application name to get settings values.
    if it is null, returns settings of all apps

  Returns:
    Dictionary indicating setting value and type

    ex:
      {app_name:{ settings... }}
  """
  if app_name:
    return {app_name:
            engine.runtime.context().get_app_settings(app_name).get_dict()}
  else:
    return dict(
        [(name,
          engine.runtime.context().get_app_settings(
              name).to_dict()) for name in __DATA__.apps.keys()])


@endpoint(description='Set app setting values',
          name='set_app_settings',
          elevated=True,
          protection=PRIVILEGED,
          )
def set_app_settings(values):
  """ Sets app setting values

  Args:
    values: Dictionary with key and value

    Ex:
      {app_name: {key: value, key2: value2}}

  """
  for app_name, items in values.items():
    app_settings = engine.runtime.context().get_app_settings(app_name)
    for key, value in items.items():
      app_settings.set(key, value)
    app_settings.flush()
  engine.mq.publish(APP_SETTINGS_CHANGED_TOPIC, values)


@endpoint(description='Reset app setting values',
          name='reset_app_settings',
          elevated=True,
          protection=PRIVILEGED,
          )
def reset_app_settings(app_name):
  """ Reset app setting values
  """
  app_config = os.path.join(engine.runtime.context().get_app_base_dir(app_name),
                            APP_CONFIG_FILE_NAME)
  engine.aengel.execute_cmd('rm -rf %s', app_config)
  engine.mq.publish(APP_SETTINGS_CHANGED_TOPIC, {app_name: None})


@endpoint(description='Reset app setting values',
          name='refresh_info',
          elevated=True,
          protection=PRIVILEGED,
          )
def __refresh_info__(app_name):
  return refresh_info(app_name)


def refresh_info(app_name):
  old_app = __DATA__.apps[app_name]
  if old_app.state != READY_STATE:
    raise Exception(0, "App '%s' is working now." % app_name)
  _tear_down_with_app_info(old_app)
  new_app = App.build(engine.runtime.context().get_app_info(app_name))
  __DATA__.apps[app_name] = new_app
  _set_up_with_app_info(new_app)
  return True


class ServiceBackupError(Exception):
  pass


class ServiceExecutionError(Exception):
  pass


class ServiceInstallationError(Exception):
  pass


class ServiceRequestError(Exception):
  pass


class ServiceTermiantionError(Exception):
  pass
