#!/bin/env/python
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


""" App Module tests
"""


import os
import tempfile
import uuid
import shutil
import engine.appinstall
import pwd
import psutil
import time
import tarfile
from mock import Mock
from nose.plugins.attrib import attr
from unittest import skip

import engine.app
import engine.mq
import engine.runtime
from adt.config import JsonSettings
from adt.concurrency import Future
from adt.testing import Restorer
from rpclib.testing import RpcTestCase


__TEST_DIR__ = os.path.dirname(os.path.abspath(__file__))
__APP_DATA_DIR__ = '''core/python_app'''


class mock_settings():
  def flush(self, context_filepath):
    pass


class mock_policy():
  def is_immortal(self, appname):
    return False


class mock_context():
  engine_settings = mock_settings()
  engine_info = mock_settings()
  engine_policy = mock_policy()

  def get_app_settings(self, appname):
    return JsonSettings([])

  def get_app_info(self, appname):
    return JsonSettings([])

  def rebase(self, path):
    pass


class MockTaskQueue(object):
  def __init__(self, ioloop):
    self.ioloop = ioloop

  def do(self, callback, *args, **kwargs):
    future = Future()

    def do_callback():
      try:
        value = callback(*args, **kwargs)
        if isinstance(value, Future):
          value.then(future.set_value)
        else:
          future.set_value(value)
      except Exception, e:
        future.set_value(e)
    self.ioloop.add_callback(do_callback)
    return future


class AppGenerator(object):
  def __init__(self, root=None):
    self.root = root or engine.runtime.APPS_PATH
    self.apps = set()
    self.app_info = os.environ.get('SAMPLE_APP_INFO_PATH', '/')

  def _init_app(self, name):
    basepath = os.path.join(self.root, name)

    os.path.exists(basepath) and shutil.rmtree(basepath)

    appbase = os.path.join(basepath, 'app')
    database = os.path.join(basepath, 'data')
    conf_file = os.path.join(appbase, '.info')
    os.makedirs(appbase)
    os.makedirs(database)
    shutil.copy(self.app_info, conf_file)
    settings = JsonSettings([conf_file])
    settings.set('application.name', name)
    settings.set('application.display', name)
    settings.flush()

  def set(self, name):
    self.apps.add(name)
    self._init_app(name)

  def clean(self):
    for appname in self.apps:
      basepath = os.path.join(self.root, appname)
      shutil.rmtree(basepath)


def _get_app_data(app_dir):
  class AppData():
    def __init__(self, name, display):
      self.name = name
      self.display = display
  app_info_path = os.path.join(__APP_DATA_DIR__,
                               os.path.join('.info'))
  app_info = JsonSettings([app_info_path])
  name = app_info.get('application.name')
  display = app_info.get('application.display')
  return AppData(name, display)


@attr(species="clique", genus="core", family="app", name="uninst")
class AppUninstallTest(RpcTestCase):
  """ Tests for app.execute
  """
  def setUp(self):
    RpcTestCase.setUp(self)
    self.res = Restorer()
    self.appname = 'sample'
    self.gen = AppGenerator()
    self.gen.set(self.appname)
    engine.runtime.__DATA__.task_queue = MockTaskQueue(self.ioloop())

  def tearDown(self):
    RpcTestCase.tearDown(self)
    self.res.restore()
    self.gen.clean()
    del engine.runtime.__DATA__.context
    del engine.runtime.__DATA__.task_queue
    del engine.app.__DATA__.apps

  def collect(self):
    return {}

  def test_success_one_uninstall(self):
    """ Tests engine.app.uninstall success

    precondition:
      uninstall app once
      app exists
      app is not running

    postcondition:
      remove app
      return app info
    """
    def assert_cb(topic, appname):
      self.assertEqual(1, engine.app._remove_app_directory.call_count)
      self.assertEqual(1, engine.app._remove_dependency.call_count)
      self.assertIsNone(engine.app.__DATA__.apps.get(appname))
      self.stop()

    self.res.mock(engine.mq, 'publish', assert_cb)
    self.res.mock(engine.progress, 'update_progress', Mock())
    self.res.mock(engine.app, '_remove_app_directory', Mock())
    self.res.mock(engine.app, '_remove_dependency', Mock())

    appname = self.appname

    engine.app.uninstall(appname)
    self.start()

  def _test_fail_uninstall_twice(self):
    pass

  def _test_fail_app_running(self):
    """ Tests uninstall fail : specified app is running
    """
    pass

  def _test_fail_app_not_exsits(self):
    pass


@attr(species="clique", genus="core", family="app", name="inst")
class AppInstallTests(RpcTestCase):
  """ Tests for app.execute
  """
  def setUp(self):
    RpcTestCase.setUp(self)
    self.base_path = os.path.join(os.path.dirname(__file__),
                                  str(uuid.uuid4())[:8])
    self.res = Restorer()
    os.makedirs(self.base_path)
    self.tmp_file = os.path.join(self.base_path, str(uuid.uuid4())[:8])
    self.res = Restorer()

    self.o_mkdtemp = tempfile.mkdtemp
    tempfile.mkdtemp = Mock()
    tempfile.mkdtemp.return_value = self.base_path
    self.o_mktemp = tempfile.mktemp
    tempfile.mktemp = Mock()
    tempfile.mktemp.return_value = self.tmp_file
    engine.runtime.__DATA__.task_queue = MockTaskQueue(self.ioloop())
    engine.runtime.__DATA__.context = None
    engine.app.__DATA__.appbase = None
    self.o_appinstall = engine.appinstall.appinstall
    self.o_collect = engine.app._collect_installed_apps
    self.o_exists = os.path.exists
    self.o_update = engine.app.App.update
    self.res.mock(engine.app, '_parsing_app_path', self._parsing_app_path)
    self.res.mock(engine.app, '_extract_tar', self._extract_tar)
    self.res.mock(engine.app, '_remove_dir', self._remove_dir)

    self.value = None
    engine.app.start()

  def tearDown(self):
    RpcTestCase.tearDown(self)
    self.res.restore()
    if os.path.exists(self.base_path):
      shutil.rmtree(self.base_path)

    tempfile.mkdtemp = self.o_mkdtemp
    tempfile.mktemp = self.o_mktemp
    engine.appinstall.appinstall = self.o_appinstall
    engine.app._collect_installed_apps = self.o_collect
    os.path.exists = self.o_exists
    engine.app.App.update = self.o_update
    self.res.restore()
    del engine.app.__DATA__.apps
    del engine.app.__DATA__.appbase
    del engine.runtime.__DATA__.task_queue
    del engine.runtime.__DATA__.context
    engine.app.stop()

  def collect(self):
    return {}

  def _parsing_app_path(self, data_path, owner):
    return __APP_DATA_DIR__

  def _extract_tar(file_path, dst_path):
    with tarfile.open(file_path, 'r:bz2') as apppack:
      apppack.extractall(path=dst_path)

  def _remove_dir(dir_path):
    shutil.rmtree(dir_path)

  def test_success_install_call(self):
    """ Tests engine.app.install success

    precondition:
      specified app is not installed
      call install once

    postcondition:
      loading app info
      return app info
    """
    engine.app._collect_installed_apps = self.collect
    engine.appinstall.appinstall = Mock()

    app = _get_app_data(__APP_DATA_DIR__)
    app.name = 'test'

    os.path.exists = Mock()
    os.path.exists.return_value = True
    engine.app.App.update = Mock()

    def assert_cb(topic, appname):
      self.assertEqual(engine.app.__DATA__.apps[app.name].state,
                       engine.app.READY_STATE)
      self.assertEqual(1, engine.app.App.update.call_count)
      self.assertEqual(1, engine.appinstall.appinstall.call_count)
      self.stop()

    self.res.mock(engine.mq, 'publish', assert_cb)
    progress_ident = engine.app.install(__APP_DATA_DIR__, '', app)

    self.assertEqual(progress_ident, 'install_test')
    app_info = engine.app.get_app(app.name)
    self.assertEqual(app_info.state, engine.app.INSTALLING_STATE)
    self.start()

  def test_fail_install_twice(self):
    pass

  def test_fail_app_uninstalling(self):
    pass

  def test_fail_not_enough_storage(self):
    pass

  def test_fail_app_running(self):
    pass

  def test_fail_installed_app_running(self):
    pass

  def test_fail_installed_app_is_high_version(self):
    pass

  def test_fail_installed_app_is_same_version(self):
    pass

  def test_success_installed_app_is_low_version(self):
    pass

  def test_fail_unexpected_error_while_installing(self):
    pass


@attr(species="clique", genus="core", family="app", name="exe")
class AppExecuteTest(RpcTestCase):
  """ Tests for app.execute
  """
  def setUp(self):
    RpcTestCase.setUp(self)

    self.base_path = os.path.dirname(__file__)
    engine.runtime.__DATA__.task_queue = MockTaskQueue(self.ioloop())
    self.o_add_service_route = engine.isc.add_service_route
    self.o_publish = engine.mq.publish
    self.o_build = engine.app.App.build
    self.o_check_required_app = engine.app._check_required_app
    self.o_getpwnam = pwd.getpwnam
    self.o_context = engine.runtime.context
    self.o_Popen = psutil.Popen
    self.o_Monitor = engine.app.Monitor
    self.o_collect = engine.app._collect_installed_apps
    self.o_mount = engine.app._mount
    self.o_create_log = engine.app._create_log
    self.o_cmd = engine.aengel.execute_cmd
    engine.aengel.execute_cmd = lambda a: None
    engine.app.start()

  def tearDown(self):
    RpcTestCase.tearDown(self)

    engine.isc.add_service_route = self.o_add_service_route
    engine.mq.publish = self.o_publish
    engine.app.App.build = self.o_build
    engine.app._check_required_app = self.o_check_required_app
    pwd.getpwnam = self.o_getpwnam
    engine.runtime.context = self.o_context
    psutil.Popen = self.o_Popen
    engine.app.Monitor = self.o_Monitor
    engine.app._collect_installed_apps = self.o_collect
    engine.app._mount = self.o_mount
    engine.app._create_log = self.o_create_log
    del engine.app.__DATA__.apps
    del engine.runtime.__DATA__.context
    del engine.runtime.__DATA__.task_queue
    engine.app.stop()

  def collect(self):
    return {}

  def test_success_execute_once(self):
    """ Tests engine.app.do_execute success

    precondition:
      execute app once

    postcondition:
      register service
      register route
      return app info
    """
    engine.app._collect_installed_apps = self.collect
    engine.runtime.context = Mock()
    engine.runtime.context.return_value = mock_context()
    engine.mq.publish = Mock()
    engine.app._mount = Mock()
    engine.app._create_log = Mock()
    engine.isc.add_service_route = Mock()

    class pw():
      pw_dir = self.base_path

    pwd.getpwnam = Mock()
    pwd.getpwnam.return_value = pw()

    class mock_Monitor():
      def __init__(self, appname, process):
        pass

    engine.app.Monitor = mock_Monitor

    class mock_Process():
      pid = 99

      def is_running(self):
        return True

      def get_children(self):
        return [99, ]

    psutil.Popen = Mock()
    psutil.Popen.return_value = mock_Process()

    appname = "SAMPLE"
    app_info = engine.app.App(appname, appname)
    app_info._entry = "entry.py"
    app_info.engine_version = 0.1
    app_info.version = 0.1
    app_info.state = engine.app.READY_STATE
    engine.app.__DATA__.apps[appname] = app_info

    progress_ident = engine.app.execute(appname)

    self.assertEqual(progress_ident, 'execute_SAMPLE')
    self.assertEqual(engine.app.RUNNING_STATE,
                     engine.app.get_app(appname).state)
    self.assertEqual(1, engine.isc.add_service_route.call_count)
    self.assertEqual(3, engine.mq.publish.call_count)

  def test_fail_execute_app_twice(self):
    """ Tests execute fail : call execute twice

    precondition:
      service not exists

    postcondition:
      first call:
        register service
        register route
        return service
      second call:
        return exception
    """
    pass

  def test_fail_service_exists(self):
    """ Tests execute fail : service already exists

    precondition:
      service exists

    postcondition:
      service not exists
      route is not registered
      return exception
    """
    pass

  def test_fail_entry_file_not_exists(self):
    """ Tests execute fail : specified entry file not exists

    precondition:
      service not exists
      entry file not exists

    postcondition:
      service not exists
      route is not registered
      return exception
    """
    pass

  def test_fail_engine_version_not_sufficient(self):
    """ Tests execute fail : required engine version not sufficient

    precondition:
      service not exists
      engine version lower than required

    postcondition:
      service not exists
      route is not registered
      return exception
    """
    pass

  def test_fail_while_app_running(self):
    """ Tests execute fail : occurs error while running

    precondition:
      service not exists

    postcondition:
      service not exists
      route is not registered
      return exception
    """
    pass

  def test_fail_process_is_not_running(self):
    """ Tests execute fail : process is not running

    precondition:
      service not exists

    postcondition:
      service not exists
      route is not registered
      return exception
    """
    pass

  def test_fail_not_enough_memory(self):
    """ Tests execute fail : occurs error while running

    precondition:
      service not exists

    postcondition:
      service not exists
      route is not registered
      return exception
    """
    pass

  def test_fail_required_app_not_running(self):
    """ Tests execute fail : required apps are not running

    precondition:
      service not exists

    postcondition:
      service not exists
      route is not registered
      return exception
    """
    pass

  def test_fail_sandbox_not_exsits(self):
    """ Tests execute fail : sandbox not exists

    precondition:
      service not exists

    postcondition:
      service not exists
      route is not registered
      return exception
    """
    pass


@attr(species="clique", genus="core", family="app", name="ter")
class AppTerminateTest(RpcTestCase):
  """ Tests for app.execute
  """
  def setUp(self):
    RpcTestCase.setUp(self)
    engine.runtime.__DATA__.task_queue = MockTaskQueue(self.ioloop())
    self.res = Restorer()

  def tearDown(self):
    RpcTestCase.tearDown(self)
    self.res.restore()
    del engine.app.__DATA__.apps
    del engine.runtime.__DATA__.policy
    del engine.runtime.__DATA__.context
    del engine.runtime.__DATA__.task_queue

  def collect(self):
    return {}

  def test_success_terminate_once(self):
    """ Tests engine.app.terminate success

    precondition:
      service running

    postcondition:
      remove from service
      remove from route
      return True
    """

    class mock_Process():
      pid = 99

      def __init__(self, pid):
        pass

      def is_running(self):
        return True

      def get_children(self, recursive=True):
        return [self, ]

    class mock_monitor():
      future = None
      timeout = None

    self.res.mock(psutil, 'pid_exists', lambda x: True)
    self.res.mock(psutil, 'Process', mock_Process)
    self.res.mock(engine.isc, 'remove_service_route', Mock())
    self.res.mock(engine.app, '_collect_installed_apps', self.collect)
    self.res.mock(engine.mq, 'publish', Mock())
    self.res.mock(engine.app, '_umount', Mock())
    self.res.mock(engine.aengel, 'execute_cmd', Mock())
    engine.runtime.__DATA__.policy = mock_policy()

    appname = "SAMPLE"
    service = engine.app.Service(time.time())
    service.pid = 99
    service._monitor = mock_monitor()

    engine.app.__DATA__.apps[appname] = engine.app.App(appname, appname)
    engine.app.__DATA__.apps[appname].state = engine.app.RUNNING_STATE
    engine.app.__DATA__.apps[appname].service = service

    f = engine.app.terminate(appname)

    def assert_cb(v):
      self.assertEqual(engine.app.TERMINATING_STATE,
                       engine.app.__DATA__.apps[appname].state)
      self.assertEqual(1, engine.isc.remove_service_route.call_count)

    service.monitor.future.set_value(None)
    f.then(assert_cb)

  def test_success_process_already_terminated(self):
    """ Tests terminate success : process is already terminated

    precondition:
      service exists
      process of specified pid is not exists

    postcondition:
      remove from service
      remove from route
      return True
    """
    pass

  def test_fail_terminate_twice(self):
    """ Tests terminate fail : Call terminate twice

    precondition:
      service exists

    postcondition:
      first call:
        remove from service
        remove from route
        return True
      second call:
        return exception
    """
    pass

  def test_fail_service_not_exists(self):
    """ Tests terminate fail : service not exists

    precondition:
      service not exists

    postcondition:
      return exception
    """
    self.res.mock(engine.app, '_collect_installed_apps', self.collect)
    self.res.mock(engine.isc, 'remove_service_route', Mock())
    self.res.mock(engine.mq, 'publish', Mock())
    engine.runtime.__DATA__.policy = mock_policy()

    appname = "SAMPLE"
    mock_app = engine.app.App(appname, appname)
    mock_app.state = engine.app.START_STATE
    engine.app.__DATA__.apps[appname] = mock_app

    self.assertRaises(Exception, engine.app.terminate, appname)

    self.assertEqual(0, engine.isc.remove_service_route.call_count)

  def test_fail_service_while_terminating(self):
    """ Tests terminate fail : service not exists

    precondition:
      service exists, state is terminating

    postcondition:
      return exception
    """
    pass


@attr(species="clique", genus="core", family="app", name="func")
class AppFuncTest(RpcTestCase):
  """ Tests for app helper functions and class
  """
  def setUp(self):
    self.install_path = os.path.join(__TEST_DIR__, str(uuid.uuid4())[:8])

  def tearDown(self):
    if os.path.exists(self.install_path):
      shutil.rmtree(self.install_path)

  @skip("lhtak")
  def test_check_required_app(self):
    """ Tests app._check_required_app
    """
    dest_list = {}
    dest_list['a01'] = Mock(version=1.1)
    dest_list['a02'] = Mock(version=2.6)
    dest_list['a03'] = Mock(version=3.4)
    dest_list['a04'] = Mock(version=4.8)
    dest_list['a05'] = Mock(version=5.3)
    dest_list['a06'] = Mock(version=1.1)

    app_info = Mock(required_apps="a01>=1.2", name="TestAPP")
    self.assertRaises(engine.app.ServiceRequestError,
                      engine.app._check_required_app, app_info, dest_list)
    app_info = Mock(required_apps="a02=<2.5", name="TestAPP")
    self.assertRaises(engine.app.ServiceRequestError,
                      engine.app._check_required_app, app_info, dest_list)
    app_info = Mock(required_apps="a03<3.2", name="TestAPP")
    self.assertRaises(engine.app.ServiceRequestError,
                      engine.app._check_required_app, app_info, dest_list)
    app_info = Mock(required_apps="a04>4.8", name="TestAPP")
    self.assertRaises(engine.app.ServiceRequestError,
                      engine.app._check_required_app, app_info, dest_list)
    app_info = Mock(required_apps="a05>5.3", name="TestAPP")
    self.assertRaises(engine.app.ServiceRequestError,
                      engine.app._check_required_app, app_info, dest_list)
    app_info = Mock(required_apps="a06==1.2", name="TestAPP")
    self.assertIsNone(engine.app._check_required_app(app_info, dest_list))
    app_info = Mock(required_apps="a07==1.3", name="TestAPP")
    self.assertRaises(engine.app.ServiceRequestError,
                      engine.app._check_required_app, app_info, dest_list)

  def test_parse_required_apps_info(self):
    """ Checks parsing data of required app info
    """
    data = "webserver>1.0 transcoder someapp>=12.01"
    parsed_data = engine.app._parse_required_apps_info(data.split())

    appname, version, sign = parsed_data[0]
    self.assertEqual(appname, 'webserver')
    self.assertEqual(version, '1.0')
    self.assertEqual(sign, '>')
    appname, version, sign = parsed_data[1]
    self.assertEqual(appname, 'transcoder')
    self.assertIsNone(version)
    self.assertIsNone(sign)
    appname, version, sign = parsed_data[2]
    self.assertEqual(appname, 'someapp')
    self.assertEqual(version, '12.01')
    self.assertEqual(sign, '>=')

  def test_app_error_clean(self):
    """ Test clean error if app state is chagned
    """
    app = engine.app.App('sample', 'sample')
    app.set_error(1, "error")
    self.assertEquals(app.error, (1, "error"))
    app.state = engine.app.START_STATE
    self.assertIsNone(app.error)
