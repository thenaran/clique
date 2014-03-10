# -*- coding: utf-8 -*-
# Copyright 2012-2013 Narantech Inc. All rights reserved.
#  __    _ _______ ______   _______ __    _ _______ _______ _______ __   __
# |  |  | |   _   |    _ | |   _   |  |  | |       |       |       |  | |  |
# |   |_| |  |_|  |   | || |  |_|  |   |_| |_     _|    ___|       |  |_|  |
# |       |       |   |_||_|       |       | |   | |   |___|       |       |
# |  _    |       |    __  |       |  _    | |   | |    ___|      _|       |
# | | |   |   _   |   |  | |   _   | | |   | |   | |   |___|     |_|   _   |
# |_|  |__|__| |__|___|  |_|__| |__|_|  |__| |___| |_______|_______|__| |__|


"""Storage management.
"""


# Default python lib
import logging
import os

# Thirdparty Lib
import pyudev

# Clique
import engine.mq
import engine.aengel
import engine.runtime
from engine.isc import endpoint
from engine.isc import PRIVILEGED
from adt.concurrency import Lazy
from engine.runtime import APPS_PATH
from engine.runtime import ROOT_PATH
from engine.runtime import CLIQUE_USER
from rpclib.proxy import Base


__DATA__ = Lazy()
__DATA__.mounts = {}         # devname : [(mount_point, fs_type), binded_path. ...]
__DATA__.registers = set()   # [appname, ...]


__FS_TYPE__ = '''ID_FS_TYPE'''
__DEVNAME__ = '''DEVNAME'''
__SIZE__ = '''UDISKS_PARTITION_SIZE'''
__NAME__ = '''ID_MODEL'''
__DEFAULT_BINDING_TARGET__ = '''media'''  # TODO : change media
__DEFAULT_BASE__ = ROOT_PATH + __DEFAULT_BINDING_TARGET__

# Mount Topics
MOUNT_ADD_TOPIC = '''mount.add'''
MOUNT_REMOVE_TOPIC = '''mount.remove'''


class StorageDevice(Base):
  ALL_POSSIBLE = '''*'''

  def __init__(self, ident, name, fs_type, usage, size, mount_point):
    """ Device's ident is path in the DEFAULT_BINDING_TARGET, it never be same as other's
    """
    self.ident = ident
    self.name = name
    self.fs_type = fs_type
    self.usage = usage
    self.size = size
    self.mount_point = mount_point
    self.bindings = set()
    self._usable = self.ALL_POSSIBLE

  def add_usable_app(self, app_name):
    if isinstance(self._usable, set):
      self._usable.add(app_name)
    else:
      self._usable = set([app_name])

  def is_usable(self, app_name):
    if self._usable == self.ALL_POSSIBLE:
      return True
    elif isinstance(self._usable, set) and app_name in self._usable:
      return True
    return False


class Storage(Base):
  """The storage for apps
  """
  def __init__(self, name, fs_type, usage, size, path):
    self.name = name
    self.fs_type = fs_type
    self.usage = usage
    self.size = size
    self.path = path

  @classmethod
  def build(cls, ident, dev_storage):
    return  Storage(dev_storage.name, dev_storage.fs_type,
                    dev_storage.usage, dev_storage.size,
                    _get_mountpath(ident))


@endpoint()
def register(appname):
  '''
  TODO: Can be assign a mount path by each app
  Changes mount base path from mnt to media
  '''
  __DATA__.registers.add(appname)
  for ident, dev_storage in __DATA__.mounts.items():
    if dev_storage.is_usable(appname):
      mntpath = _get_app_mountpath(ident, appname)
      mntpoint = dev_storage.mount_point
      mount_path_to_path(mntpoint, mntpath)
      dev_storage.bindings.add(mntpath)
  logging.info("Registered app '%s' for mounting", appname)


@endpoint()
def unregister(appname):
  __DATA__.registers.discard(appname)

  # umount
  for ident, dev_storage in __DATA__.mounts.items():
    mntpath = _get_app_mountpath(ident, appname)
    if mntpath in dev_storage.bindings:
      unmount_drive_from_app(mntpath)

  # remove bind target directory
  for ident, dev_storage in __DATA__.mounts.items():
    mntpath = _get_app_mountpath(ident, appname)
    if mntpath in dev_storage.bindings:
      remove_bind_target_dir(mntpath)
      dev_storage.bindings.discard(mntpath)
  logging.info("Unregistered app '%s' for mounting", appname)


@endpoint()
def get_storage_list(appname):
  """ Gets storage list.
  """
  storage_list = []
  for ident, dev_storage in __DATA__.mounts.items():
    app_mount_path = _get_app_mountpath(ident, appname)
    if app_mount_path in dev_storage.bindings:
      storage_list.append(Storage.build(ident, dev_storage))
  return storage_list


@endpoint()
def get_storage(appname, ident):
  """ Gets storage info.
  """
  dev_storage = __DATA__.mounts.get(ident)
  if dev_storage:
    app_mount_path = _get_app_mountpath(ident, appname)
    if app_mount_path in dev_storage.bindings:
      return Storage.build(ident, dev_storage)
    else:
      raise Exception("'%s' storage is not mounted to a '%s' app.",
                      ident, appname)
  else:
    raise Exception("Not exists a '%s' storage", ident)


def _get_mountpath(ident):
  return os.path.join(__DEFAULT_BINDING_TARGET__, str(ident))


def _get_app_mountpath(ident, appname):
  return os.path.join(APPS_PATH, appname, _get_mountpath(ident))


def mount_path_to_path(mntpoint, path):
  """Mount the external drive of the given ID.
  """
  if mntpoint != path:
    os.path.exists(path) or engine.aengel.execute_cmd("mkdir -p %s" % path)
    engine.aengel.execute_cmd('mount --bind %s %s' % (mntpoint, path))
    logging.debug("Mounted binding '%s' to '%s'", mntpoint, path)


def unmount_drive_from_app(path):
  """Unmount the external drive.
  """
  if not os.path.exists(path):
    logging.warn("%s to unmount is not exists.", path)
    return
  try:
    engine.aengel.execute_cmd('umount %s' % path)
    logging.debug("Unmounted binding '%s'", path)
  except:
    engine.aengel.execute_cmd('umount -l %s' % path)
    logging.warn("Failed umount. Try force umount.", exc_info=True)


def remove_bind_target_dir(path):
  """ Remove bind target directory.
  """
  try:
    engine.aengel.execute_cmd('rmdir %s' % path)
    logging.debug("Remove bind target directory : %s", path)
  except:
    if os.path.ismount(path):
      unmount_drive_from_app(path)
      engine.aengel.execute_cmd('rmdir %s' % path)
    else:
      engine.aengel.execute_cmd('rm -rf %s' % path)
    logging.warn("Failed to remove target directory. Try force remove it",
                 exc_info=True)


@endpoint(protection=PRIVILEGED)
def get_storage_device(ident):
  if ident in __DATA__.mounts:
    return __DATA__.mounts[ident]
  return None


@endpoint(protection=PRIVILEGED)
def list_storage_devices():
  """List all storage devices.
  """
  return __DATA__.mounts.values()


def get_storage_info(devname=None):
  """Retrieve a list of the storage spaces in the form of
  (devname, used, total) tuple in bytes for each device.
  If devname is specified, it will return only one value for the device.
  """
  if devname:
    output = engine.aengel.execute_cmd("df -k %s" % devname)
    device, size, used, available, percent, mntpoint = output.split('\n')[1].split()
    storage_device = __DATA__.mounts.get(devname)
    if storage_device:
      storage_device.used = used

    return (device, used, size)
  else:
    output = engine.aengel.execute_cmd("df -k")
    infos = []
    for data in output.split('\n')[1:]:
      device, size, used, _, _, _ = data.split()
      storage_device = __DATA__.mounts.get(device)
      if storage_device:
        storage_device.used = used
        infos.append((device, size, used))
    return infos


def get_calculate_usage(path):
  """Calculate the storage usage under the path. It might not finish immediately.
  """
  pass


@endpoint(protection=PRIVILEGED)
def unmount_storage_device(ident):
  logging.debug("unmount storage device ident:%s", str(ident))
  _unregister_device(ident)


@endpoint(protection=PRIVILEGED)
def mount_cifs_to_main(host, devname, username, password, label=None):
  """Mount cifs to main
     host - 192.168.1.1
     devname - /mydev/sambafolder
     label - name of cifs driver
     username - name of shared folder
     password - password of user
  """
  logging.debug("mount cifs host:%s, devname:%s, username:%s, password:%s, label:%s",
                str(host), str(devname), str(username), str(password), str(label))
  try:
    ident, mntpoint, label = _init_net_drv(host, devname, label)
  except Exception, e:
    logging.debug("fail to init nfs drv:%s", str(e))
    return e

  engine.aengel.execute_cmd('mount -t cifs //%s%s %s -o username=%s,password=%s,uid=%s,gid=%s' %
                            (str(host), str(devname), str(mntpoint), str(username),
                            str(password), str(CLIQUE_USER), str(CLIQUE_USER)))
  logging.debug("Mount cifs //%s%s %s", str(host), str(devname), str(mntpoint))

  # TODO handle dev size and usage
  storage_dev = StorageDevice(ident, label, 'cifs', 0, 0,
                              mntpoint)
  _register_device(storage_dev, mntpoint)


@endpoint(protection=PRIVILEGED)
def mount_nfs_to_main(host, devname, label=None):
  """Mount nfs to main
     host - 192.168.1.1
     devname - /mydev/nfsfolder
     label - name of nfs driver
  """
  logging.debug("mount nfs host:%s, devname:%s, label:%s",
                str(host), str(devname), str(label))
  try:
    ident, mntpoint, label = _init_net_drv(host, devname, label)
  except Exception, e:
    logging.debug("fail to init nfs drv:%s", str(e))
    return e

  engine.aengel.execute_cmd('mount -t nfs -o nolock %s:%s %s' %
                            (host, devname, mntpoint))
  logging.debug("Mount nfs %s:%s to %s",
                str(host), str(devname), str(label))

  # TODO handle dev size and usage
  storage_dev = StorageDevice(ident, label, 'nfs', 0, 0,
                              mntpoint)
  _register_device(storage_dev, mntpoint)


def _init_net_drv(host, devname, label):
  if not label:
    label = os.path.basename(devname)

  mntname = os.path.basename(devname)
  ident = os.path.join(str(host), mntname)

  mntpoint = os.path.join(__DEFAULT_BASE__, str(host), mntname)
  os.path.exists(mntpoint) or engine.aengel.execute_cmd("mkdir -p %s" % mntpoint)

  return (ident, mntpoint, label)


def mount_drive_to_main(fs_type, devname, mntpoint):
  cmd = 'mount -o iocharset=utf8 -t %s %s %s' % (fs_type, devname, mntpoint)

  if fs_type == 'vfat':
    cmd = 'mount -t %s -o iocharset=utf8,umask=000 %s %s' % (fs_type, devname, mntpoint)
  elif fs_type == 'ext3' or fs_type == 'ext2':
    cmd = 'mount -t %s -o iocharset=utf8,clique %s %s' % (fs_type, devname, mntpoint)

  engine.aengel.execute_cmd(cmd)
  logging.debug("Mounted '%s' to '%s'", devname, mntpoint)


def mount_share_app_dir(app_name, target, permits, able_apps):
  # TODO: Process permits
  ident = os.path.join(engine.runtime.context().get_app_root_dir(app_name),
                       target[1:])[1:]
  mntpoint = os.path.join(__DEFAULT_BASE__, ident)
  mount_path_to_path(ident, mntpoint)
  model_name = '{app_name}_{target_name}'.format(
      app_name=app_name, target_name=os.path.basename(target))
  storage_dev = StorageDevice(ident, model_name, 'share', 0, 0, mntpoint)
  if StorageDevice.ALL_POSSIBLE not in able_apps:
    for able_app in able_apps:
      storage_dev.add_usable_app(able_app)
  _register_device(storage_dev, mntpoint)


def unmount_share_app_dir(app_name, target):
  ident = os.path.join(engine.runtime.context().get_app_root_dir(app_name),
                       target[1:])[1:]
  _unregister_device(ident)


def _register_device(storage_dev, mntpoint):
  for app in __DATA__.registers:
    if storage_dev.is_usable(app):
      mntpath = os.path.join(engine.runtime.context().get_app_root_dir(app),
                             mntpoint[len(ROOT_PATH):])
      mount_path_to_path(mntpoint, mntpath)
      storage_dev.bindings.add(mntpath)
  __DATA__.mounts[storage_dev.ident] = storage_dev
  engine.mq.publish(MOUNT_ADD_TOPIC, storage_dev.ident)


def _unregister_device(ident):
  storage_dev = __DATA__.mounts.get(ident)
  if storage_dev:
    logging.info("Unmounts '%s', '%s'", ident, storage_dev.name)
    unmount_drive_from_app(storage_dev.mount_point)
    for path in storage_dev.bindings:
      unmount_drive_from_app(path)
    # Remove bind target directory
    for path in storage_dev.bindings:
      remove_bind_target_dir(path)
    remove_bind_target_dir(storage_dev.mount_point)
    del __DATA__.mounts[ident]
    engine.mq.publish(MOUNT_REMOVE_TOPIC, ident)
  else:
    logging.info("%s is not mounted", ident)


def handle_device_event(action, device):
  if __FS_TYPE__ not in device:
    # It is not file system
    return

  if action == 'add':
    devname = device[__DEVNAME__]
    ident = os.path.basename(devname)
    if ident in __DATA__.mounts:
      logging.info("%s is already mounted", devname)
    else:
      fs_type = device[__FS_TYPE__]
      model_name = device[__NAME__]
      logging.info("Mounts '%s', '%s', file system is '%s'", devname,
                   model_name, fs_type)
      mntpoint = os.path.join(__DEFAULT_BASE__, ident)
      if os.path.exists(mntpoint):
        logging.debug("device mount point exist:%s", str(mntpoint))
      else:
        engine.aengel.execute_cmd("mkdir %s" % mntpoint)

      mount_drive_to_main(fs_type, devname, mntpoint)
      _, usage, size = get_storage_info(devname)
      storage_dev = StorageDevice(ident, model_name, fs_type, usage, size,
                                  mntpoint)
      _register_device(storage_dev, mntpoint)
  elif action == 'remove':
    devname = device[__DEVNAME__]
    ident = os.path.basename(devname)
    _unregister_device(ident)


def validate():
  pass


def start():
  """
  Note:
    Edits comment of start function in main.py, if edit it.
  """
  try:
    context = pyudev.Context()
    engine.mq.create_topic(MOUNT_ADD_TOPIC)
    engine.mq.create_topic(MOUNT_REMOVE_TOPIC)

    for device in context.list_devices(subsystem='block', DEVTYPE='partition'):
      devname = device[__DEVNAME__]
      if devname.startswith('/dev/sd'):
        handle_device_event('add', device)

    monitor = pyudev.Monitor.from_netlink(context)
    monitor.filter_by('block')
    __DATA__.observer = pyudev.MonitorObserver(monitor, handle_device_event)
    __DATA__.observer.start()
  except:
    logging.exception("Fail to start storage")


def stop():
  try:
    __DATA__.observer.stop()
    del __DATA__.observer
    del __DATA__.mounts
    del __DATA__.registers
    engine.mq.delete_topic(MOUNT_ADD_TOPIC)
    engine.mq.delete_topic(MOUNT_REMOVE_TOPIC)
  except:
    logging.exception("Fail to stop storage")
