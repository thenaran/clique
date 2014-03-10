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


import os
import subprocess
import logging
from cStringIO import StringIO

import engine.node
from engine.runtime import APPS_PATH
from engine.runtime import CLIQUE_USER
from engine.runtime import HOST_NAME


__LONG_INTERVAL__ = 3000
__PACKAGE_EXTENSION__ = '''.tgz*'''


def execute_cmd(cmd, timeout=None):
  return subprocess.check_call(cmd, shell=True)


def update_progress(ident, now=0, total=100, response=None):
  print "%s : %d/%d" % (ident, now, total)

if os.getuid() != 0:
  import engine.aengel
  execute_cmd = engine.aengel.execute_cmd
  import engine.progress
  update_progress = engine.progress.update_progress


def set_app_core_permision(app_path, data_path):
  cmd = StringIO()

  cmd.write('chown -R %s:%s %s %s;' % (CLIQUE_USER, CLIQUE_USER, app_path,
                                       data_path))
  cmd.write('chmod -R 755 %s;' % app_path)
  cmd.write('chmod -R 775 %s' % data_path)
  execute_cmd(cmd.getvalue())
  cmd.close()


def merge_user(engine_conf, app_conf):
  engine_user = {}
  app_user = {}

  with open(engine_conf) as rf:
    for line in rf:
      engine_user[line.split(':')[0]] = line.strip()

  if os.path.exists(app_conf):
    with open(app_conf) as rf:
      for line in rf:
        app_user[line.split(':')[0]] = line.strip()

  io = StringIO()
  io.write('cat "%s" > %s;' % (engine_conf, app_conf))
  for key, value in app_user.items():
    if key not in engine_user:
      io.write('echo "%s" >> %s;' % (value, app_conf))
  execute_cmd(io.getvalue())
  io.close()


def install_app_data(app_base, app_name, dst_path):
  import json
  path = os.path.join(os.path.dirname(app_base), 'empty')
  install_cmd = StringIO()
  install_cmd.write('cd {path};'.format(path=path))
  install_cmd.write('cat {name}{ext} | tar xvzfp - -C {path}'.format(
      name='empty', ext=__PACKAGE_EXTENSION__, path=dst_path))
  cmd = install_cmd.getvalue()
  install_cmd.close()
  execute_cmd(cmd, timeout=__LONG_INTERVAL__)
  info_path = os.path.join(dst_path, 'app', '.info')
  with open(info_path, 'r') as f:
    config = json.load(f)
  config['application']['value']['name']['value'] = app_name
  config['application']['value']['display']['value'] = app_name
  with open(info_path, 'w') as f:
    json.dump(config, f)


def install_package_data(src_path, app_name, dst_path):
  install_cmd = StringIO()
  install_cmd.write('cd {path};'.format(path=src_path))
  install_cmd.write('cat {name}{ext} | tar xvzfp - -C {path}'.format(
      name=app_name, ext=__PACKAGE_EXTENSION__, path=dst_path))
  cmd = install_cmd.getvalue()
  install_cmd.close()
  return execute_cmd(cmd, timeout=__LONG_INTERVAL__)


def appinstall(path, appname, appbase, progress_ident=None):
  """Proceed to install the appliation files in `path` to app.
  """
  appname = appname.strip()
  app_dir = os.path.join(APPS_PATH, appname)
  ident = progress_ident or 'install_%s' % appname
  logging.info("Installing essential files for app '%s' running...", appname)

  try:
    update_progress(ident, 70)
    if os.path.exists(app_dir):
      execute_cmd('rm -rf %s' % app_dir)

    # create user
    os.system('mkdir -p %s' % app_dir)

    # Uncmpress app env into app directory
    logging.debug("Copying a appbase to %s...", app_dir)
    execute_cmd('rsync -ar %s/* %s/' % (appbase, app_dir),
                timeout=__LONG_INTERVAL__)
    logging.debug("Completed to copy a appbase.")
    execute_cmd('chown %s:%s %s' % (CLIQUE_USER, CLIQUE_USER, app_dir))

    # Crate home and data directory
    logging.debug("Creating home and data directories to %s...", app_dir)
    init_cmd = StringIO()
    data_dir = os.path.join(app_dir, 'data')
    app_home_dir = os.path.join(app_dir, 'home', appname)
    xauthority = os.path.join(app_dir, 'home', appname, '.Xauthority')
    if not os.path.exists(data_dir):
      init_cmd.write('mkdir -p %s;' % data_dir)
      init_cmd.write('chmod 775 %s;' % data_dir)
      init_cmd.write('chown %s:%s %s;' % (CLIQUE_USER, CLIQUE_USER,
                                          data_dir))
    if not os.path.exists(app_home_dir):
      init_cmd.write('mkdir -p %s;' % app_home_dir)
    init_cmd.write('touch %s;' % xauthority)
    init_cmd.write('chown -R %s:%s %s;' % (CLIQUE_USER, CLIQUE_USER,
                                           app_home_dir))
    init_cmd.write('chmod 600 %s;' % xauthority)
    execute_cmd(init_cmd.getvalue())
    init_cmd.close()
    logging.debug("Completed to create home and data directories.")

    update_progress(ident, 85)
    logging.info("Setting a essential structures...")
    # copy dir from path
    logging.debug("Syncing %s to %s...", path, app_dir)
    if os.path.exists(path):
      install_package_data(path, appname, app_dir)
    else:
      install_app_data(appbase, appname, app_dir)
    logging.debug("Completed to sync %s to %s", path, app_dir)
    update_progress(ident, 95)

    # make tmp dir
    dir_cmd = StringIO()
    tmp_dir_path = os.path.join(app_dir, 'tmp')
    if not os.path.exists(tmp_dir_path):
      dir_cmd.write('mkdir -p %s;' % tmp_dir_path)
      dir_cmd.write('chmod 1777 %s;' % tmp_dir_path)

    # make dev dir
    dev_dir_path = os.path.join(app_dir, 'dev')
    if not os.path.exists(dev_dir_path):
      dir_cmd.write('mkdir -p %s;' % dev_dir_path)
      dir_cmd.write('chmod 755 %s;' % dev_dir_path)
    dir_cmd.write('touch %s;' % os.path.join(dev_dir_path, 'null'))
    execute_cmd(dir_cmd.getvalue())
    dir_cmd.close()
    logging.info("Completed to set a essential structures...")

    # set permission
    logging.info("Setting permission a %s app...", appname)
    shm_path = '/var/run/shm'
    app_path = os.path.join(app_dir, 'app')
    data_path = os.path.join(app_dir, 'data')

    if os.path.exists(shm_path):
      execute_cmd('chmod 1777 %s' % shm_path)

    set_app_core_permision(app_path, data_path)
    update_progress(ident, 97)

    # Config /etc files
    etc_cmd = StringIO()

    passwd_path = os.path.join(app_dir, 'etc', 'passwd')
    group_path = os.path.join(app_dir, 'etc', 'group')
    hosts_path = os.path.join(app_dir, 'etc', 'hosts')
    hostname_path = os.path.join(app_dir, 'etc', 'hostname')

    # Read /etc/passwd and find user, then write.
    merge_user('/etc/passwd', passwd_path)
    merge_user('/etc/group', group_path)
    etc_cmd.write('sed -i s/clique/%s/g %s;' % (appname, passwd_path))
    etc_cmd.write('chmod 644 %s;' % passwd_path)
    etc_cmd.write('chown root:root %s;' % passwd_path)
    etc_cmd.write('sed -i s/clique/%s/g %s;' % (appname, group_path))
    etc_cmd.write('chmod 644 %s;' % group_path)
    etc_cmd.write('chown root:root %s;' % group_path)

    # Fix /etc/hosts and /etc/hostname
    if not os.path.exists(hosts_path):
      etc_cmd.write('cp -af %s %s;' % ('/etc/hosts', hosts_path))
    etc_cmd.write('sed -i \'$s/$/\\n127.0.1.1 %s/\' %s;' % (HOST_NAME,
                                                            hosts_path))
    etc_cmd.write('sed -i \'$s/$/\\n127.0.0.1 %s/\' %s;' % (appname,
                                                            hosts_path))
    if not os.path.exists(hostname_path):
      etc_cmd.write('cp -af %s %s;' % ('/etc/hostname', hostname_path))
    etc_cmd.write('echo "%s" > %s;' % (appname, hostname_path))
    execute_cmd(etc_cmd.getvalue())
    etc_cmd.close()
    logging.info("Completed to set permission a %s app...", appname)
    update_progress(ident, 98)

    # Create symlink
    src = '/opt/vc/lib'
    if os.path.exists(src):
      symlink_cmd = StringIO()
      for name in os.listdir(src):
        src_path = os.path.join(src, name)
        target_path = os.path.join(app_dir, 'usr/lib', name)
        arm_target_path = os.path.join(app_dir, 'usr/lib/arm-linux-gnueabihf',
                                       name)
        symlink_cmd.write('ln -fs %s %s;' % (src_path, target_path))
        symlink_cmd.write('ln -fs %s %s;' % (src_path, arm_target_path))
      execute_cmd(symlink_cmd.getvalue())
      symlink_cmd.close()
      logging.debug("Completed create /opt/vc/lib symbolic link.")

    # Create usb dir
    usb_path_1 = os.path.join(app_dir, "mnt/sda1")
    usb_path_2 = os.path.join(app_dir, "mnt/sdb1")
    usb_cmd = StringIO()
    usb_cmd.write('mkdir %s %s;' % (usb_path_1, usb_path_2))
    usb_cmd.write('chown %s:%s %s %s;' % (CLIQUE_USER, CLIQUE_USER, usb_path_1,
                                          usb_path_2))
    usb_cmd.write('chmod 777 %s %s;' % (usb_path_1, usb_path_2))
    execute_cmd(usb_cmd.getvalue())
    usb_cmd.close()
    logging.debug("Completed create usr dir from /mnt.")

    logging.info("Completed to installing essential files for app"
                 "'%s' running...", appname)
    update_progress(ident, 99)
  except:
    logging.exception("Failed to install")
    raise


if __name__ == "__main__":
  import sys
  _, path, appname, appbase = sys.argv
  appinstall(path, appname, appbase)
