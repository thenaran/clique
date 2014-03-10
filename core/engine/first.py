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


"""Script to run for the first time booting (executed by aengel.py).

TODO:
  * generate public key and private key
  * remove the root private key
  * register serial number to myprota.me with its public key
  * delete itself...
  * flash the LED for QA to remove the power
"""


import subprocess
import StringIO


def generate_rsa_keys():
  """Generate the public-private key pair.
  """
  from engine.identity import generate_rsa_keys
  generate_rsa_keys()


def run_qa_tests():
  """Execute the QA tests (HDMI, wifi, RF, ethernet, USB).
  """
  # TODO: if failed, flash the LED (do not shutdown, stay on).
  # And raise an exception to stop.
  pass


def register():
  """Register the serial number.
  """
  import os
  import clique
  from engine.system import _serial_number
  from engine.runtime import STORE_PRIVATE_KEY_PATH
  from engine.runtime import STORE_PUBLIC_KEY_PATH
  from rpclib.proxy import SingleRpcProxy
  from rpclib.proxy import sync
  serial = _serial_number()
  address = ("reg.myprota.co", 19401)
  # Register timeout is 300 sec
  proxy = SingleRpcProxy(address, timeout=300)

  print "Connect with the Nest for register a serial '%s'." % serial
  pri_key, pub_key = sync(proxy.register_serial)(serial)
  print "Getting a store key."
  keydir = os.path.dirname(STORE_PRIVATE_KEY_PATH)
  os.path.exists(keydir) or os.makedirs(keydir)
  with open(STORE_PRIVATE_KEY_PATH, 'w') as wf:
    wf.write(pri_key)
  with open(STORE_PUBLIC_KEY_PATH, 'w') as wf:
    wf.write(pub_key)
  subprocess.check_call('chmod 600 %s' % STORE_PRIVATE_KEY_PATH, shell=True)
  print "Completed to register a serial '%s'." % serial


def finalize():
  """Finalize installation.
  """
  # TODO:
  # 1. LED notification when completed
  # 1. Remove itself
  # 1. DO NOT SHUTDOWN (Let QAs remove the power).
  import os
  if 'PROD' in os.environ:
    # Remove itself if it's a production version.
    os.remove(__file__)


def setup_interface():
  """Setup network interface.
  """
  interfaces_path = '/etc/network/interfaces'
  cmd = StringIO.StringIO()
  cmd.write('sed -i \'/wlan0/d\' %s;' % interfaces_path)
  cmd.write('sed -i \'/default/d\' %s;' % interfaces_path)
  cmd.write('sed -i \'/wpa-roam/d\' %s;' % interfaces_path)
  subprocess.check_call(cmd.getvalue(), shell=True)
  cmd.close()


if __name__ == '__main__':
  generate_rsa_keys()
  run_qa_tests()
  setup_interface()
  #register()
  finalize()
