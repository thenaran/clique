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


"""Identity management with SSL public keys.
"""
import os
import time
import logging
import ssl

from adt.concurrency import Lazy
from adt.db import DoubleKeyDict
import engine.runtime
import engine.aengel
from engine.isc import PRIVILEGED
from engine.isc import endpoint
from engine.runtime import APPS_PATH


# Identity kinds
NODE_KIND = 0
ADMIN_KIND = 1

__ROOT_KEY_FILENAME__ = '''id_rsa'''
__ROOT_CERT_FILENAME__ = '''id_rsa.pub'''

CA_CERTS_FILENAME = '''ca_certs'''
KEY_FILENAME = '''keyfile'''
CERT_FILENAME = '''certfile'''

__IDENTITY_PATH__ = '''.ident'''

__DATA__ = Lazy()
__DATA__.add_initializer('cert_path',
                         lambda: engine.runtime.context().rebase_to_data(
                             CERT_FILENAME))
__DATA__.add_initializer('key_path',
                         lambda: engine.runtime.context().rebase_to_data(
                             KEY_FILENAME))
__DATA__.add_initializer('ca_certs',
                         lambda: engine.runtime.context().rebase_to_data(
                             CA_CERTS_FILENAME))

__DATA__.add_initializer('ident_db_path',
                         lambda: engine.runtime.context().rebase_to_data(
                             __IDENTITY_PATH__))
__DATA__.add_initializer('ident_db',
                         lambda: _init_db())


def _init_db():
  dd = DoubleKeyDict(int, __DATA__.ident_db_path)
  return dd


class Identity(object):
  def __init__(self, node_id, pubkey, registered_date):
    self.node_id = node_id
    self.pubkey = pubkey
    self.registered_date = registered_date

  def __repr__(self):
    return "Identity {node_id: %s, pubkey: %s, registered_date: %s}" % \
           (str(self.node_id), str(self.pubkey), str(self.registered_date))


def register(node_id, pubkey):
  if pubkey not in __DATA__.ident_db:
    logging.debug("register pubkey of node_id:%s, pubkey:%s",
                  str(node_id), str(pubkey))
    __DATA__.ident_db[node_id, pubkey] = int(time.time())
  else:
    raise Exception("pubkey of node_id:%s is already registered", str(node_id))


def unregister(node_id):
  logging.debug("unregister identity name:%s", str(node_id))
  del __DATA__.ident_db[node_id]


def find(pubkey):
  """Find an :class:`Identity` for the given public key. `None` if not known
  identity.
  """
  try:
    return Identity(*__DATA__.ident_db[pubkey])
  except:
    logging.exception("fail to find ident with pubkey=%s", str(pubkey))


def get_ssl_options():
  return {'certfile': __DATA__.cert_path,
          'keyfile': __DATA__.key_path,
          }


def get_server_ssl_options():
  return {'certfile': __DATA__.cert_path,
          'keyfile': __DATA__.key_path,
          'cert_reqs': ssl.CERT_OPTIONAL,
          'ca_certs': __DATA__.ca_certs
          }


def get_cert(raw=False):
  """ get certification path
  if raw is True, return binary data of cert
  """
  cert = __DATA__.cert_path
  if raw:
    with open(cert) as f:
      cert = f.read()
  return cert


def generate_rsa_keys():
  """Generate the public-private key pair.
  """
  from engine.runtime import DATA_PATH
  import tempfile
  import uuid
  import subprocess

  ca_cert_path = os.path.join(DATA_PATH, CA_CERTS_FILENAME)
  if os.path.exists(ca_cert_path):
    return

  root_key = os.path.join(DATA_PATH, __ROOT_KEY_FILENAME__)
  root_cert = os.path.join(DATA_PATH, __ROOT_CERT_FILENAME__)
  password = str(uuid.uuid4())  # safer than number-only random passwords
  subject = "/C=KO/ST=SEOUL/L=SEOUL/O=Narantech Inc/OU=CLIQUE/CN=CLIQUE/emailAddress=narantech@narantech.com"

  key_path = os.path.join(DATA_PATH, KEY_FILENAME)
  req_path = tempfile.mkstemp()[1]
  priv_path = tempfile.mkstemp()[1]
  cert_path = os.path.join(DATA_PATH, CERT_FILENAME)

  subprocess.check_call('openssl genrsa -out %s 2048' % priv_path, shell=True)
  subprocess.check_call('openssl req -passout pass:%s -subj "%s" -key %s -new '
                        '> %s' % (password, subject, priv_path, req_path),
                        shell=True)
  subprocess.check_call('openssl rsa -passin pass:%s -in %s -out %s' %
                        (password, priv_path, key_path), shell=True)
  subprocess.check_call('openssl x509 -req -in %s -out %s -signkey %s -CA %s '
                        '-CAkey %s -CAcreateserial -days 1095' %
                        (req_path, cert_path, key_path, root_cert, root_key),
                        shell=True)
  subprocess.check_call("mv %s %s" % (root_cert, ca_cert_path), shell=True)
  subprocess.check_call("rm %s %s" % (req_path, priv_path), shell=True)


@endpoint(elevated=True, protection=PRIVILEGED)
def set_store_key(appname, owner=None):
  if owner:
    ssh_dir = 'home/%s/.ssh' % owner
  else:
    ssh_dir = 'root/.ssh'

  ssh_dir = os.path.join(APPS_PATH, appname, ssh_dir)
  engine.aengel.execute_cmd('mkdir -p %s' % ssh_dir)
  engine.aengel.execute_cmd('cp -af %s* %s/' % (
      engine.runtime.STORE_PRIVATE_KEY_PATH, ssh_dir))

  if owner:
    engine.aengel.execute_cmd('chown -R clique:clique %s' % ssh_dir)
  else:
    engine.aengel.execute_cmd('chown -R root:root %s' % ssh_dir)
