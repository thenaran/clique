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


"""Administration module.
"""


def register(email, password, displayname):
  """Register a new admin.
  """
  pass


def unregister(email):
  """Remove the admin. Note that at least one admin must exist.
  """
  pass


def add_identity(email, title, ident):
  """Add the :class:`Identity` under the admin account.
  """
  pass


def remove_identity(email, title):
  """Remove the identity named `title` from the admin.
  """
  pass


def list_identities(email):
  """List all identities of the admin.
  """
  pass


def is_admin(ident):
  """Check if the given :class:`Identity` represents an admin.
  """
  pass


def get_admin(email):
  """Get the admin data object.
  """
  pass


def list_all_admins():
  """Return all admin data objects.
  """
  pass


class Admin(object):
  """Administrator data asbtraction.
  """
  def __init__(self, email, displayname, registered_date, last_login_date,
               last_login_ip, num_failed_login_attemps):
    pass
