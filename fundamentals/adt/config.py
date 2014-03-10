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


""" Configuration related ADTs.
"""


import logging
import json
import os
from StringIO import StringIO
from ConfigParser import ConfigParser


__VALUE__ = '''value'''


class Settings(object):
  """Abstraction for configuration setting values.
  """
  def __init__(self, paths):
    """
    Args:
      paths: a iterator of config file path
    """
    self._paths = paths
    self._config = ConfigParser(allow_no_value=True)
    self._config.read(paths)

  def get(self, section, option, default=None):
    """Gets the string value associated with the given option.
    """
    try:
      value = self._config.get(section, option)
      if value:
        return value
    except:
      pass
    self.set(section, option, default)
    return default

  def get_int(self, section, option, default=None):
    """Gets the integer value associated with the given option.
    """
    try:
      value = self._config.getint(section, option)
      if value:
        return value
    except:
      pass
    self.set(section, option, default)
    return default

  def get_float(self, section, option, default=None):
    """Gets the float value accociated with the given option.
    """
    try:
      value = self._config.getfloat(section, option)
      if value:
        return value
    except:
      pass
    self.set(section, option, default)
    return default

  def get_bool(self, section, option, default=None):
    """Gets the boolean value accociated with the given option.
    """
    try:
      value = self._config.getboolean(section, option)
      if value:
        return value
    except:
      pass
    self.set(section, option, default)
    return default

  def set(self, section, option, value):
    """Sets the given value to the option under the section.
    """
    if value is not None:
      if not self._config.has_section(section):
        self._config.add_section(section)
      self._config.set(section, option, str(value))
    elif self._config.has_section(section) and \
        self._config.has_option(section, option):
      # If the value is None, remove the option.
      self._config.remove_option(section, option)

  def has_section(self, section):
    """Check if the section exists.
    """
    return self._config.has_section(section)

  def has_option(self, section, option):
    """Check if the option exists.
    """
    return self._config.has_option(section, option)

  def options(self, section):
    """Returns a list of options available in the specified section
    """
    return self._config.options(section)

  def remove_option(self, section, option):
    """Remove the specified option from the specified section. If the section
    does not exist, raise NoSectionError. If the option existed to be removed,
    return True; otherwise return False.
    """
    return self._config.remove_option(section, option)

  def get_items(self, section, defaults=None):
    """Gets all key-value configurations under the section in dictionary.
    """
    if self.has_section(section):
      return dict(self._config.items(section))
    elif defaults:
      self._config.add_section(section)
      self.set_itmes(section, defaults)
    return defaults

  def set_items(self, section, items):
    """Sets the given key-value items under the section.
    """
    if items:
      for option, value in items.iteritems():
        self.set(section, option, value)

  def reload(self):
    """Reloads the settings from the file system.
    """
    self._config.read(self._paths)

  def flush(self, path=None):
    """Flushes the current settings to the given path.
    """
    def make_dir(p):
      d = os.path.dirname(p)
      os.path.exists(d) or os.makedirs(d)
    if path:
      make_dir(path)
      with open(path, 'wb') as f:
        self._config.write(f)
    else:
      for path in reversed(self._paths):
        try:
          make_dir(path)
          with open(path, 'wb') as f:
            self._config.write(f)
          break
        except:
          logging.exception("Failed to flush the settings.")


class JsonSettings(object):
  """Abstraction for configuration setting values.
  """
  def __init__(self, paths):
    """
    Args:
      paths: a iterator of config file path
    """
    self._paths = paths
    self._read(self._paths)

  def _read(self, paths):
    self._config = {}
    self._config_symbol = {}

    for path in paths:
      if os.path.exists(path):
        #if path.startswith('~clique'):
        #  raise Exception("%s %s" % (path, os.path.exists(path)))
        with open(path, 'r') as f:
          try:
            config = json.load(f)
            self._update_value(config)
          except:
            logging.exception("Occured error while loading a config.")
            with open(path, 'r') as rf:
              logging.warn("path: %s\ncontents:\n%s", path, rf.read())
            pass

  def _update_value(self, config):
    if not self._config:
      init_set = True
      self._config = config
    else:
      init_set = False

    values = [('', k, v) for k, v in config.items()]
    while values:
      parent_key, key, value = values.pop()
      key_io = StringIO()
      key_io.write(parent_key)
      key_io.write(key)
      value_set = value.get(__VALUE__)
      if isinstance(value_set, dict):
        key_io.write('.')
        for k, v in value_set.items():
          values.append((key_io.getvalue(), k, v))
      else:
        if init_set:
          self._config_symbol[key_io.getvalue()] = value
        elif value_set is None:
          pass
        else:
          self._config_symbol[key_io.getvalue()][__VALUE__] = value_set
        key_io.close()

  def _parse_key(self, key):
    """ Parsing a string key to a list sorted by hirerchy.
    """
    return key.split('.')

  def to_string(self):
    return json.dumps(self._config)

  def to_dict(self):
    return self._config

  def get(self, key, default=None):
    """Gets the string value associated with the key.
    """
    value_set = self._config_symbol.get(key)
    if value_set:
      value = value_set[__VALUE__]
      if value:
        return value
    value = default
    self.set(key, value)
    return value

  def get_int(self, key, default=0):
    """Gets the integer value associated with the key.
    """
    return int(self.get(key, default))

  def get_float(self, key, default=0):
    """Gets the float value accociated with the key.
    """
    return float(self.get(key, default))

  def get_bool(self, key, default=False):
    """Gets the boolean value accociated with the key.
    """
    return bool(self.get(key, default))

  def set(self, key, value):
    """Sets the given value to the key.
    """
    val = self._config_symbol.get(key)
    if val:
      val[__VALUE__] = value
    else:
      # initiailize key
      keys = self._parse_key(key)
      config = self._config
      level = 0
      for k in keys:
        level += 1
        v = config.get(k)
        if isinstance(v, dict) and __VALUE__ in v and isinstance(
                v[__VALUE__], dict):
          config = v.get(__VALUE__)
          continue
        break
      new_value = {__VALUE__: value}
      self._config_symbol[key] = new_value
      keys = keys[level - 1:]
      while keys:
        new_value = {__VALUE__: {keys.pop(): new_value}}
      config.update(new_value.values()[0])

  def __iter__(self):
    for key in self._config_symbol.keys():
      yield key

  def remove(self, key):
    """Removes the specified key.
    """
    if key in self._config_symbol:
      del self._config_symbol[key]

  def keys(self, key):
    """Returns a list of child keys available in the specified key
    """
    keys = self._parse_key(key)
    config = self._config
    for k in keys:
      config = config[k]
    return config.keys()

  def get_dict(self, key=None):
    """Gets items by dictionary set
    """
    config = self._config
    if key:
      keys = self._parse_key(key)
      for k in keys:
        config = config[k]
    return config

  def reload(self):
    """Reloads the settings from the file system.
    """
    self._read(self._paths)

  def flush(self, path=None):
    """Flushes the current settings value to the given path.
    """
    def make_dir(p):
      d = os.path.dirname(p)
      os.path.exists(d) or os.makedirs(d)
    if path:
      make_dir(path)
      with open(path, 'w') as f:
        json.dump(self._config, f)
    else:
      for path in reversed(self._paths):
        try:
          make_dir(path)
          with open(path, 'w') as f:
            json.dump(self._config, f)
          break
        except:
          logging.exception("Failed to flush the settings.")


class AppSettings(JsonSettings):

  def __init__(self, paths):
    JsonSettings.__init__(self, paths)
    self._handlers = set()

  def call_changed_handler(self, setting_values):
    if setting_values:
      for key, value in setting_values:
        JsonSettings.set(self, key, value)
    else:
      JsonSettings.reload(self)

    for handler in self._handlers:
      handler(setting_values)

  def register_config_changed_handler(self, func):
    self._handlers.add(func)

  def deregister_config_changed_handler(self, func):
    self._handlers.discard(func)
