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

"""DB
"""
import logging
import sqlite3 as sql

from cStringIO import StringIO

__TEXT_TYPE__ = '''TEXT'''
__INTEGER_TYPE__ = '''INTEGER'''
__REAL_TYPE__ = '''REAL'''


class DoubleKeyDict(object):
  """DoubleKeyDict that has double key set and value that has double key and data
     if path is not None db will be initialized
  Usase:
     1. set item  dic[key1, key2] = data
     2. get item  key1, key2, data = dic[key1 or key2]
     3. remove item del dic[key]
     4. has item key in dic

  """
  def __init__(self, typ, path=None):
    self._typ = typ
    self._conn = None
    self._map = {}
    self._path = path
    if path:
      self._init_db(path)

  def _get_type_str(self):
    if self._typ == str:
      return __TEXT_TYPE__
    if self._typ == int:
      return __INTEGER_TYPE__
    if self._typ == float:
      return __REAL_TYPE__

  def _init_db(self, path):
    self._conn = sql.connect(path)
    self._conn.isolation_level = None
    cur = self._conn.cursor()

    try:
      query = ("CREATE TABLE IF NOT EXISTS Dic( \
               key1 TEXT PRIMARY KEY, \
               key2 TEXT NOT NULL, \
               data %s NOT NULL \
               )" %
               (self._get_type_str()))
      cur.execute(query)
      cur.execute('CREATE INDEX IF NOT EXISTS key1_index on Dic (key1)')
      cur.execute('CREATE INDEX IF NOT EXISTS key2_index on Dic (key2)')
      cur.execute('CREATE INDEX IF NOT EXISTS data_index on Dic (data)')
      logging.debug("Table for double key dic is created. the type str:%s",
                    str(self._get_type_str()))
    except:
      logging.exception("Fail to initialize db on path:%s", str(path))
    finally:
      cur.close()

    all_items = self._get_all_items()
    if all_items and len(all_items) > 0:
      for item in all_items:
        self._insert(item[0], item[1], item[2])

  def close(self):
    if self._conn:
      self._conn.close()

  def values(self):
    return self._map.values()

  def keys(self):
    return self._keys()

  def _get_all_items(self):
    res = []
    if self._path:
      try:
        cur = self._conn.cursor()
        cur.execute('SELECT * FROM Dic')
        res = cur.fetchall()
      except:
        logging.exception("Failed to get all items")
      finally:
        cur.close()
    return res

  def __getitem__(self, key):
    return self._map[key]

  def __len__(self):
    return len(self._map)

  def __setitem__(self, double_key, data):
    key1, key2 = double_key
    self._insert(key1, key2, data)
    self._insert_to_db(key1, key2, data)

  def __delitem__(self, key):
    self._remove(key)
    self._remove_from_db(key)

  def __contains__(self, item):
    return item in self._map

  def __iter__(self):
    return self._map.iterkeys()

  def _remove(self, key):
    key1, key2, data = self._map[key]
    del self._map[key1]
    del self._map[key2]

  def _insert(self, key1, key2, data):
    self._map[key1] = key1, key2, data
    self._map[key2] = key1, key2, data

  def _insert_to_db(self, key1, key2, data):
    if self._path:
      cur = self._conn.cursor()
      try:
        cur.execute('INSERT INTO Dic VALUES(?, ?, ?)',
                    (key1,
                     key2,
                     data
                     ))
        self._conn.commit()
      except:
        self._conn.rollback()
        logging.warn("Fail to insert key1:%s, key2:%s, data:%s",
                     str(key1), str(key2), str(data), exc_info=True)
        raise
      finally:
        cur.close()

  def _remove_from_db(self, key):
    if self._path:
      cur = self._conn.cursor()
      try:
        cur.execute('DELETE FROM Dic WHERE key1=? or key2=?', (key, key))
        self._conn.commit()
      except:
        logging.exception("Failed to remove key:%s", str(key))
        self._conn.rollback()
      finally:
        cur.close()

  def __repr__(self):
    result = StringIO()
    result.write("{")
    for key in self._map:
      result.write("%s, ", str(key))
    result.write("}")
    return result.getvalue()
