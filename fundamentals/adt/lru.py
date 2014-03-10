#
# Copyright 2012 Narantech Inc.

# This program is a property of Narantech Inc. Any form of infringement is
# strictly prohibited. You may not, but not limited to, copy, steal, modify
# and/or redistribute without appropriate permissions under any circumstance.
#
#  __    _ _______ ______   _______ __    _ _______ _______ _______ __   __
# |  |  | |   _   |    _ | |   _   |  |  | |       |       |       |  | |  |
# |   |_| |  |_|  |   | || |  |_|  |   |_| |_     _|    ___|       |  |_|  |
# |       |       |   |_||_|       |       | |   | |   |___|       |       |
# |  _    |       |    __  |       |  _    | |   | |    ___|      _|       |
# | | |   |   _   |   |  | |   _   | | |   | |   | |   |___|     |_|   _   |
# |_|  |__|__| |__|___|  |_|__| |__|_|  |__| |___| |_______|_______|__| |__|


"""LRU cache library.
"""


def none_func(key):
  return None


class _DoublyLinkedNode:
  def __init__(self, prev, key, item, next):
    self.prev = prev
    self.key = key
    self.item = item
    self.next = next


class LruCache(object):
  """An LRU cache of a given size caching calls to a given function.
  """
  def __init__(self, size, if_missing=none_func):
    """Create an LRUCache given a size and a function to call for missing keys.
    """
    self.size = size
    self.slow_lookup = if_missing
    self.hash = {}
    self.list_front = None
    self.list_end = None

  def get(self, key):
    """Get the value associated with a certain key from the cache.
    """
    if key in self.hash:
      return self._from_cache(key)
    else:
      new_item = self.slow_lookup(key)
      if new_item:
        self.put(key, new_item)
      return new_item

  def _from_cache(self, key):
    """Look up a key known to be in the cache.
    """
    node = self.hash[key]
    assert node.key == key, "Node for LRU key has different key"

    if node.prev is None:
      # it's already in front
      pass
    else:
      # Link the nodes around it to each other
      node.prev.next = node.next
      if node.next is not None:
        node.next.prev = node.prev
      else:  # Node was at the list_end
        self.list_end = node.prev

      # Link the node to the front
      node.next = self.list_front
      self.list_front.prev = node
      node.prev = None
      self.list_front = node

    return node.item

  def _kick_item(self):
    """Kick an item from the cache, making room for a new item.
    """
    last = self.list_end
    if last is None:  # Same error as [].pop()
      raise IndexError("Can't kick item from empty cache")

    # Unlink from list
    self.list_end = last.prev
    if last.prev is not None:
      last.prev.next = None

    # Delete from hash table
    del self.hash[last.key]
    last.prev = last.next = None  # For GC purposes

  def put(self, key, item):
    if len(self.hash) >= self.size:
      self._kick_item()

    node = _DoublyLinkedNode(None, key, item, None)
    # Link node into place
    node.next = self.list_front
    if self.list_front is not None:
      self.list_front.prev = node
    self.list_front = node

    # Add to hash table
    self.hash[key] = node
