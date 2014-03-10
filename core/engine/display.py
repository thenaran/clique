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


""" Display resource.

Display here means the physical monitor-like device to output graphical data.
Screen refers to a virtual space for drawing the actual graphical contents.

It runs the X server and manage it.
"""

import os
import logging
import subprocess
from subprocess import Popen, PIPE
import Xlib.display
from threading import Thread

from adt.concurrency import Lazy
from rpclib.proxy import Base
from engine.isc import endpoint
import engine.mq
import engine.led


DISPLAY_CONNECTED_TOPIC = '''display.connected'''
DISPLAY_DISCONNECTED_TOPIC = '''display.disconnected'''


__DATA__ = Lazy()
# TODO: if the node name changes, it must be re-initialized.
__DATA__.add_initializer('display', lambda: Display.current())


class Display(Base):
  """Abstract representation of graphical display.
  """
  def __init__(self, nodename, device_number, screen_number=None):
    self.ident = nodename + ':' + device_number + \
                 (screen_number and '.' + screen_number or '')
    self.device_name = __DATA__.tvservice.get_device_name()
    self._display = Xlib.display.Display(self.ident)

  @classmethod
  def current(cls):
    import engine.node
    try:
      return Display(engine.node.name(), 0)
    except:
      logging.exception("Error initializing the display.")
    return None


class _Xserver(object):
  """X display manager.
  """
  def __init__(self):
    s = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'display.s')
    self.pipe = Popen('xinit %s' % s, shell=True, stdout=PIPE)

  def stop(self):
    self.pipe.kill()


class _TvService(object):
  """Display control service.
  """
  def __init__(self):
    self.stopped = False
    Thread(target=self._run).start()

  def reconnect(self):
    """Reconnect the display with preferred settings.
    """
    subprocess.check_call('tvservice -p')

  def power_off(self):
    """Power off the display.
    """
    subprocess.check_call('tvservice -o')

  def get_device_name(self):
    """Connected display device name.
    """
    output = subprocess.check_output('tvservice -n')
    try:
      return output.split('=')[1]
    except:
      logging.debug("Error getting the display device name.", exc_info=True)
    return ''

  def stop(self):
    self.stopped = True

  def _run(self):
    pipe = Popen('tvservice -M', shell=True, stdout=PIPE)
    output = pipe.stdout
    last_line = None
    for line in output:
      if self.stopped or pipe.returncode is None:
        break
      if line != last_line:
        try:
          if 'unplugged' in line:
            # disconnected display.
            del __DATA__.display
            engine.mq.publish(DISPLAY_DISCONNECTED_TOPIC, None)
            engine.led.warn()
          elif 'HDMI in standby mode' in line:
            # display connected.
            self.reconnect()
            engine.mq.publish(DISPLAY_CONNECTED_TOPIC, None)
            engine.led.notify()
        except:
          logging.exception("Error handling the tvservice output.")
        last_line = line
      else:
        logging.debug("tvservice: the same output. ignored.")
      logging.debug("tvservice: %s", line)
    pipe.kill()
    logging.info("tvservice terminated.")


def validate():
  pass


def start():
  """Start the display service.
  Note:
    Edits comment of start function in main.py, if edit it.
  """
  engine.mq.create_topic(DISPLAY_CONNECTED_TOPIC)
  engine.mq.create_topic(DISPLAY_DISCONNECTED_TOPIC)
  __DATA__.xserver = _Xserver()
  __DATA__.tvservice = _TvService()


def stop():
  """Stop the display service.
  """
  __DATA__.xserver.stop()
  del __DATA__.xserver
  __DATA__.tvservice.stop()
  del __DATA__.tvservice
  del __DATA__.display
  engine.mq.delete_topic(DISPLAY_CONNECTED_TOPIC)
  engine.mq.delete_topic(DISPLAY_DISCONNECTED_TOPIC)


@endpoint()
def get_display():
  """Gets the current display information. If not connected, `None` is returned.
  """
  return __DATA__.display


def set_display_settings(width, height, refresh_rate):
  """Sets the current display settings.
  """
  pass


def get_display_settings():
  """Gets the supported display settings.
  """
  pass


def try_reconnect():
  """Try to reconnect the display in case it's not hot-pluggable.
  """
  __DATA__.tvservice.reconnect()


def power_on_off(on):
  """Set the display power on/off.
  """
  if not on:
    __DATA__.tvservice.power_off()
  else:
    try_reconnect()


def allow(nodename):
  """Allow the node to connect to the current node's display.
  """
  # TODO: must create the authority file first to restrict other users.
  pass


def disallow(nodename):
  """Disallow the node to connect to the current node's display.
  """
  pass
