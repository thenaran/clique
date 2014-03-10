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

"""The LED control module.
"""

import logging
import time
from threading import Thread
from Queue import Queue

from rpclib.proxy import SingleRpcProxy
from engine.runtime import DATA_PATH
from engine.isc import endpoint
from adt.concurrency import Lazy
from adt.concurrency import Timer
import adt.funcs
import clique.gpio as gpio
from engine.runtime import ROOT_PATH
import engine.aengel
import rpclib
import rpclib.proxy

__PATH__ = DATA_PATH + '''/.led'''


class SystemLed(object):
  MAX_BRIGHTNESS = 255

  def __init__(self):
    self.enable_system_led()

  def enable_system_led(self, enable=True):
    """Enable or disable the given LED.
    """
    led_path = ROOT_PATH + '''sys/class/leds/led0/trigger'''
    value = enable and 'none' or 'mmc0'
    try:
      return engine.aengel.execute_cmd_async("echo %s > %s" % (value, led_path))
    except:
      logging.warn("Error while enabling/disabling the LED.", exc_info=True)

  def set_brightness(self, brightness):
    """Control the given led as defined by a unique ID.

    brightness: scale from 0 to 255, 255 being the brightness possible value.
    """
    led_path = ROOT_PATH + '''sys/class/leds/led0/brightness'''
    value = str(adt.funcs.adjust_number(brightness, 0,
                                        SystemLed.MAX_BRIGHTNESS))
    try:
      return engine.aengel.execute_cmd_async("echo %s > %s" % (value, led_path))
    except:
      logging.warn("Error while controlling the brightness of the LED.",
                   exc_info=True)


class MainLed(object):
  # min should be 1, because gpio pwm not support zero.
  MIN_BRIGHTNESS = 1
  MAX_BRIGHTNESS = 1023
  WAITING_BRIGHTNESS = 200
  PWM_INTERVAL = 0.002
  MIN_SPEED = 1
  MAX_SPEED = MAX_BRIGHTNESS
  PAUSER = object()
  CENTER = object()
  LED_PIN = 1
  CENTER_LED_PIN = 0

  @classmethod
  def setup(cls):
    """Setup the main LED to use in the engine. It must be executed in the
    `beginning` function.
    """
    try:
      gpio.setup()
    except:
      logging.exception("Error configuring the main LED.")

  def __init__(self):
    gpio.pinMode(MainLed.LED_PIN, gpio.PWM_OUTPUT)
    gpio.pinMode(MainLed.CENTER_LED_PIN, gpio.OUTPUT)
    self._breathe_stopped = False
    self._breathe_thread = None
    self._brightness = 0
    logging.info("Main enabled")
    self._queue = Queue()
    Thread(target=self._do_work).start()

  def _do_work(self):
    while True:
      value, rate, interpolate = self._queue.get()
      if value is None:
        continue
      if value == MainLed.PAUSER:
        # sleep if pauser.
        time.sleep(rate)
        continue
      if value == MainLed.CENTER:
        self._on_center(rate)
        continue

      value = adt.funcs.adjust_number(value, MainLed.MIN_BRIGHTNESS,
                                      MainLed.MAX_BRIGHTNESS)
      speed = adt.funcs.adjust_number(rate, MainLed.MIN_SPEED)
      start_value = self._brightness
      sign = self._brightness > value and -1 or 1
      try:
        while True:
          if value * sign <= self._brightness * sign:
            break
          progress = (self._brightness - start_value) * sign / \
              (value - start_value) * sign
          speed = interpolate and \
                  adt.funcs.adjust_number(interpolate(progress),
                                          MainLed.MIN_SPEED) \
                  or speed
          self._brightness = \
              adt.funcs.adjust_number(self._brightness + speed * sign,
                                      MainLed.MIN_BRIGHTNESS,
                                      MainLed.MAX_BRIGHTNESS)
          gpio.pwmWrite(MainLed.LED_PIN, self._brightness)
          time.sleep(MainLed.PWM_INTERVAL)
      finally:
        self._queue.task_done()

  def _on_center(self, on=True):
    if on:
      gpio.digitalWrite(MainLed.CENTER_LED_PIN, 0)
    else:
      gpio.digitalWrite(MainLed.CENTER_LED_PIN, 1)

  def on_center(self, on=True):
    self._queue.put_nowait((MainLed.CENTER, on, None))

  def set_brightness(self, brightness, speed=MIN_SPEED, interpolate=None):
    self._queue.put_nowait((brightness, speed, interpolate))

  def set_pause(self, interval):
    """Pause the LED for the given amount of interval.
    """
    self._queue.put_nowait((MainLed.PAUSER, interval, None))

  def clear(self):
    """Clear the current LED routines.
    """
    old_queue = self._queue
    self._queue = Queue()
    # Just in case the old queue was blocking.
    old_queue.put_nowait((None, None, None))


def setup():
  gpio.setup_sys()


@endpoint()
def warn():
  """Warn the user.
  """
  SingleRpcProxy(__PATH__).warn()


@endpoint()
def fatal_error():
  """Let the user know that the system is unrecoverable (and must be shutdown).
  """
  SingleRpcProxy(__PATH__).fatal_error()


@endpoint()
def on(brightness=MainLed.MAX_BRIGHTNESS):
  """On all led.
  """
  SingleRpcProxy(__PATH__).on(brightness)


@endpoint()
def off():
  """Off all led.
  """
  SingleRpcProxy(__PATH__).off()


@endpoint()
def on_center():
  """On center led.
  """
  SingleRpcProxy(__PATH__).on_center()


@endpoint()
def off_center():
  """Off center led.
  """
  SingleRpcProxy(__PATH__).off_center()


@endpoint()
def blink(interval=1, count=10):
  """Blink led.
  """
  SingleRpcProxy(__PATH__).blink(interval, count)


@endpoint()
def breathe(speed=4, interval=1, count=10):
  """Breathe led.
  """
  SingleRpcProxy(__PATH__).breathe(speed, interval, count)


@endpoint()
def stop():
  """Stop led breathe or blink.
  """
  SingleRpcProxy(__PATH__).stop()


if __name__ == '__main__':
  import subprocess
  from rpclib.server import RpcServerOverUnixSocket
  from engine.runtime import CLIQUE_USER

  __DATA__ = Lazy()
  __DATA__.add_initializer('main_led',
                           lambda: MainLed())
  __DATA__.add_initializer('system_led', lambda: SystemLed())

  def do_warn():
    for i in range(20):
      __DATA__.main_led.set_brightness(MainLed.MAX_BRIGHTNESS, 8)
      __DATA__.main_led.set_brightness(MainLed.MIN_BRIGHTNESS, 8)
    __DATA__.main_led.set_brightness(MainLed.WAITING_BRIGHTNESS, 1)

  def do_fatal_error():
    toggle = [False]

    def blink():
        __DATA__.system_led.set_brightness(
            toggle[0] and SystemLed.MAX_BRIGHTNESS or 0)
        __DATA__.main_led.set_brightness(
            toggle[0] and MainLed.MAX_BRIGHTNESS or MainLed.MIN_BRIGHTNESS,
            MainLed.MAX_SPEED)
        toggle[0] = not toggle[0]
    Timer(rpclib.ioloop(), 1, blink, True)

  def do_on(brightness=MainLed.MAX_BRIGHTNESS):
    __DATA__.main_led.on_center()
    __DATA__.main_led.set_brightness(brightness, MainLed.MAX_SPEED)

  def do_off():
    __DATA__.main_led.on_center(False)
    __DATA__.main_led.set_brightness(MainLed.MIN_BRIGHTNESS, MainLed.MAX_SPEED)

  def do_on_center():
    __DATA__.main_led.on_center()

  def do_off_center():
    __DATA__.main_led.on_center(False)

  def do_blink(interval=1, count=10):
    toggle = [False]
    for i in range(count):
      if toggle[0]:
        __DATA__.main_led.on_center()
        __DATA__.main_led.set_brightness(MainLed.MAX_BRIGHTNESS,
                                         MainLed.MAX_SPEED)
      else:
        __DATA__.main_led.on_center(False)
        __DATA__.main_led.set_brightness(MainLed.MIN_BRIGHTNESS,
                                         MainLed.MAX_SPEED)
      __DATA__.main_led.set_pause(interval)
    __DATA__.main_led.set_brightness(MainLed.WAITING_BRIGHTNESS, 1)
    __DATA__.main_led.on_center()

  def do_breathe(speed=4, interval=1, count=10):
    """when the system is booting.
    """
    for i in range(count):
      __DATA__.main_led.set_brightness(MainLed.MAX_BRIGHTNESS, speed)
      __DATA__.main_led.set_pause(interval)
      __DATA__.main_led.set_brightness(MainLed.MIN_BRIGHTNESS, speed)
      __DATA__.main_led.set_pause(interval)
    __DATA__.main_led.set_brightness(MainLed.WAITING_BRIGHTNESS, 1)

  def do_stop():
    __DATA__.main_led.clear()
    __DATA__.main_led.set_brightness(MainLed.WAITING_BRIGHTNESS, 2)

  MainLed.setup()
  do_breathe()

  server = RpcServerOverUnixSocket(path=__PATH__)
  server.register_function(do_warn, name='warn')
  server.register_function(do_fatal_error, name='fatal_error')
  server.register_function(do_on, name='on')
  server.register_function(do_off, name='off')
  server.register_function(do_on_center, name='on_center')
  server.register_function(do_off_center, name='off_center')
  server.register_function(do_blink, name='blink')
  server.register_function(do_breathe, name='breathe')
  server.register_function(do_stop, name='stop')
  server.serve()
  subprocess.check_call('chown %s:%s %s' % ('root', CLIQUE_USER, __PATH__),
                        shell=True)

  rpclib.start()
