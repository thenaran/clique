#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Copyright 2012-2013 Narantech Inc. All rights reserved.
#  __    _ _______ ______   _______ __    _ _______ _______ _______ __   __
# |  |  | |   _   |    _ | |   _   |  |  | |       |       |       |  | |  |
# |   |_| |  |_|  |   | || |  |_|  |   |_| |_     _|    ___|       |  |_|  |
# |       |       |   |_||_|       |       | |   | |   |___|       |       |
# |  _    |       |    __  |       |  _    | |   | |    ___|      _|       |
# | | |   |   _   |   |  | |   _   | | |   | |   | |   |___|     |_|   _   |
# |_|  |__|__| |__|___|  |_|__| |__|_|  |__| |___| |_______|_______|__| |__|


""" Main module
"""


# Python default libraries
import logging
import atexit

# Clique related libraries
import clique.runtime


def start():
  logging.info("Completed to start a Hello app...")
  return "Hello"


def stop():
  logging.info("Terminating a Hello app...")


if __name__ == '__main__':
  try:
    logging.info("Booting a Hello app...")
    # Configure clique runtime.
    clique.runtime.configure(blocking=False)
    # Registers a function for terminating
    atexit.registration(stop)
    # Run app
    start()
  except:
    # Handle exception while running a app.
    logging.exception("Fail to running a Hello app")
  finally:
    # waiting until terminated
    clique.wait()
