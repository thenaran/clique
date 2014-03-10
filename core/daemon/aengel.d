#! /bin/sh
### BEGIN INIT INFO
# Provides:          aengel.d
# Required-Start:    $remote_fs $syslog
# Required-Stop:     $remote_fs $syslog
# Default-Start:     2 3 4 5
# Default-Stop:      0 1 6
# Short-Description: Clique service script.
# Description:       Clique service script.
### END INIT INFO
# Here is /etc/init.d/clique
#
# Copyright 2012-2013 Narantech Inc. All rights reserved.
#  __    _ _______ ______   _______ __    _ _______ _______ _______ __   __
# |  |  | |   _   |    _ | |   _   |  |  | |       |       |       |  | |  |
# |   |_| |  |_|  |   | || |  |_|  |   |_| |_     _|    ___|       |  |_|  |
# |       |       |   |_||_|       |       | |   | |   |___|       |       |
# |  _    |       |    __  |       |  _    | |   | |    ___|      _|       |
# | | |   |   _   |   |  | |   _   | | |   | |   | |   |___|     |_|   _   |
# |_|  |__|__| |__|___|  |_|__| |__|_|  |__| |___| |_______|_______|__| |__|
#
# Clique Engine

#------------------------------------------------------------------------------
#                               Consts
#------------------------------------------------------------------------------
EXEC=/usr/bin/python
AENGEL=/core/engine/aengel.pyc
PRERUN_BASE=/data/prerun
PRERUN=$PRERUN_BASE/prerun.sh
NO_PRERUN=$PRERUN_BASE/.no_prerun

PS="aengel"
PIDNAME=$PS
PIDFILE=$PIDNAME.pid
PIDSPATH=/var/running

DESCRIPTION="Clique $PS for taking care of clique process..."

RUNAS=root

SCRIPT_OK=0
SCRIPT_ERROR=1
TRUE=1
FALSE=2

lockfile=/var/lock/$PS

#------------------------------------------------------------------------------
#                               Funtions
#------------------------------------------------------------------------------
touch /var/lock/

setFilePerms(){
  if [ -f $PIDSPATH/$PIDFILE ]; then
    chmod 400 $PIDSPATH/$PIDFILE
  fi
}

getPSCount() {
  return 'pgrep -f $PS | wc -l'
}

log_aengel_msg() {
  logger "$@";
}

log_end_msg() {
  [ $1 -eq 0 ] && RES=OK; logger ${RES:=FAIL};
}

isRunning() {
  pidof_aengel
  PID=$?

  if [ $PID -gt 0 ]; then
    return 1
  else
    return 0
  fi
}

pidof_aengel() {
  PID='pidof $PS' || true

  [ -e $PIDSPATH/$PIDFILE ] && PIDS2='cat $PIDSPATH/$PIDFILE'

  for i in $PIDS; do
    if [ "$i" = "$PIDS" ]; then
      return 1
    fi
  done

  return 0
}

removePIDFile(){
  if [ $1 ]; then
    if [ -f $1 ]; then
      rm -f $1
    fi
  else
    #Do default removal
    if [ -f $PIDSPATH/$PIDFILE ]; then
      rm -f $PIDSPATH/$PIDFILE
    fi
  fi
}

start(){
  log_aengel_msg "Starting $DESCRIPTION"

  isRunning
  isAlive=$?

  if [ -f $PRERUN -a ! -f $NO_PRERUN ]; then
    /bin/sh $PRERUN
    rm -rf $PRERUN_BASE
  fi

  if [ "${isAlive}" -eq $TRUE ]; then
    log_end_msg $SCRIPT_ERROR
  else
    export PROD=1 &&
    start-stop-daemon --start --quiet --chuid $RUNAS --pidfile $PIDSPATH/$PIDFILE --exec $EXEC -- $AENGEL
    setFilePerms
    log_end_msg $SCRIPT_OK
  fi
}


case "$1" in
  start)
    start
    ;;
  *)
    FULLPATH=/etc/init.d/$PS
    echo "Usage: $FULLPATH {start}"
    exit 1
    ;;
esac

exit 0
