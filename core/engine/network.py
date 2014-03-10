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


"""Network management.

TODO:
Refer to https://github.com/rockymeza/wifi for wifi connections.
"""


# Python Default Libraries
import os
import psutil
import logging
import tempfile
from cStringIO import StringIO

# Clique related
import engine.aengel
import engine.ifcfg
import engine.runtime
import engine.wifi
from engine.isc import endpoint
from adt.concurrency import Lazy
from rpclib.proxy import Base


__IPTABLE_ORIGINAL__ = '''/etc/iptables.original'''
__SYSCTL__ = '''/etc/sysctl.conf'''
__SYSCTL_BACKUP__ = '''/etc/sysctl.conf.bk'''
__RESOLV_CONF__ = '''/etc/resolv.conf'''

__MACADDR_KEY__ = '''ether'''
__IPADDR_KEY__ = '''inet'''
__NETMASK_KEY__ = '''netmask'''
__SSID_KEY__ = '''ssid'''

__DISABLE_IP_ADDRESS__ = ['127.0.0.1', None]

__STATIC_TYPE__ = '''static'''
__DHCP_TYPE__ = '''dhcp'''
__DEBIAN_INTERFACES_PATH__ = '''/etc/network/interfaces'''
__DEBIAN_INTERFACES_PATH_BACKUP__ = '''/etc/network/interfaces.bk'''
__DEBIAN_INTERFACES_FORM__ = '''
auto lo

iface lo inet loopback
iface %s inet %s
'''

__DATA__ = Lazy()


class NetworkInterface(Base):
  """Abstract representation of a network interface containing typical
  ifconfig information.
    * mac address
    * IP address
    * Netmask
    * ssid (optional)
  """
  def __init__(self, macaddr, ipaddr, netmask, ssid='', gateway=''):
    self.macaddr = macaddr
    self.ipaddr = ipaddr
    self.netmask = netmask
    self.ssid = ssid
    self.gateway = gateway


def validate():
  restore_settings()


def start():
  """Initialize network setting.
  """
  port_data = __DATA__.portmap.get_items().items()
  for d_p, to_p in port_data:
    do_map_network_port(to_p, d_p)


def stop():
  pass


@endpoint(name='list_network_interface_info')
def __list_network_interface_info__():
  return list_network_interfaces()


def list_network_interfaces():
  """List the available network interfaces.
  """
  interfaces = {}
  interface_info = engine.ifcfg.interfaces()

  for name, device in interface_info.items():
    interfaces[name] = NetworkInterface(
        macaddr=device.get(__MACADDR_KEY__), ipaddr=device.get(__IPADDR_KEY__),
        netmask=device.get(__NETMASK_KEY__), ssid=device.get(__SSID_KEY__))

  # interface is none, then route is occur error
  try:
    output = engine.aengel.execute_cmd('route -n').strip()
    if output:
      lines = output.strip().split('\n')
      for line in lines[2:]:
        dest, gate, _, flags, _, _, _, iface = line.strip().split()
        if dest != '0.0.0.0' or not 'G' in flags:
          continue
        if iface in interfaces:
          interfaces[iface].gateway = gate
  except:
    logging.warn("Failed to gets the gateway address and interface.",
                 exc_info=True)

  return interfaces


def list_nameservers():
  with open(__RESOLV_CONF__, 'r') as rf:
    return [line.strip().split()[1] for line in rf]


@endpoint()
def get_link_stats(pid=None, interface=None):
  """Retrieve a list of network statistics in the form of (uplink, downlink)
  in bytes for each network interface. If `interface` is specified, it will
  return only one stat value for the interface.

  Returns:
    (uplink state, downlink state)
  """
  if pid:
    return None

  stat = psutil.network_io_counters()
  return stat.bytes_sent, stat.bytes_recv


@endpoint(elevated=True)
def add_nameservers(ips):
  """
  Args:
    ip: collection or sting of nameserver ip
  """
  nameservers = list_nameservers()
  ips = [ips] if isinstance(ips, str) else ips
  for ip in ips:
    if not ip in nameservers:
      engine.aengel.execute_cmd('echo "nameserver %s" >> %s' %
                                (ip, __RESOLV_CONF__))
      logging.info("add nameserver '%s'", ip)
    else:
      logging.warn("nameserver '%s' is duplicated", ip)


def _rollback_debian_network_interface():
  if os.path.exists(__DEBIAN_INTERFACES_PATH_BACKUP__):
    cmd = 'cat %s > %s' % (__DEBIAN_INTERFACES_PATH_BACKUP__,
                           __DEBIAN_INTERFACES_PATH__)
    engine.aengel.execute_cmd(cmd)


def _save_debian_network_interface(interface, type, address=None,
                                   netmask=None, gateway=None):
  io = StringIO()
  io.write(__DEBIAN_INTERFACES_FORM__ % (interface, type))
  if type == __STATIC_TYPE__:
    if address:
      io.write('\n  address\t%s' % address)
    if netmask:
      io.write('\n  netmask\t%s' % netmask)
    if gateway:
      io.write('\n  gateway\t%s' % gateway)
  # backup
  cmd = 'cat %s > %s' % (__DEBIAN_INTERFACES_PATH__,
                         __DEBIAN_INTERFACES_PATH_BACKUP__)
  engine.aengel.execute_cmd(cmd)
  try:
    # save
    cmd = 'echo "%s" > %s' % (io.getvalue(), __DEBIAN_INTERFACES_PATH__)
    engine.aengel.execute_cmd(cmd)
  except:
    logging.exception("Occurs error while saving a network settings")
    _rollback_debian_network_interface()


@endpoint(elevated=True)
def set_ip_address(interface, ip=None, netmask=None, gateway=None,
                   nameservers=None):
  """Set the IP address of the given interface. If `ip` is None, then the IP
  address will be given by the DHCP server.
  """
  _check_network_interface(interface)
  nameservers and add_nameservers(nameservers)

  if ip and netmask:
    cmd_io = StringIO()
    cmd_io.write('ifconfig %s down;ifconfig %s %s' %
                 (interface, interface, ip))
    netmask and cmd_io.write(' netmask %s' % netmask)
    cmd_io.write(' up')
    engine.aengel.execute_cmd(cmd_io.getvalue())
    cmd_io.close()
    logging.debug("Set %s interface static ip: %s, netmask: %s",
                  interface, ip, netmask)
    _save_debian_network_interface(interface, __STATIC_TYPE__, ip, netmask,
                                   gateway)
  elif ip:
    engine.aengel.execute_cmd('ifconfig %s %s' % (interface, ip))
    logging.debug("Set %s interface static ip : %s", interface, ip)
  else:
    engine.aengel.execute_cmd('echo "" > %s' % __RESOLV_CONF__)
    if interface.startswith('eth'):
      engine.aengel.execute_cmd('dhclient %s' % interface)
      logging.debug("Set %s interface dhcp ip.", interface)
    elif interface.startswith('wlan'):
      engine.aengel.execute_cmd('ifconfig %s 0.0.0.0' % interface)
      logging.debug("Set %s interface reset ip.", interface)
    else:
      logging.debug("Interface not hw device. name is %s", interface)
    _save_debian_network_interface(interface, __DHCP_TYPE__)

  if gateway:
    engine.aengel.execute_cmd('route add default gw %s dev %s' %
                              (gateway, interface))
    logging.debug("Set %s gateway to %s", ip, interface)


@endpoint()
def share_connection(source, target):
  """Share the source interface's internet connection with `target` interface.
  Change sysctl.conf, must be execute cmd 'sysctl -p'
  """
  """TODO:
    Backup the current /etc/sysctl.conf. Add net.ipv4.ip_forward=1 at the end.
    If there's the backup file already, raise an error.
    Then, do the following:
      iptables -t nat -A POSTROUTING -o *source* -j MASQUERADE
      iptables -A FORWARD -i *source* -o *target* -m state --state RELATED,ESTABLISHED -j ACCEPT
      iptables -A FORWARD -i *target* -o *source* -j ACCEPT
  """
  if os.path.exists(__SYSCTL_BACKUP__):
    raise Exception("Already shared connection.")
  _check_network_interface(source)
  _check_network_interface(target)

  share_cmd = StringIO()
  if not os.path.exists(__IPTABLE_ORIGINAL__):
    share_cmd.write('sh -c iptables-save > %s' % __IPTABLE_ORIGINAL__)
    logging.debug("Save original iptable value")

  share_cmd.write('cp -a %s %s;' % (__SYSCTL__, __SYSCTL_BACKUP__))
  share_cmd.write('echo \'net.ipv4.ip_forward=1\' >> %s;' % __SYSCTL__)
  share_cmd.write('iptables -t nat -A POSTROUTING -o %s -j MASQUERADE;' %
                  source)
  share_cmd.write('iptables -A FORWARD -i %s -o %s -m state --state'
                  ' RELATED,ESTABLISHED -j ACCEPT;' % (source, target))
  share_cmd.write('iptables -A FORWARD -i %s -o %s -j ACCEPT;' % (target,
                                                                  source))
  share_cmd.write('sysctl -p')
  engine.aengel.execute_cmd(share_cmd.getvalue())
  share_cmd.close()

  logging.debug("Set shared connection %s to %s", source, target)


@endpoint()
def restore_settings(refresh=None):
  """Restore all network settings.
  Note:
    Edits comment of start function in main.py, if edit it.
  """
  """TODO:
    1. IP addresses
    For all network interfaces, call set_ip_address(interface, None)

    2. IP forward settings
    restore the /etc/sysctl.conf.bk and remove it.
    iptables-restore < /etc/iptables.original

    Note that /etc/iptables.original has to be created in the genesis phase by
    sudo sh -c "iptables-save > /etc/iptables.original"
  """

  restore_cmd = StringIO()
  if refresh:
    interface_list = list_network_interfaces()
    for interface in interface_list.keys():
      set_ip_address(interface, None)

  if os.path.exists(__SYSCTL_BACKUP__):
    restore_cmd.write('cp -af %s %s;' % (__SYSCTL_BACKUP__, __SYSCTL__))
    restore_cmd.write('rm %s;' % __SYSCTL_BACKUP__)
    logging.debug("Restore sysctl.conf.")

  if os.path.exists(__IPTABLE_ORIGINAL__):
    restore_cmd.write('iptables-restore < %s;' % __IPTABLE_ORIGINAL__)
    logging.debug("Restore iptables.")

  engine.aengel.execute_cmd(restore_cmd.getvalue())
  restore_cmd.close()
  logging.debug("Success restore network settings.")


@endpoint(name='get_ip')
def __get_ip(interface=None):
  return get_ip(interface)


def get_ip(interface=None):
  """Get ip address for interface.
  """
  try:
    ifaces = list_network_interfaces()
    if interface:
      return ifaces[interface].ipaddr
    else:
      for value in ifaces.values():
        if value.ipaddr not in __DISABLE_IP_ADDRESS__:
          return value.ipaddr
      return None
  except:
    raise Exception("%s is not include interface list." % interface)


@endpoint()
def get_interface_info(interface):
  """Get info of interface.
  """
  try:
    info = list_network_interfaces()[interface]
    return (info.ipaddr, info.netmask, info.gateway)
  except:
    raise Exception("%s is not include interface list." % interface)


@endpoint()
def execute_iptables(args_list):
  iptables_cmd = StringIO()
  for args in args_list:
    iptables_cmd.write('iptables %s;' % args)

  output = engine.aengel.execute_cmd(iptables_cmd.getvalue())
  iptables_cmd.close()
  return output


def _check_network_interface(interface):
  if interface not in list_network_interfaces():
    raise Exception("%s is not include network interface list." % interface)


# Wifi Configuration {{{


@endpoint()
def list_ssids(interface):
  """List all SSIDs for the given wireless `interface`.
  """
  return engine.wifi.Cell(interface)


def connect_to_encrypted_ssid(interface, ssid, ident, passkey, encrypted):
  cmd = StringIO()
  conf_path = tempfile.NamedTemporaryFile().name
  if encrypted == 'wpa2':
    cmd.write('wpa_passphrase {ssid} {pwd} > {path};'.format(
        ssid=ssid, pwd=passkey, path=conf_path))
    cmd.write('wpa_supplicant -B -i{iface} -Dwext -c{path};'.format(
        iface=interface, path=conf_path))
    cmd.write('dhclient {iface}'.format(iface=interface))
    engine.aengel.execute_cmd(cmd.getvalue(), timeout=120)
    cmd.close()


def connect_to_ssid(interface, ssid, ident, passkey):
  cell = None
  for c in engine.wifi.Cell(interface):
    if c.ssid == ssid:
      cell = c
      break

  if cell:
    ident = ident or ssid
    scheme = engine.wifi.Scheme.for_cell(interface, ident, cell, passkey)
    logging.debug("Connecting to SSID=%s", ssid)
    scheme.save()
    scheme.activate()
    logging.info("Connected to SSID=%s", ssid)
    return True
  else:
    logging.warn("Failed to connect to SSID=%s, passkey=%s", ssid, passkey)
    return False


@endpoint(name="connect_to_ssid")
def __connect_to_ssid__(interface, ssid, ident, passkey, encrypted=None):
  """Connect to the given wireless `ssid`.
  """
  if encrypted:
    connect_to_encrypted_ssid(interface, ssid, ident, passkey, encrypted)
  else:
    connect_to_ssid(interface, ssid, ident, passkey)
  ipaddr = get_ip(interface)
  return ipaddr not in __DISABLE_IP_ADDRESS__


# }}}


# Port forwading configuration {{{


__PRE_ROUT_CHAIN__ = '''PREROUTING'''
__PORT_MAP_CONF__ = os.path.join(engine.runtime.HOME_PATH, 'portmap.conf')
__DATA__.add_initializer('portmap', lambda: PortMapLog())


class PortMapLog(object):

  def __init__(self):
    from adt.config import Settings
    self._config = Settings([__PORT_MAP_CONF__])
    self._attr = 'portmap'

  def get_items(self):
    return self._config.get_items(self._attr, {})

  def add(self, port, d_port):
    if self._config.has_option(self._attr, d_port):
      logging.warn("%s -> %s is duplicated.", d_port, port)
    else:
      self._config.set(self._attr, d_port, port)
      self._config.flush()

  def delete(self, d_port):
    if self._config.has_option(self._attr, d_port):
      self._config.remove_option(self._attr, d_port)
      self._config.flush()
    else:
      logging.warn("a %s port map is not exsits.", d_port)


@endpoint(elevated=True)
def map_network_port(port, d_port, interface=None, save=False):
  p = str(port)
  d_p = str(d_port)
  if do_map_network_port(p, d_p, interface) and save:
    __DATA__.portmap.add(p, d_p)


def do_map_network_port(port, d_port, interface=None):
  """Map the given `d_port` to the previleged port (usually < 1024).
  """
  logging.debug("Inserting a port mapping '%s' -> '%s'", d_port, port)
  ports_map = list_mapped_ports()
  port_data = ports_map.get(d_port)
  if port_data:
    logging.warn("Already mapped a port map '%s' -> '%s'", d_port, port)
    return False

  cmd = '-A %s -t nat -p tcp -m tcp --dport %s -j REDIRECT --to-ports %s' %\
      (__PRE_ROUT_CHAIN__, d_port, port)
  execute_iptables([cmd])
  logging.debug("Inserted a port mapping '%s' -> '%s'", d_port, port)
  return True


@endpoint(elevated=True)
def unmap_network_port(port, d_port, interface=None, save=False):
  p = str(port)
  d_p = str(d_port)
  try:
    if do_unmap_network_port(p, d_p, interface) and save:
      __DATA__.portmap.delete(d_p)
  except:
    logging.exception("Failed to remove '%s' -> '%s'", d_port, port)


def do_unmap_network_port(port, d_port, interface=None):
  """Un-map a destination port mappings.
  """
  logging.info("Deleting a '%s' -> '%s' port mapping", d_port, port)
  ports_map = list_mapped_ports()
  port_data = ports_map.get(d_port)
  if port_data and port_data[0] == port:
    to_p, num = port_data
    cmd = '-D %s %s -t nat' % (__PRE_ROUT_CHAIN__, num)
    execute_iptables([cmd])
    logging.debug("Deleted '%s' -> '%s'", port, to_p)
    return True
  else:
    logging.warn("a '%s' -> '%s' port mapping is not exists", d_port, port)
    return False


@endpoint(elevated=True)
def list_mapped_ports(interface=None):
  """List the mapped network ports.
  """
  output = execute_iptables(['-S %s -t nat' % __PRE_ROUT_CHAIN__])
  lines = output.strip().split('\n')[1:]

  ports_map = {}
  num = 0   # rule number
  for line in lines:
    fields = line.strip().split()
    flag, _, _, _, _, _, _, d_port, _, _, _, to_port = fields
    if flag == '-A':
      port_data = ports_map.get(d_port)
      if port_data:
        logging.warn("Duplicated %s -> %s with %s -> %s.", d_port, to_port,
                     d_port, port_data[0])
        continue
      num += 1
      ports_map[d_port] = (to_port, num)

  logging.debug("Gets ports map %s", ports_map)
  return ports_map


# }}}
