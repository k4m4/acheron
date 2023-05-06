import socket
import logging
from pprint import pprint

class Peer:
  def __init__(self, torrent, peer_info):
    self.torrent = torrent
    self.ip = peer_info[b'ip'].decode('utf-8')
    self.port = peer_info[b'port']
    self.peer_id = peer_info[b'peer id']
    self.connected = False

    self.__debug(f'Creating peer')

  def connect(self):
    try:
      socket.inet_pton(socket.AF_INET6, self.ip)
      protocol = socket.AF_INET6
    except socket.error:
      try:
        socket.inet_pton(socket.AF_INET, self.ip)
        protocol = socket.AF_INET
      except socket.error:
        self.__warn(f'Invalid IP address: {self.ip}')
        return

    self.socket = socket.socket(protocol, socket.SOCK_STREAM)

    self.__debug(f'Connecting to {self.ip}:{self.port}')
    self.socket.connect((self.ip, self.port))
    self.connected = True
    self.__debug(f'Connected')

  def __warn(self, msg):
    logging.warn(f'[{self.peer_id}] {msg}')

  def __debug(self, msg):
    logging.debug(f'[{self.peer_id}] {msg}')
