import socket
import logging
import struct
from version import peer_id_to_human_peer_id
from pprint import pprint
from message import HandshakeMessage

PROTOCOL_STRING = b'BitTorrent protocol'
TEST_WITH_LOCAL_PEER = False

class ProtocolException(Exception):
  pass

class Peer:
  def __init__(self, torrent, peer_info):
    self.torrent = torrent
    if TEST_WITH_LOCAL_PEER:
      self.ip = '127.0.0.1'
      self.port = 6881
    else:
      self.ip = peer_info[b'ip'].decode('utf-8')
      self.port = peer_info[b'port']
    self.peer_id = peer_info[b'peer id']
    self.connected = False

    self.am_choking = True
    self.peer_choking = True
    self.am_interested = False
    self.peer_interested = False

    self.handshook = False
    self.human_peer_id = None

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
    # TODO: handle error
    self.socket.connect((self.ip, self.port))
    self.connected = True
    self.__debug(f'Connected')
    self.__send_handshake()
    self.__main_loop()

  def __main_loop(self):
    buffer = b''
    while True:
      # blocking
      self.__debug(f'Waiting for data')
      buffer += self.socket.recv(4096)
      self.__debug(f'Received {len(buffer)} bytes')
      ready_to_consume = True
      if not buffer:
        continue
      while ready_to_consume:
        ready_to_consume = False
        try:
          buffer = self.__on_data(buffer)
          ready_to_consume = True
        except struct.error:
          pass

  def __on_data(self, buffer):
    if not self.handshook:
      # TODO: handle the case where no handshake is received first, but an error message is initially received
      buffer = self.__on_handshake(buffer)
      self.handshook = True
      self.__debug(f'Handshake completed')
      return buffer
    # TODO: handle non-handshake messages

  def __on_handshake(self, buffer):
    handshake_message, remaining_buffer = HandshakeMessage.from_bytes(buffer)
    self.__debug(f'Remote client is using protocol {handshake_message.protocol_string}')
    matches = [
      {
        'expected': PROTOCOL_STRING,
        'actual': handshake_message.protocol_string,
        'error': 'Invalid protocol string'
      },
      {
        'expected': self.torrent.info_hash,
        'actual': handshake_message.info_hash,
        'error': 'Invalid info hash'
      },
      {
        'expected': self.peer_id,
        'actual': handshake_message.peer_id,
        'warn': 'Peer id does not match'
      }
    ]
    for match in matches:
      if match['expected'] != match['actual']:
        if 'error' in match:
          self.__close_with_error(f"{match['error']}: expected {match['expected']}, got {match['actual']}")
        else:
          # It is possible that the peer_id reported by the tracker
          # does not match the peer_id reported by the peer itself.
          # This is due to e.g., Azureus "anonymity" option
          # See: https://wiki.theory.org/BitTorrentSpecification#Handshake
          self.__warn(f"{match['warn']}: expected {match['expected']}, got {match['actual']}")
    self.human_peer_id = peer_id_to_human_peer_id(self.peer_id)

    self.__debug(f'Remote peer is running {self.human_peer_id}')
    return remaining_buffer

  def __close_with_error(self, msg):
    self.__warn(msg)
    self.socket.close()
    raise ProtocolException(msg)

  def __send_handshake(self):
    self.__debug(f'Sending handshake')
    handshake_message = HandshakeMessage(PROTOCOL_STRING, self.torrent.info_hash, self.torrent.client.peer_id)

    self.__send_message(handshake_message)

  def __send_message(self, message):
    self.__debug('Sending message')
    self.__debug(message.to_bytes())

    self.socket.send(message.to_bytes())

  def __warn(self, msg):
    logging.warn(f'[{self.__identifier()}] {msg}')

  def __debug(self, msg):
    logging.debug(f'[{self.__identifier()}] {msg}')

  def __identifier(self):
    return f'{self.human_peer_id if self.human_peer_id is not None else self.peer_id} ({self.ip})'

  def __str__(self):
    return f'Peer {self.__identifier()}'
