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
    self.has = set()

    self.socket = None

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
      try:
        buffer = self.__on_handshake(buffer)
      except struct.error:
        # Handshake does not yet have enough bytes to complete
        return buffer
      self.handshook = True
      self.__debug(f'Handshake completed')
      return buffer
    # TODO: handle non-handshake messages
    length_prefix, = struct.unpack('!I', buffer[:4])
    buffer = buffer[4:]
    if length_prefix == 0:
      self.__on_keep_alive()
      return buffer
    message_buffer = buffer[:length_prefix]
    message_id, = struct.unpack('!B', message_buffer[:1])
    message_buffer = message_buffer[1:]

    try:
      # TODO: refactor using decorators
      # TODO: check that the length correctly corresponds to the message id
      [
        self.__on_choke,
        self.__on_unchoke,
        self.__on_interested,
        self.__on_not_interested,
        self.__on_have,
        self.__on_bitfield,
        self.__on_request,
        self.__on_piece,
        self.__on_cancel,
        self.__on_port
      ][message_id](message_buffer)
    except IndexError:
      self.__warn(f'Invalid message id: {message_id}')
    return buffer

  # TODO: DRY these 4 functions
  def __on_choke(self, message_buffer):
    self.__debug(f'Received choke message')
    self.peer_choking = True

  def __on_unchoke(self, message_buffer):
    self.__debug(f'Received unchoke message')
    self.peer_choking = False

  def __on_interested(self, message_buffer):
    self.__debug(f'Received interested message')
    self.peer_interested = True

  def __on_not_interested(self, message_buffer):
    self.__debug(f'Received not interested message')
    self.peer_interested = False

  def __on_have(self, message_buffer):
    piece_index, = struct.unpack('!I', message_buffer[:4])
    message_buffer = message_buffer[4:]
    self.__debug(f'Received have message for piece {piece_index}')
    self.__mark_has(piece_index)
    self.has.add(piece_index)

  def __ensure_piece_index_in_range(self, piece_index):
    if not 0 <= piece_index < self.torrent.num_pieces:
      # TODO: handle this gracefully
      raise ProtocolException(f'Invalid piece index: {piece_index}')

  def __mark_has(self, piece_index):
    self.__ensure_piece_index_in_range(piece_index)
    self.has.add(piece_index)

  def __on_bitfield(self, message_buffer):
    # TODO: ensure this message was sent immediately after the handshake
    # TODO: ensure length of bitfield message matches the expected length
    self.__debug(f'Received bitfield message')

    for i, byte in enumerate(message_buffer):
      for j in range(8):
        if byte & (1 << (7 - j)):
          self.__mark_has(i * 8 + j)

    self.__debug(f'Peer has pieces: {self.has}')

  def __on_request(self, message_buffer):
    request_message = RequestMessage(message_buffer)

    self.__ensure_piece_index_in_range(request_message.index)
    self.__debug(f'Received request message for piece {request_message.index} at offset {request_message.begin} with length {request_message.length}')

    # TODO: respond to request

  def __on_piece(self, message_buffer):
    index, begin = struct.unpack('!II', message_buffer[:8])
    block = message_buffer[8:]

    self.__ensure_piece_index_in_range(index)
    self.__debug(f'Received piece message for piece {index} at offset {begin} with length {len(block)}')

    # TODO: handle piece

  def __on_cancel(self, message_buffer):
    index, begin, length = struct.unpack('!III', message_buffer)

    self.__ensure_piece_index_in_range(index)
    self.__debug(f'Received cancel message for piece {index} at offset {begin} with length {length}')

  def __on_port(self, message_buffer):
    port, = struct.unpack('!H', message_buffer)

    self.__debug(f'Received port message with port {port}')

  def __on_keep_alive(self):
    self.__debug(f'Received keep-alive message')

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

    # TODO: show reserved bits
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

  def __send_request(self):
    self.__debug(f'Sending request')

  def __send_message(self, message):
    self.__debug('Sending message')

    self.socket.send(message.to_bytes())

  def __warn(self, msg):
    logging.warn(f'[{self.__identifier()}] {msg}')

  def __debug(self, msg):
    logging.debug(f'[{self.__identifier()}] {msg}')

  def __identifier(self):
    return f'{self.human_peer_id if self.human_peer_id is not None else self.peer_id} ({self.ip})'

  def __str__(self):
    return f'Peer {self.__identifier()}'
