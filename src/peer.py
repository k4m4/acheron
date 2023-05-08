import socket
import logging
import struct
from version import peer_id_to_human_peer_id
from pprint import pprint
from message import (
  HandshakeMessage,
  KeepAliveMessage,
  ChokeMessage,
  UnchokeMessage,
  InterestedMessage,
  NotInterestedMessage,
  HaveMessage,
  BitfieldMessage,
  RequestMessage,
  PieceMessage,
  CancelMessage,
  PortMessage
)
from math import ceil

PROTOCOL_STRING = b'BitTorrent protocol'
TEST_WITH_LOCAL_PEER = False
BLOCK_LENGTH = 16 * 1024

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

    self._debug(f'Creating peer')

  def connect(self):
    try:
      socket.inet_pton(socket.AF_INET6, self.ip)
      protocol = socket.AF_INET6
    except socket.error:
      try:
        socket.inet_pton(socket.AF_INET, self.ip)
        protocol = socket.AF_INET
      except socket.error:
        self._warn(f'Invalid IP address: {self.ip}')
        return

    self.socket = socket.socket(protocol, socket.SOCK_STREAM)

    self._debug(f'Connecting to {self.ip}:{self.port}')
    # TODO: handle error
    self.socket.connect((self.ip, self.port))
    self.connected = True
    self._debug(f'Connected')
    self._send_handshake()
    self._make_interested(True)
    self._main_loop()

  def _main_loop(self):
    buffer = b''
    while True:
      # blocking
      # self._debug(f'Waiting for data')
      buffer += self.socket.recv(4096)
      # self._debug(f'Received {len(buffer)} bytes')
      ready_to_consume = True
      if not buffer:
        continue
      while ready_to_consume:
        ready_to_consume = False
        try:
          buffer = self._on_data(buffer)
          if buffer:
            ready_to_consume = True
        except (struct.error, ValueError) as e:
          self._debug(f'Partial data received: {e}')

  def _on_data(self, buffer):
    if not self.handshook:
      # TODO: handle the case where no handshake is received first, but an error message is initially received
      try:
        buffer = self._on_handshake(buffer)
      except struct.error:
        # Handshake does not yet have enough bytes to complete
        return buffer
      self.handshook = True
      self._debug(f'Handshake completed')
      return buffer
    # TODO: handle the case where only a partial message is received
    length_prefix, = struct.unpack('!I', buffer[:4])
    if length_prefix == 0:
      buffer = self._on_keep_alive(buffer)
      return buffer
    message_id, = struct.unpack('!B', buffer[4:4 + 1])

    try:
      # TODO: refactor using decorators
      # TODO: check that the length correctly corresponds to the message id
      message_class = [
        ChokeMessage,
        UnchokeMessage,
        InterestedMessage,
        NotInterestedMessage,
        HaveMessage,
        BitfieldMessage,
        RequestMessage,
        PieceMessage,
        CancelMessage,
        PortMessage
      ]
      message, buffer = message_class[message_id].from_bytes(buffer)
      self._on_message(message)
    except IndexError:
      self._warn(f'Invalid message id: {message_id}')
    return buffer

  def _on_message(self, message):
    self._debug(f'Received message of type {type(message).__name__}: {message}')

    [
      self._on_choke,
      self._on_unchoke,
      self._on_interested,
      self._on_not_interested,
      self._on_have,
      self._on_bitfield,
      self._on_request,
      self._on_piece,
      self._on_cancel,
      self._on_port
    ][message.message_id](message)

  # TODO: DRY these 4 functions
  def _on_choke(self, choke_message):
    self.peer_choking = True

  def _on_unchoke(self, unchoke_message):
    self.peer_choking = False

    request_message = RequestMessage(index=0, begin=0, length=BLOCK_LENGTH)
    self._send_request(request_message)

  def _on_interested(self, interested_message):
    self.peer_interested = True

  def _on_not_interested(self, not_interested_message):
    self.peer_interested = False

  def _on_have(self, have_message):
    # TODO: refactor using MessageHave
    _, _, piece_index, = struct.unpack_from('!IBI', buffer)
    buffer = buffer[4 + 1 + 4:]
    self._mark_has(piece_index)

  def _ensure_piece_index_in_range(self, piece_index):
    if not 0 <= piece_index < self.torrent.num_pieces:
      # TODO: handle this gracefully
      raise ProtocolException(f'Invalid piece index: {piece_index}')

  def _mark_has(self, piece_index):
    self._ensure_piece_index_in_range(piece_index)
    self.has.add(piece_index)

  def _on_bitfield(self, bitfield_message):
    # TODO: ensure this message was sent immediately after the handshake
    # TODO: handle gracefully
    if bitfield_message.num_pieces != ceil(self.torrent.num_pieces / 8) * 8:
      raise ProtocolException(f'Invalid bitfield length. bitfield message num_pieces: {bitfield_message.num_pieces}, torrent num_pieces: {self.torrent.num_pieces}')

    for piece in bitfield_message.pieces:
      self._mark_has(piece)
    # self._debug(f'Peer has pieces: {self.has}')

  def _on_request(self, request_message):
    self._ensure_piece_index_in_range(request_message.index)
    # TODO: respond to request

  def _on_piece(self, piece_message):
    _, _, index, begin = struct.unpack_from('!IBII', buffer)
    block = buffer[4 + 1 + 4 + 4:]

    self._ensure_piece_index_in_range(index)

    # TODO: handle piece

  def _on_cancel(self, cancel_message):
    _, _, index, begin, length = struct.unpack_from('!IBIII', buffer)

    buffer = buffer[4 + 1 + 4 + 4 + 4:]

    self._ensure_piece_index_in_range(index)

  def _on_port(self, port_message):
    _, _, port, = struct.unpack_from('!IBH', buffer)

  def _on_keep_alive(self):
    self._debug(f'Received keep-alive message')

  def _on_handshake(self, buffer):
    handshake_message, buffer = HandshakeMessage.from_bytes(buffer)
    self._debug(f"Remote client is using protocol {handshake_message.data['protocol_string']}")
    matches = [
      {
        'expected': PROTOCOL_STRING,
        'actual': handshake_message.data['protocol_string'],
        'error': 'Invalid protocol string'
      },
      {
        'expected': self.torrent.info_hash,
        'actual': handshake_message.data['info_hash'],
        'error': 'Invalid info hash'
      },
      {
        'expected': self.peer_id,
        'actual': handshake_message.data['peer_id'],
        'warn': 'Peer id does not match'
      }
    ]
    for match in matches:
      if match['expected'] != match['actual']:
        if 'error' in match:
          self._close_with_error(f"{match['error']}: expected {match['expected']}, got {match['actual']}")
        else:
          # It is possible that the peer_id reported by the tracker
          # does not match the peer_id reported by the peer itself.
          # This is due to e.g., Azureus "anonymity" option
          # See: https://wiki.theory.org/BitTorrentSpecification#Handshake
          self._warn(f"{match['warn']}: expected {match['expected']}, got {match['actual']}")
    self.human_peer_id = peer_id_to_human_peer_id(self.peer_id)

    # TODO: show reserved bits
    self._debug(f'Remote peer is running {self.human_peer_id}')
    return buffer

  def _close_with_error(self, msg):
    self._warn(msg)
    self.socket.close()
    raise ProtocolException(msg)

  def _send_handshake(self):
    self._debug(f'Sending handshake')
    handshake_message = HandshakeMessage(
      protocol_string=PROTOCOL_STRING,
      info_hash=self.torrent.info_hash,
      peer_id=self.torrent.client.peer_id
    )

    self._send_message(handshake_message)

  def _make_interested(self, am_interested=True):
    self._debug(f'Changing interested flag to {am_interested}')
    if am_interested == self.am_interested:
      return
    self.am_interested = am_interested
    if am_interested:
      self._send_interested()
    else:
      self._send_not_interested()

  def _make_choking(self, am_choking=True):
    self._debug(f'Changing choking flag to {am_choked}')
    if am_choking == self.am_choking:
      return
    self.am_choking = am_choking
    if am_choked:
      self._send_choke()
    else:
      self._send_unchoke()

  def _send_choke(self, choke_message=ChokeMessage()):
    self._send_message(choke_message)

  def _send_unchoke(self, unchoke_message=UnchokeMessage()):
    self._send_message(unchoke_message)

  def _send_interested(self, interested_message=InterestedMessage()):
    self._send_message(interested_message)

  def _send_not_interested(self, not_interested_message=NotInterestedMessage()):
    self._send_message(not_interested_message)

  def _send_request(self, request_message):
    self._send_message(request_message)

  def _send_message(self, message):
    self._debug(f'Sending message of type {type(message).__name__}')
    self.socket.send(message.to_bytes())

  def _warn(self, msg):
    logging.warn(f'[{self._identifier()}] {msg}')

  def _debug(self, msg):
    logging.debug(f'[{self._identifier()}] {msg}')

  def _identifier(self):
    return f'{self.human_peer_id if self.human_peer_id is not None else self.peer_id} ({self.ip})'

  def __str__(self):
    return f'Peer {self._identifier()}'
