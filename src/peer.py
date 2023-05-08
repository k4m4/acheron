import socket
import logging
import struct
from version import peer_id_to_human_peer_id
from pprint import pprint
from message import *
from math import ceil

PROTOCOL_STRING = b'BitTorrent protocol'
TEST_WITH_LOCAL_PEER = False
BLOCK_LENGTH = 16 * 1024

class ProtocolException(Exception):
  pass

dispatch_handlers = {}

def dispatcher(message_class):
  def decorator(method):
    def wrapper(self, message):
      method(self, message)

    if message_class not in dispatch_handlers:
      dispatch_handlers[message_class] = []
    dispatch_handlers[message_class].append(wrapper)

    return wrapper
  return decorator

class Peer:
  def __init__(self, torrent, peer_info, panic_callback=None):
    self.panic_callback = panic_callback
    self.torrent = torrent
    if TEST_WITH_LOCAL_PEER:
      self.ip = '127.0.0.1'
      self.port = 6881
    else:
      self.ip = peer_info[b'ip'].decode('utf-8')
      self.port = peer_info[b'port']
    self.peer_id = peer_info[b'peer id']
    self.is_connected = False

    self.am_choking = True
    self.peer_choking = True
    self.am_interested = False
    self.peer_interested = False

    self.handshook = False
    self.received_non_handshake_message = False
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

    try:
      self.socket.connect((self.ip, self.port))
    except ConnectionRefusedError:
      self.panic('Connection refused')
      return
    except TimeoutError:
      self.panic('Connection timed out')
      return

    self.is_connected = True
    self._debug(f'Connected')
    self._send_handshake()
    self._make_interested(True)
    self._main_loop()

  def _main_loop(self):
    buffer = b''
    while True:
      # blocking
      try:
        buffer += self.socket.recv(4096)
      except (ConnectionResetError, ConnectionAbortedError, BrokenPipeError):
        self.panic('Connection with remote peer failed while receiving data')
        return

      # self._debug(f'Received {len(buffer)} bytes')
      if not buffer:
        continue
      while True:
        old_buffer_len = len(buffer)
        try:
          buffer = self._on_data(buffer)
          if not buffer:
            # we consumed everything -- no need to keep processing current buffer
            break
          if len(buffer) == old_buffer_len:
            # we consumed nothing -- no need to keep processing current buffer
            break
        except (struct.error, ValueError) as e:
          self._debug(f'Partial data received: {e}')

  def _on_data(self, buffer):
    if not self.handshook:
      # TODO: handle the case where no handshake is received first, but an error message is initially received
      try:
        handshake_message, buffer = HandshakeMessage.from_bytes(buffer)
        self._on_handshake(handshake_message)
      except struct.error:
        # Handshake does not yet have enough bytes to complete
        return buffer
      self.handshook = True
      self._debug(f'Handshake completed')
      return buffer
    # TODO: handle the case where only a partial message is received
    message = None
    try:
      # import pdb; pdb.set_trace()
      message, buffer = Message.from_buffer(buffer)
    except ValueError as e:
      self._warn(e)
      return buffer

    self._on_message(message)
    self.received_non_handshake_message = True
    return buffer

  def _on_message(self, message):
    self._debug(f'Received message of type {type(message).__name__}: {message}')

    for dispatcher in dispatch_handlers[type(message)]:
      dispatcher(self, message)

  @dispatcher(ChokeMessage)
  def _on_choke(self, _):
    self.peer_choking = True

  @dispatcher(UnchokeMessage)
  def _on_unchoke(self, _):
    self.peer_choking = False

    request_message = RequestMessage(index=0, begin=0, length=BLOCK_LENGTH)
    self._send_request(request_message)

  @dispatcher(InterestedMessage)
  def _on_interested(self, _):
    self.peer_interested = True

  @dispatcher(NotInterestedMessage)
  def _on_not_interested(self, _):
    self.peer_interested = False

  @dispatcher(HaveMessage)
  def _on_have(self, have_message):
    self._mark_has(have_message.data['piece_index'])

  def _ensure_piece_index_in_range(self, piece_index):
    if not 0 <= piece_index < self.torrent.num_pieces:
      # TODO: handle this gracefully
      self.panic(f'Invalid piece index: {piece_index}')
      return

  def _mark_has(self, piece_index):
    self._ensure_piece_index_in_range(piece_index)
    self.has.add(piece_index)

  @dispatcher(BitfieldMessage)
  def _on_bitfield(self, bitfield_message):
    if self.received_non_handshake_message:
      self.panic(f'Bitfield message was not received immediately after handshake')
      return

    if bitfield_message.num_pieces != ceil(self.torrent.num_pieces / 8) * 8:
      self.panic(f'Invalid bitfield length. bitfield message num_pieces: {bitfield_message.num_pieces}, torrent num_pieces: {self.torrent.num_pieces}')
      return

    for piece in bitfield_message.pieces:
      self._mark_has(piece)
    # self._debug(f'Peer has pieces: {self.has}')

  @dispatcher(RequestMessage)
  def _on_request(self, request_message):
    self._ensure_piece_index_in_range(request_message.data['index'])
    # TODO: respond to request

  @dispatcher(PieceMessage)
  def _on_piece(self, piece_message):
    self._ensure_piece_index_in_range(piece_message.data['index'])
    # TODO: handle piece

  @dispatcher(CancelMessage)
  def _on_cancel(self, cancel_message):
    self._ensure_piece_index_in_range(cancel_message.data['index'])

  @dispatcher(PortMessage)
  def _on_port(self, port_message):
    pass

  @dispatcher(KeepAliveMessage)
  def _on_keep_alive(self, keep_alive_message):
    pass

  @dispatcher(HandshakeMessage)
  def _on_handshake(self, handshake_message):
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

  def panic(self, reason):
    self.socket.close()
    self._warn(f'Peer panic: {reason}')
    self.is_connected = False
    if self.panic_callback is not None:
      self.panic_callback(reason)

  def __str__(self):
    return f'Peer {self._identifier()}'
