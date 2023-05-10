import logging
import struct
from version import peer_id_to_human_peer_id
from pprint import pprint
from message import *
from math import ceil
from piece import Piece
from connection import Connection

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

class Peer(Connection):
  def __init__(self, torrent, peer_info, panic_handler=None, piece_download_handler=None):
    self.panic_handler = panic_handler
    self.piece_download_handler = piece_download_handler
    self.torrent = torrent

    if TEST_WITH_LOCAL_PEER:
      ip = '127.0.0.1'
      port = 6881
    else:
      ip = peer_info['ip']
      port = peer_info['port']
    self.peer_id = peer_info['peer id']
    super().__init__(ip, port)

    self.am_choking = True
    self.peer_choking = True
    self.am_interested = False
    self.peer_interested = False

    self.handshook = False
    self.received_non_handshake_message = False
    self.human_peer_id = None
    self.has = set()

    self.pending_pieces = {} # piece index => Piece()

    self._debug(f'Creating peer')

  def on_connect(self):
    self._debug(f'Connected')
    self._send_handshake()
    self._make_interested(True)
    self._main_loop()

  def on_data(self, buffer):
    if not self.handshook:
      try:
        handshake_message, buffer = HandshakeMessage.from_bytes(buffer)
        self._on_handshake(handshake_message)
      except struct.error:
        # Handshake does not yet have enough bytes to complete
        return buffer
      self.handshook = True
      self._debug(f'Handshake completed')
      return buffer
    message = None
    try:
      message, buffer = Message.from_buffer(buffer)
    except (ValueError, struct.error) as e:
      # We may not have enough data to parse the full message yet
      # self._debug(e)
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
    self.request_piece(0)

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

    percentage_peer_has = round((len(self.has) / self.torrent.num_pieces) * 100)
    self._info(f'Peer has {percentage_peer_has}% of pieces')

  @dispatcher(RequestMessage)
  def _on_request(self, request_message):
    index = request_message.data['index']
    begin = request_message.data['begin']
    length = request_message.data['length']
    self._ensure_piece_index_in_range(index)

    if self.am_choking:
      self._debug(f'Peer requested piece while choked')
      return

    if not self.peer_interested:
      self._debug(f'Peer requested piece while not interested')
      return

    if index not in self.torrent.have:
      self._debug(f'Peer requested piece that we do not have')
      return

    if index < self.torrent.num_pieces - 1:
      current_piece_length = self.torrent.piece_length
    else:
      current_piece_length = self.torrent.length % self.torrent.piece_length
      if current_piece_length == 0:
        current_piece_length = self.torrent.piece_length

    if not begin + length <= current_piece_length:
      self._debug(f'Peer requested piece with invalid length')
      return

    data = self.torrent.read_piece_from_disk(index)[begin:begin+length]
    self._send(PieceMessage(index=index, begin=begin, block=data))

  @dispatcher(PieceMessage)
  def _on_piece(self, piece_message):
    self._ensure_piece_index_in_range(piece_message.data['index'])

    piece_index = piece_message.data['index']
    if piece_index not in self.pending_pieces:
      def on_completed(piece_data):
        self._debug(f'Piece {piece_index} completed')
        del self.pending_pieces[piece_index]
        if self.piece_download_handler:
          self.piece_download_handler(piece_index, piece_data)

      def on_error(reason):
        self.panic(f'Piece {piece_index} failed: {reason}')

      self.pending_pieces[piece_index] = Piece(
        self,
        piece_index,
        self.torrent.piece_length,
        self.torrent.get_piece_hash(piece_index),
        BLOCK_LENGTH,
        on_completed,
        on_error
      )
    piece = self.pending_pieces[piece_index]
    piece.on_block_arrival(piece_message.data['begin'], piece_message.data['block'])

  def request_piece(self, piece_index):
    self._ensure_piece_index_in_range(piece_index)

    for i in range(0, self.torrent.piece_length, BLOCK_LENGTH):
      self._send(RequestMessage(index=piece_index, begin=i, length=BLOCK_LENGTH))

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
          if self.peer_id is None:
            # Tracker compact mode means we don't know the peer_id yet
            continue
          # It is possible that the peer_id reported by the tracker
          # does not match the peer_id reported by the peer itself.
          # This is due to e.g., Azureus "anonymity" option
          # See: https://wiki.theory.org/BitTorrentSpecification#Handshake
          self._warn(f"{match['warn']}: expected {match['expected']}, got {match['actual']}")
    self.human_peer_id = peer_id_to_human_peer_id(handshake_message.data['peer_id'])

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

    self._send(handshake_message)

  def _make_interested(self, am_interested=True):
    self._debug(f'Changing interested flag to {am_interested}')
    if am_interested == self.am_interested:
      return
    self.am_interested = am_interested
    if am_interested:
      self._send(InterestedMessage())
    else:
      self._send(NotInterestedMessage())

  def _make_choking(self, am_choking=True):
    self._debug(f'Changing choking flag to {am_choked}')
    if am_choking == self.am_choking:
      return
    self.am_choking = am_choking
    if am_choked:
      self._send(ChokeMessage())
    else:
      self._send(UnchokeMessage())

  def _send(self, message):
    self._debug(f'Sending message of type {type(message).__name__}')
    self.socket.send(message.to_bytes())

  def on_panic(self, reason):
    if self.panic_handler:
      self.panic_handler(reason)

  def _identifier(self):
    return f'{self.human_peer_id if self.human_peer_id is not None else self.peer_id} ({self.ip})'

  def __str__(self):
    return f'Peer {self._identifier()}'
