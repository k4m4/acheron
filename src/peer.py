import logging
import struct
from version import peer_id_to_human_peer_id
from pprint import pprint
from message import *
from math import ceil
from piece import Block, Piece
from connection import Connection
from event_emitter import EventEmitter

PROTOCOL_STRING = b'BitTorrent protocol'
TEST_WITH_LOCAL_PEER = False
BLOCK_LENGTH = 16 * 1024

NUM_PARALLEL_PIECE_REQUESTS_PER_PEER = 1

class ProtocolException(Exception):
  pass

dispatch_handlers = {}

def dispatcher(message_class):
  def decorator(method):
    async def wrapper(self, message):
      await method(self, message)

    if message_class not in dispatch_handlers:
      dispatch_handlers[message_class] = []
    dispatch_handlers[message_class].append(wrapper)

    return wrapper
  return decorator

class Peer(Connection, EventEmitter):
  def __init__(self, torrent, peer_info):
    EventEmitter.__init__(self)
    self.torrent = torrent

    assert torrent.piece_length % BLOCK_LENGTH == 0

    if TEST_WITH_LOCAL_PEER:
      ip = '127.0.0.1'
      port = 6881
    else:
      ip = peer_info['ip']
      port = peer_info['port']
    self.peer_id = peer_info['peer id']
    Connection.__init__(self, ip, port)

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

  async def on_connect(self):
    self._debug(f'Connected')
    await self._send_handshake()
    await self._send_bitfield()
    await self.emit('connect')

  async def on_data(self, buffer):
    if not self.handshook:
      try:
        handshake_message, buffer = HandshakeMessage.from_bytes(buffer)
        await self._on_handshake(handshake_message)
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

    await self._on_message(message)
    self.received_non_handshake_message = True
    return buffer

  async def _on_message(self, message):
    self._debug(f'<- {message}')

    for dispatcher in dispatch_handlers[type(message)]:
      await dispatcher(self, message)

  @dispatcher(ChokeMessage)
  async def _on_choke(self, _):
    self.peer_choking = True

  @dispatcher(UnchokeMessage)
  async def _on_unchoke(self, _):
    self.peer_choking = False
    await self.emit('available')

  @dispatcher(InterestedMessage)
  async def _on_interested(self, _):
    self.peer_interested = True

  @dispatcher(NotInterestedMessage)
  async def _on_not_interested(self, _):
    self.peer_interested = False

  @dispatcher(HaveMessage)
  async def _on_have(self, have_message):
    self._mark_has(have_message.data['piece_index'])

  def _ensure_piece_index_in_range(self, piece_index):
    if not 0 <= piece_index < self.torrent.num_pieces:
      self.panic(f'Invalid piece index: {piece_index}')
      return

  def _mark_has(self, piece_index):
    self._ensure_piece_index_in_range(piece_index)
    self.has.add(piece_index)

  @dispatcher(BitfieldMessage)
  async def _on_bitfield(self, bitfield_message):
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
  async def _on_request(self, request_message):
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

    data = self.torrent.read_piece(index)[begin:begin+length]
    await self.send(PieceMessage(index=index, begin=begin, block=data))

  # This message represents the data of a single block within the piece,
  # not a whole piece
  @dispatcher(PieceMessage)
  async def _on_piece(self, piece_message):
    self._ensure_piece_index_in_range(piece_message.data['index'])

    piece_index = piece_message.data['index']

    piece = self.pending_pieces[piece_index]
    await piece.on_block_arrival(piece_message.data['begin'], piece_message.data['block'])

  async def schedule_piece_download(self, piece_index):
    assert piece_index not in self.pending_pieces

    piece = Piece(
      self,
      piece_index,
      self.torrent.piece_length,
      self.torrent.get_piece_hash(piece_index),
      BLOCK_LENGTH
    )
    self.pending_pieces[piece_index] = piece

    async def on_completed(piece_data):
      self._debug(f'Piece {piece_index} completed')
      del self.pending_pieces[piece_index]
      await self.emit('piece_downloaded', piece_index, piece_data)
      await self.emit('available')

    async def on_piece_error(reason):
      self._warning(f'Piece {piece_index} failed: {reason}')
      # TODO: don't re-request the piece; instead, disconnect from peer and inform PeerManager
      await self.request_piece(piece)

    async def on_block_error(block_index, reason):
      self._warning(f'Block of piece {piece_index} failed: {reason}; re-requesting block')
      # TODO: don't re-request the block; instead, disconnect from peer and inform PeerManager
      await self.request_block(piece, block_index)

    piece.on('completed', on_completed)
    piece.on('piece_error', on_piece_error)
    piece.on('block_error', on_block_error)

    await self.request_piece(piece)

    if len(self.pending_pieces) < NUM_PARALLEL_PIECE_REQUESTS_PER_PEER:
      await self.emit('available')

  async def request_block(self, piece, block_index):
    # self._debug(f'Requesting block {block_index} of piece {piece_index}')
    block_length = Block.expected_length(
      actual_piece_length=piece.length,
      block_index=block_index,
      usual_block_length=BLOCK_LENGTH
    )
    await self.send(
      RequestMessage(index=piece.index, begin=block_index*BLOCK_LENGTH, length=block_length)
    )

  async def request_piece(self, piece):
    # self._debug(f'Requesting piece {piece_index}')
    for block_index in range(piece.num_blocks):
      await self.request_block(piece, block_index)

  @dispatcher(CancelMessage)
  async def _on_cancel(self, cancel_message):
    self._ensure_piece_index_in_range(cancel_message.data['index'])

  @dispatcher(PortMessage)
  async def _on_port(self, port_message):
    pass

  @dispatcher(KeepAliveMessage)
  async def _on_keep_alive(self, keep_alive_message):
    pass

  @dispatcher(HandshakeMessage)
  async def _on_handshake(self, handshake_message):
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
          self._warning(f"{match['warn']}: expected {match['expected']}, got {match['actual']}")
    self.human_peer_id = peer_id_to_human_peer_id(handshake_message.data['peer_id'])

    # TODO: show reserved bits
    self._debug(f'Remote peer is running {self.human_peer_id}')

  async def _close_with_error(self, msg):
    self._warning(msg)
    await self.close()
    raise ProtocolException(msg)

  async def _send_handshake(self):
    self._debug(f'Sending handshake')
    handshake_message = HandshakeMessage(
      protocol_string=PROTOCOL_STRING,
      info_hash=self.torrent.info_hash,
      peer_id=self.torrent.client.peer_id
    )

    await self.send(handshake_message)

  async def _send_bitfield(self):
    self._debug(f'Sending bitfield')
    bitfield_message = BitfieldMessage.from_pieces(self.torrent.have, self.torrent.num_pieces)
    await self.send(bitfield_message)

  async def make_interested(self, am_interested=True):
    self._debug(f'Changing interested flag to {am_interested}')
    if am_interested == self.am_interested:
      return
    self.am_interested = am_interested
    if am_interested:
      await self.send(InterestedMessage())
    else:
      await self.send(NotInterestedMessage())

  async def _make_choking(self, am_choking=True):
    self._debug(f'Changing choking flag to {am_choking}')
    if am_choking == self.am_choking:
      return
    self.am_choking = am_choking
    if am_choking:
      await self.send(ChokeMessage())
    else:
      await self.send(UnchokeMessage())

  async def send(self, message):
    self._debug(f'-> {message}')
    await self.send_data(message.to_bytes())

  def on_panic(self, reason):
    self.on('panic', reason)

  def _identifier(self):
    return f'{self.human_peer_id if self.human_peer_id is not None else self.peer_id} ({self.ip})'

  def __str__(self):
    return f'Peer {self._identifier()}'
