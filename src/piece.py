from math import ceil
from hashlib import sha1
import logging
from event_emitter import EventEmitter

class Piece(EventEmitter):
  def __init__(self, peer, index, usual_piece_length, hash, block_length):
    EventEmitter.__init__(self)
    self.peer = peer
    self.index = index
    assert 0 <= index < self.peer.torrent.num_pieces
    self.length = self.expected_length(self.peer.torrent.length, usual_piece_length, index)
    self.num_blocks = ceil(usual_piece_length / block_length)
    self.hash = hash
    self.block_length = block_length
    self.data = bytearray(self.length)
    self.blocks_received = set()

  @staticmethod
  def expected_length(torrent_length, usual_piece_length, piece_index):
    num_pieces = ceil(torrent_length / usual_piece_length)

    assert piece_index < num_pieces

    if piece_index < num_pieces - 1:
      return usual_piece_length

    return (torrent_length - 1) % usual_piece_length + 1

  async def on_block_arrival(self, begin, data):
    self.data[begin:begin+len(data)] = data
    block_index = begin // self.block_length
    if len(data) != Block.expected_length(self.length, block_index, self.block_length):
      logging.warning(f'{self} received block of length {len(data)} != {self.length} beginning at {begin}')
      await self.emit('block_error', block_index, 'Block size mismatch')
      return
    self.blocks_received.add(block_index)
    await self._check_completed()

  async def _check_completed(self):
    if len(self.blocks_received) == self.num_blocks:
      if sha1(self.data).digest() != self.hash:
        await self.emit('piece_error', 'Hash mismatch')
        return
      logging.debug(f'Piece {self.index} completed with hash {self.hash.hex()}')
      await self.emit('completed', self.data)

class Block:
  @staticmethod
  def expected_length(actual_piece_length, block_index, usual_block_length):
    num_blocks = ceil(actual_piece_length / usual_block_length)

    if block_index < num_blocks - 1:
      return usual_block_length

    last_block_length = (actual_piece_length - 1) % usual_block_length + 1

    return last_block_length
