from math import ceil
from hashlib import sha1
import logging
from event_emitter import EventEmitter

class Piece(EventEmitter):
  def __init__(self, peer, index, size, hash, block_length):
    EventEmitter.__init__(self)
    self.peer = peer
    self.index = index
    self.size = size
    self.num_blocks = ceil(size / block_length)
    self.hash = hash
    self.block_length = block_length
    self.data = bytearray(size)
    self.blocks_received = set()

  async def on_block_arrival(self, begin, block):
    self.data[begin:begin+len(block)] = block
    block_index = begin // self.block_length
    if len(block) > self.block_length:
      logging.warning(f'Piece {self.index} received block of length {len(block)} > {self.block_length} beginning at {begin}')
      await self.emit('block_error', 'Block too long')
      return
    if block_index < self.num_blocks - 1:
      if len(block) != self.block_length:
        logging.warning(f'Piece {self.index} received block of length {len(block)} != {self.block_length} beginning at {begin}')
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
