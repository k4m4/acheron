from math import ceil
from hashlib import sha1
import logging

class Piece:
  def __init__(self, peer, index, size, hash, block_length, on_completed=None, on_error=None):
    self.peer = peer
    self.index = index
    self.size = size
    self.num_blocks = ceil(size / block_length)
    self.hash = hash
    self.block_length = block_length
    self.data = bytearray(size)
    self.blocks_received = set()
    self.on_completed = on_completed
    self.on_error = on_error

  def on_block_arrival(self, begin, block):
    self.data[begin:begin+len(block)] = block
    block_index = begin // self.block_length
    assert len(block) <= self.block_length
    if block_index < self.num_blocks - 1:
      assert len(block) == self.block_length
    self.blocks_received.add(block_index)
    self._check_completed()

  def _check_completed(self):
    if len(self.blocks_received) == self.num_blocks:
      if sha1(self.data).digest() != self.hash:
        if self.on_error:
          self.on_error('Hash mismatch')
          return
      if self.on_completed:
        logging.debug(f'Piece {self.index} completed with hash {self.hash.hex()}')
        self.on_completed()
