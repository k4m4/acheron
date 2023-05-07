import struct
from pprint import pprint

# TODO: use a proper buffer (https://docs.python.org/3/c-api/buffer.html#bufferobjects)
class Message:
  pass

class HandshakeMessage(Message):
  def __init__(self, protocol_string, info_hash, peer_id):
    self.protocol_string = protocol_string
    self.info_hash = info_hash
    self.peer_id = peer_id

  def to_bytes(self):
    pstrlen = len(self.protocol_string)
    reserved = 8 * b'\x00'

    packed = struct.pack(
      f'!B{pstrlen}s{len(reserved)}s{len(self.info_hash)}s{len(self.peer_id)}s',
      pstrlen,
      self.protocol_string,
      reserved,
      self.info_hash,
      self.peer_id
    )

    return packed

  def __str__(self):
    return f'HandshakeMessage(protocol_string={self.protocol_string}, info_hash={self.info_hash}, peer_id={self.peer_id})'

  @staticmethod
  def from_bytes(buffer):
    pstrlen, = struct.unpack_from('!B', buffer)
    unpack_format = f'!{pstrlen}s8s20s20s'
    protocol_string, reserved, info_hash, peer_id = struct.unpack_from(unpack_format, buffer, offset=1)
    consumed_byte_cnt = 1 + struct.calcsize(unpack_format)

    return HandshakeMessage(protocol_string, info_hash, peer_id), buffer[consumed_byte_cnt:]
