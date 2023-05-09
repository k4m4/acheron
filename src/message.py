import struct
from pprint import pprint
import abc
import logging

# TODO: use a proper buffer (https://docs.python.org/3/c-api/buffer.html#bufferobjects)
class Message(abc.ABC):
  payload_struct = []
  message_id = None

  def __init__(self, **kwargs):
    assert len(kwargs) == len(self.payload_struct)
    keys = [k for k, _ in self.payload_struct]
    assert set(kwargs.keys()) == set(keys)
    self.data = kwargs

  def to_bytes(self):
    payload_bytes = self._payload_to_bytes()
    payload_length = len(payload_bytes)
    if self.message_id is None:
      return struct.pack('!I', payload_length) + payload_bytes
    return struct.pack('!IB', payload_length + 1, self.message_id) + payload_bytes

  @staticmethod
  def from_buffer(buffer):
    length_prefix, = struct.unpack('!I', buffer[:4])
    if length_prefix == 0:
      message = KeepAliveMessage()
      buffer = buffer[4:]
      return message, buffer

    message_id, = struct.unpack('!B', buffer[4:4 + 1])

    # TODO: refactor using decorators
    # TODO: check that the length correctly corresponds to the message id
    try:
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
    except IndexError:
      raise ValueError(f'Unknown message id {message_id}')
    return message, buffer

  @classmethod
  def _payload_struct_format(cls, num_var_bytes=0):
    struct_format = '!'
    for k, v in cls.payload_struct:
      if v == 'Xs':
        struct_format += f'{num_var_bytes}s'
      else:
        struct_format += v
    return struct_format

  @classmethod
  def _payload_num_const_bytes(cls):
    return struct.calcsize(cls._payload_struct_format())

  @classmethod
  def _payload_num_var_bytes(cls, len_message_buffer):
    return len_message_buffer - cls._payload_num_const_bytes()

  @classmethod
  def from_bytes(cls, buffer):
    payload_length, message_id = struct.unpack_from('!IB', buffer)
    assert message_id == cls.message_id
    if payload_length - 1 > len(buffer):
      raise ValueError(f'Not enough bytes to read {cls.__name__} payload')

    message_buffer = buffer[4 + 1:4 + 1 + (payload_length - 1)]
    buffer = buffer[4 + 1 + (payload_length - 1):]
    data = cls._payload_from_bytes(message_buffer)
    return cls(**data), buffer

  @classmethod
  def _payload_from_bytes(cls, message_buffer):
    num_var_bytes = cls._payload_num_var_bytes(len(message_buffer))

    data = struct.unpack_from(cls._payload_struct_format(num_var_bytes), message_buffer)
    message_buffer[struct.calcsize(cls._payload_struct_format()):]

    kwargs = {}
    for v, (k, _) in zip(data, cls.payload_struct):
      kwargs[k] = v
    return kwargs

  def _payload_to_bytes(self):
    num_var_bytes = 0
    for k, v in self.payload_struct:
      if v == 'Xs':
        num_var_bytes = len(self.data[k])

    format = self._payload_struct_format(num_var_bytes)
    return struct.pack(format, *self.data.values())

  def __str__(self):
    params = ', '.join(f'{k}={str(v)[:50] + "..." if len(str(v)) > 50 else v}' for k, v in self.data.items())
    return f"{type(self).__name__}({params})"

class HandshakeMessage(Message):
  payload_struct = [
    ('protocol_string', 's'),
    ('info_hash', '20s'),
    ('peer_id', '20s')
  ]

  def to_bytes(self):
    pstrlen = len(self.data['protocol_string'])
    reserved = 8 * b'\x00'

    packed = struct.pack(
      f'!B{pstrlen}s{len(reserved)}s{len(self.data["info_hash"])}s{len(self.data["peer_id"])}s',
      pstrlen,
      self.data['protocol_string'],
      reserved,
      self.data['info_hash'],
      self.data['peer_id']
    )

    return packed

  @staticmethod
  def from_bytes(buffer):
    pstrlen, = struct.unpack_from('!B', buffer)
    unpack_format = f'!{pstrlen}s8s20s20s'
    protocol_string, reserved, info_hash, peer_id = struct.unpack_from(unpack_format, buffer, offset=1)
    consumed_byte_cnt = 1 + struct.calcsize(unpack_format)

    return HandshakeMessage(
      protocol_string=protocol_string,
      info_hash=info_hash,
      peer_id=peer_id
    ), buffer[consumed_byte_cnt:]

class KeepAliveMessage(Message):
  pass

class ChokeMessage(Message):
  message_id = 0

class UnchokeMessage(Message):
  message_id = 1

class InterestedMessage(Message):
  message_id = 2

class NotInterestedMessage(Message):
  message_id = 3

class HaveMessage(Message):
  message_id = 4
  payload_struct = [
    ('piece_index', 'I')
  ]

class BitfieldMessage(Message):
  message_id = 5
  payload_struct = [
    ('bitfield', 'Xs')
  ]

  @classmethod
  def from_pieces(cls, pieces, num_pieces):
    return BitfieldMessage(bitfield=cls._pieces_to_bitfield(pieces, num_pieces))

  @classmethod
  def _pieces_to_bitfield(self, pieces, num_pieces):
    num_bytes = num_pieces // 8
    if num_pieces % 8 != 0:
      num_bytes += 1

    bitfield = num_bytes * [0]
    for piece in pieces:
      i = piece // 8
      j = piece % 8
      bitfield[i] |= 1 << (7 - j)

    return bytes(bitfield)

  def __init__(self, bitfield):
    super().__init__(bitfield=bitfield)
    self.num_pieces = len(bitfield) * 8
    self.pieces = set()
    for i, byte in enumerate(bitfield):
      for j in range(8):
        if byte & (1 << (7 - j)):
          self.pieces.add(i * 8 + j)

    assert len(self.pieces) <= self.num_pieces

class RequestMessage(Message):
  message_id = 6
  payload_struct = [
    ('index', 'I'),
    ('begin', 'I'),
    ('length', 'I')
  ]

class PieceMessage(Message):
  message_id = 7
  payload_struct = [
    ('index', 'I'),
    ('begin', 'I'),
    ('block', 'Xs')
  ]

class CancelMessage(Message):
  message_id = 8
  payload_struct = [
    ('index', 'I'),
    ('begin', 'I'),
    ('length', 'I')
  ]

class PortMessage(Message):
  message_id = 9
  payload_struct = [
    ('listen_port', 'H')
  ]
