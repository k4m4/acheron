import unittest
from src.message import RequestMessage, BitfieldMessage
from math import ceil
import logging

logging.basicConfig(level=logging.DEBUG)

class TestRequestMessage(unittest.TestCase):
  packed_request = b'\x00\x00\x00\x0d\x06\x00\x00\x00\x0a\x00\x00\x00\x14\x00\x00\x00\x1e'

  def test_to_bytes(self):
    message = RequestMessage(index=10, begin=20, length=30)
    self.assertEqual(message.to_bytes(), self.packed_request)

  def test_from_bytes(self):
    expected_message = RequestMessage(index=10, begin=20, length=30)
    actual_message, _ = RequestMessage.from_bytes(self.packed_request)
    self.assertEqual(actual_message.data, expected_message.data)

  def test_from_bytes_consumes_bytes(self):
    message1 = RequestMessage(index=10, begin=20, length=30)
    packed = message1.to_bytes()
    message2, remaining = RequestMessage.from_bytes(packed)
    self.assertEqual(remaining, b'')
    self.assertEqual(message2.data, message1.data)

class TestBitfieldMessage(unittest.TestCase):
  def test_to_bytes_and_from_bytes(self):
    # Test that the 'to_bytes' and 'from_bytes' methods produce consistent output
    expected_pieces = {1, 3, 5, 7, 9}
    expected_num_pieces = 10
    message = BitfieldMessage.from_pieces(expected_pieces, expected_num_pieces)
    packed_bytes = message.to_bytes()
    unpacked_message, remaining_bytes = BitfieldMessage.from_bytes(packed_bytes)
    self.assertEqual(unpacked_message.pieces, expected_pieces)
    self.assertEqual(unpacked_message.num_pieces, ceil(expected_num_pieces / 8) * 8)
    self.assertEqual(remaining_bytes, b'')

if __name__ == '__main__':
  unittest.main()
