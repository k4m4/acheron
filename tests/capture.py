import unittest
from src.capture import capture
import logging
import asyncio

logging.basicConfig(level=logging.DEBUG)

class TestCapture(unittest.TestCase):
  def test_capture(self):
    @capture(5, 7)
    async def f(a, b, c):
      self.assertEqual(a, 5)
      self.assertEqual(b, 7)
      self.assertEqual(c, 15)

    asyncio.run(f(c=15))
    asyncio.run(f(15))

if __name__ == '__main__':
  unittest.main()
