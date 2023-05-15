import socket
import logging
import abc
import asyncio

OPEN_CONNECTION_TIMEOUT = 1 # second

class Connection(metaclass=abc.ABCMeta):
  def __init__(self, ip, port):
    self.reader = None
    self.writer = None
    self.is_connecting = False
    self.is_connected = False
    self.is_processing = False
    self.ip = ip
    self.port = port

  @staticmethod
  def validate_ip(ip):
    try:
      socket.inet_pton(socket.AF_INET6, ip)
      # protocol = socket.AF_INET6
    except socket.error:
      try:
        socket.inet_pton(socket.AF_INET, ip)
        # protocol = socket.AF_INET
      except socket.error:
        return False
    return True

  async def connect(self):
    if self.is_connecting:
      raise ConnectionError('The peer is already trying to connect')
    if self.is_connected:
      raise ConnectionError('The peer is already connected')
    self.is_connecting = True

    if not self.validate_ip(self.ip):
      self._warning(f'Invalid IP address: {self.ip}')
      return

    self._debug(f'Connecting to {self.ip}:{self.port}')

    try:
      self.reader, self.writer = await asyncio.wait_for(asyncio.open_connection(self.ip, self.port), timeout=OPEN_CONNECTION_TIMEOUT)
    except (
      ConnectionResetError,
      ConnectionAbortedError,
      BrokenPipeError,
      asyncio.CancelledError,
      TimeoutError,
      OSError
    ) as e:
      await self.panic(f'Failed to establish a connection: {e}')
      return

    self.is_connected = True
    self.is_connecting = False

    await self.on_connect()

  # Under normal circumstances, this function never returns
  async def main_loop(self):
    if self.is_processing:
      raise ConnectionError('The peer is already processing data')
    if not self.is_connected:
      raise ConnectionError('The peer is not connected')
    self.is_processing = True

    buffer = b''
    while True:
      try:
        new_buffer = await self.reader.read(4096)
        if not new_buffer:
          await self.panic('Read an empty buffer')
          return

        buffer += new_buffer
      except (
        ConnectionResetError,
        ConnectionAbortedError,
        BrokenPipeError,
        asyncio.CancelledError,
        TimeoutError,
        OSError
      ) as e:
        await self.panic(f'Connection with remote peer failed while receiving data: {e}')
        self.is_processing = False
        return

      # self._debug(f'Received {len(buffer)} bytes')
      if not buffer:
        continue
      while True:
        old_buffer_len = len(buffer)
        try:
          buffer = await self.on_data(buffer)
          if not buffer:
            # we consumed everything -- no need to keep processing current buffer
            break
          if len(buffer) == old_buffer_len:
            # we consumed nothing -- no need to keep processing current buffer
            break
        except ValueError as e:
          pass

  async def send_data(self, data):
    try:
      self.writer.write(data)
      await self.writer.drain()
    except (
      ConnectionResetError,
      ConnectionAbortedError,
      BrokenPipeError,
      asyncio.CancelledError,
      TimeoutError,
      OSError
    ) as e:
      await self.panic(f'Connection with remote peer failed while sending data: {e}')

  async def close(self):
    if not self.writer:
      return

    self.writer.close()
    await self.writer.wait_closed()

  async def panic(self, reason):
    await self.close()
    self._warning(f'Peer panic: {reason}')
    self.is_connected = False
    self.is_connecting = False
    self.is_processing = False
    await self.on_panic(reason)

  def _identifier(self):
    return f'{self.ip}:{self.port}'

  def _warning(self, msg):
    logging.warning(f'[{self._identifier()}] {msg}')

  def _debug(self, msg):
    logging.debug(f'[{self._identifier()}] {msg}')

  def _info(self, msg):
    logging.info(f'[{self._identifier()}] {msg}')

  async def on_panic(self, reason):
    pass

  async def on_connect(self):
    pass

  async def on_data(self, buffer):
    pass
