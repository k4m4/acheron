import socket
import logging
import abc

class Connection(metaclass=abc.ABCMeta):
  def __init__(self, ip, port):
    self.socket = None
    self.is_connected = False
    self.ip = ip
    self.port = port

  def connect(self):
    try:
      socket.inet_pton(socket.AF_INET6, self.ip)
      protocol = socket.AF_INET6
    except socket.error:
      try:
        socket.inet_pton(socket.AF_INET, self.ip)
        protocol = socket.AF_INET
      except socket.error:
        self._warning(f'Invalid IP address: {self.ip}')
        return

    self.socket = socket.socket(protocol, socket.SOCK_STREAM)

    self._debug(f'Connecting to {self.ip}:{self.port}')

    try:
      self.socket.connect((self.ip, self.port))
    except ConnectionRefusedError:
      self.panic('Connection refused')
      return
    except TimeoutError:
      self.panic('Connection timed out')
      return

    self.is_connected = True
    self.on_connect()

  def main_loop(self):
    buffer = b''
    while True:
      # blocking
      try:
        buffer += self.socket.recv(4096)
      except (ConnectionResetError, ConnectionAbortedError, BrokenPipeError):
        self.panic('Connection with remote peer failed while receiving data')
        return

      # self._debug(f'Received {len(buffer)} bytes')
      if not buffer:
        continue
      while True:
        old_buffer_len = len(buffer)
        try:
          buffer = self.on_data(buffer)
          if not buffer:
            # we consumed everything -- no need to keep processing current buffer
            break
          if len(buffer) == old_buffer_len:
            # we consumed nothing -- no need to keep processing current buffer
            break
        except ValueError as e:
          pass

  def panic(self, reason):
    self.socket.close()
    self._warning(f'Peer panic: {reason}')
    self.is_connected = False
    if self.on_panic:
      self.on_panic(reason)

  def _identifier(self):
    return f'{self.ip}:{self.port}'

  def _warning(self, msg):
    logging.warning(f'[{self._identifier()}] {msg}')

  def _debug(self, msg):
    logging.debug(f'[{self._identifier()}] {msg}')

  def _info(self, msg):
    logging.info(f'[{self._identifier()}] {msg}')

  @abc.abstractmethod
  def on_connect(self):
    pass

  @abc.abstractmethod
  def on_panic(self, reason):
    pass

  @abc.abstractmethod
  def on_data(self, buffer):
    pass
