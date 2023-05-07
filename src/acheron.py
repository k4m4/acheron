from torrent import Torrent
from secrets import token_bytes
from pprint import pprint
import logging

LISTEN_PORT = 6881
CLIENT_ID = b'-AH0001-'

# TODO: listen

class Client:
  def __init__(self):
    logging.basicConfig(level=logging.DEBUG)

    logging.info('Acheron v0.0.1 - A torrent client')

    self.peer_id = CLIENT_ID + token_bytes(20 - len(CLIENT_ID))
    self.listen_port = LISTEN_PORT

    with open('fixtures/ubuntu-23.04-desktop-amd64.iso.torrent', 'rb') as f:
      metadata = f.read()
      torrent = Torrent(self, metadata)

client = Client()
