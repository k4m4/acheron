from torrent import Torrent
from secrets import token_bytes
import logging
import warnings
import argparse
import sys
from exception import ExecutionCompleted

LISTEN_PORT = 6881
CLIENT_ID = b'-AH0001-'
VERSION = '0.1.0'
CLIENT_NAME = 'Acheron'
DESCRIPTION='A BitTorrent client'

# TODO: listen

class Client:
  def __init__(self, torrent_file):
    logging.basicConfig(level=logging.INFO)

    logging.info(f'{CLIENT_NAME} {VERSION} - {DESCRIPTION}')

    self.peer_id = CLIENT_ID + token_bytes(20 - len(CLIENT_ID))
    self.key = token_bytes(4).hex()
    self.listen_port = LISTEN_PORT

    with open(torrent_file, 'rb') as f:
      metadata = f.read()
      try:
        torrent = Torrent(self, metadata)
      except ExecutionCompleted as e:
        # Terminate program because execution completed successfully
        logging.info(f'Execution completed: {e}')
        sys.exit(0)

def main():
  parser = argparse.ArgumentParser(
    prog='acheron',
    description=DESCRIPTION,
    epilog=f'{CLIENT_NAME} {VERSION} - {DESCRIPTION}'
  )
  parser.add_argument('-v', '--version', action='version', version='%(prog)s {VERSION}')
  parser.add_argument('torrent_file', help='path to .torrent file', type=str)
  args = parser.parse_args()
  warnings.filterwarnings("error", category=RuntimeWarning)
  client = Client(torrent_file=args.torrent_file)

if __name__ == '__main__':
  main()
