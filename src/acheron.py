from torrent import Torrent
from secrets import token_bytes
import logging
import warnings
import argparse
import sys
from exceptions import ExecutionCompleted

LOG_LEVEL = 'debug'
LISTEN_PORT = 6881
CLIENT_ID = b'-AH0001-'
VERSION = '0.1.0'
CLIENT_NAME = 'Acheron'
DESCRIPTION = 'A BitTorrent client'

DEFAULT_MAX_ACTIVE_CONNECTIONS = 30
DEFAULT_MAX_DOWNLOADING_FROM = 20
DEFAULT_MAX_UPLOADING_TO = 20

# TODO: listen

class Client:
  def __init__(
    self,
    torrent_file,
    max_active_connections,
    max_downloading_from,
    max_uploading_to
  ):
    logging.info(f'{CLIENT_NAME} {VERSION} - {DESCRIPTION}')

    self.peer_id = CLIENT_ID + token_bytes(20 - len(CLIENT_ID))
    self.key = token_bytes(4).hex()
    self.listen_port = LISTEN_PORT

    with open(torrent_file, 'rb') as f:
      metadata = f.read()
      try:
        torrent = Torrent(
          self,
          metadata,
          max_active_connections,
          max_downloading_from,
          max_uploading_to
        )
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
  parser.add_argument('--max-active-connections', help='maximum number of active connections', type=int, default=DEFAULT_MAX_ACTIVE_CONNECTIONS)
  parser.add_argument('--max-downloading-from', help='maximum number of peers to download from', type=int, default=DEFAULT_MAX_DOWNLOADING_FROM)
  parser.add_argument('--max-uploading-to', help='maximum number of peers to upload to', type=int, default=DEFAULT_MAX_UPLOADING_TO)
  parser.add_argument('--log', help='log level (debug, info, warning)', choices=['debug', 'info', 'warn'], default='info')

  args = parser.parse_args()

  log_level = {
    'debug': logging.DEBUG,
    'info': logging.INFO,
    'warn': logging.WARNING
  }[args.log]

  logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=log_level,
    datefmt='%Y-%m-%d %H:%M:%S'
  )

  warnings.filterwarnings("error", category=RuntimeWarning)
  client = Client(
    torrent_file=args.torrent_file,
    max_active_connections=args.max_active_connections,
    max_downloading_from=args.max_downloading_from,
    max_uploading_to=args.max_uploading_to
  )

if __name__ == '__main__':
  main()
