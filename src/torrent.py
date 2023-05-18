import bencodepy
from pathlib import Path
from tracker import Tracker
from pprint import pprint
from peer import Peer
import logging
from hashlib import sha1
from math import ceil
from peer_manager import PeerManager
import sys
from storage import Storage
from time import time
import asyncio
from exceptions import ExecutionCompleted

DOWNLOAD_SPEED_ESTIMATE_WINDOW = 100

class Torrent:
  def __init__(
    self,
    client,
    bencoded_metadata,
    max_active_connections,
    max_downloading_from,
    max_uploading_to,
    download_directory,
    remote_ip,
    remote_port
  ):
    self.announce_url = None
    self.comment = None
    self.created_by = None
    self.creation_date = None
    self.info_value = None
    self.info_hash = None
    self.num_piece = None
    self.files = None
    self.length = None
    self.name = None
    self.piece_length = None
    self.start_time = time()
    self.event_loop = asyncio.get_event_loop()
    self.single_peer_mode = remote_ip and remote_port

    # TODO: store data returned from tracker to meta file, in case tracker becomes unavailable
    self._init_from_metadata(bencoded_metadata)
    self.storage = Storage(download_directory, self.name, self.info_hash.hex())

    self.have = self.storage.read_meta_file()

    downloaded_percentage = len(self.have) / self.num_pieces * 100
    if downloaded_percentage > 0:
      logging.info(f'We have already downloaded {downloaded_percentage:.2f}% of the torrent')

    self.want = set(range(self.num_pieces)) - self.have

    num_pieces_left = len(self.want)
    if num_pieces_left == 0:
      logging.info('We already have all the pieces')
      logging.info('Seeding...')
      max_downloading_from = 0
    else:
      logging.info(f'We still need to download {num_pieces_left} piece{"s" if num_pieces_left != 1 else ""}')

    self.pending = set()
    self.recent_pieces_downloaded = [] # for estimating download speed

    self.client = client

    # try:
    self.tracker = Tracker(self)
    # except Exception as e:
    #   logging.error(f'Failed to initialize tracker: {e}')
    #   sys.exit(1)

    logging.debug(f'Found {len(self.tracker.peers_info)} peers:')

    peers_info = self.tracker.peers_info
    if self.single_peer_mode:
      peer_info = {
        'ip': remote_ip,
        'port': remote_port,
        'peer id': None
      }

      peers_info = [peer_info]

    self.peer_manager = PeerManager(
      self,
      peers_info,
      max_active_connections,
      max_downloading_from,
      max_uploading_to
    )
    self.peer_manager.on('piece_downloaded', self.on_piece_downloaded)

    async def handle_new_peer(reader, writer):
      ip, port = writer.get_extra_info('peername')
      peer_info = {
        'ip': ip,
        'port': port,
        'peer id': None
      }
      peer = Peer(self, peer_info)
      peer.reader = reader
      peer.writer = writer
      peer.is_connected = True
      peer.ip = ip
      peer.port = port
      self.peer_manager.handle_new_peer(peer)
      await peer.on_connect()

    self.event_loop.create_task(asyncio.start_server(handle_new_peer, '0.0.0.0', self.client.listen_port))
    self.event_loop.create_task(self.peer_manager.connect())
    self.event_loop.run_forever()

  def on_piece_downloading(self, piece_index):
    # Discard, because we might be in end game
    self.want.discard(piece_index)
    # TODO: pending must timeout at some point
    self.pending.add(piece_index)

  def download_speed(self): # bytes per second
    recent_timestamp = self.recent_pieces_downloaded[0]['timestamp']
    recent_amount = sum([piece['amount'] for piece in self.recent_pieces_downloaded])
    download_speed = recent_amount / (time() - recent_timestamp)
    return download_speed

  def human_download_speed(self):
    download_speed = self.download_speed()
    if len(self.recent_pieces_downloaded) <= 1:
      return 'Unknown'
    return f'{download_speed / 1024:.2f} KB/s'

  @staticmethod
  def seconds_to_human(secs):
    if secs > 100:
      mins = secs / 60
      if mins > 100:
        hours = mins / 60
        return f'{hours:.2f} hours'
      return f'{mins:.2f} minutes'
    return f'{secs:.2f} seconds'

  def human_eta(self):
    if len(self.recent_pieces_downloaded) <= 1:
      return 'Unknown'
    download_speed = self.download_speed()
    bytes_downloaded = len(self.have) * self.piece_length
    secs = (self.length - bytes_downloaded) / download_speed
    if secs < 0:
      # TODO: account for last piece being smaller
      secs = 0

    return self.seconds_to_human(secs)

  async def on_piece_downloaded(self, index, data):
    # TODO: In end game, cancel this piece from other peers
    self.recent_pieces_downloaded.append({
      'index': index,
      'amount': len(data),
      'timestamp': time()
    })
    if len(self.recent_pieces_downloaded) > DOWNLOAD_SPEED_ESTIMATE_WINDOW:
      self.recent_pieces_downloaded = self.recent_pieces_downloaded[-DOWNLOAD_SPEED_ESTIMATE_WINDOW:]

    logging.info(f'Download speed: {self.human_download_speed()}')

    self.storage.write_piece(self.piece_length, index, data)
    self.have.add(index)
    # TODO: handle receiving a piece that was not pending
    self.pending.remove(index)
    logging.info(f'Download progress: {len(self.have) / self.num_pieces * 100:.2f}% ({len(self.have)}/{self.num_pieces})')
    self.storage.write_meta_file(self.have)
    logging.info(f'ETA: {self.human_eta()}')
    logging.info(f'Connected peers: {len(self.peer_manager.connected_peers)}.'\
                  + f'\tDownloading from: {len(self.peer_manager.downloading_from)}.'\
                  + f'\tUploading to: {len(self.peer_manager.uploading_to)}')

    if len(self.have) == self.num_pieces:
      logging.info('Download completed')
      logging.info(f'Data saved to {self.storage.data_file}')
      download_duration = self.seconds_to_human(time() - self.start_time)
      logging.info(f'Download took: {download_duration}')
      # TODO: seed here
      logging.info('Shutting down')
      self.event_loop.stop()

  def read_piece(self, index):
    assert 0 <= index < self.num_pieces
    assert index in self.have
    return self.storage.read_piece(self.piece_length, index)

  def get_piece_hash(self, index):
    return self.piece_hashes[index]

  def _init_from_metadata(self, bencoded_metadata):
    logging.debug('Parsing torrent metadata')

    decoded = bencodepy.decode(bencoded_metadata)
    self.announce_url = decoded[b'announce']
    self.comment = decoded.get(b'comment' )
    self.created_by = decoded.get(b'created by')
    self.creation_date = decoded.get(b'creation date')
    info = decoded[b'info']
    self.info_value = bencodepy.encode(info)
    hashes_str = info[b'pieces']
    if len(hashes_str) % 20 != 0:
      # TODO: gracefully handle this
      # TODO: custom exception here for file format errors
      raise Exception('Invalid pieces length')
    self.num_pieces = len(hashes_str) // 20
    piece_length = info[b'piece length']
    # TODO: handle this gracefully
    assert self.num_pieces == ceil(info[b'length'] / piece_length)
    self.piece_hashes = []
    for i in range(self.num_pieces):
      self.piece_hashes.append(hashes_str[i*20:(i+1)*20])
    self.info_hash = sha1(self.info_value).digest()

    logging.debug(f'Info hash is {self.info_hash.hex()}')

    if b'files' in info: # multifile mode
      self.info(f'Downloading file: {info[b"files"]}')
      self.files = info[b'files']
      raise NotImplemented('Multifile mode is not supported')
      # TODO: handle multifile mode
      # TODO: fill in self.length
    else: # single file mode
      self.length = info[b'length']
      self.name = info[b'name']
      self.piece_length = info[b'piece length']
