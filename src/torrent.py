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

class Torrent:
  def __init__(self, client, bencoded_metadata):
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

    self.storage = Storage(data_file="downloads/ubuntu.iso", meta_file="downloads/ubuntu.metadata")
    self.have = self.storage.read_meta_file()

    # TODO: store data returned from tracker to meta file, in case tracker becomes unavailable
    self._init_from_metadata(bencoded_metadata)

    logging.info(f'We have already downloaded {len(self.have) / self.num_pieces * 100:.2f}% of the torrent')

    self.want = set(range(self.num_pieces)) - self.have
    self.pending = set()

    self.client = client

    try:
      self.tracker = Tracker(self)
    except Exception as e:
      logging.error(f'Failed to initialize tracker: {e}')
      sys.exit(1)

    logging.debug(f'Found {len(self.tracker.peers_info)} peers:')

    self.peer_manager = PeerManager(self, self.tracker.peers_info)
    self.peer_manager.on('piece_downloaded', self.on_piece_downloaded)
    self.peer_manager.connect()

  def on_piece_downloading(self, piece_index):
    self.want.remove(piece_index)
    self.pending.add(piece_index)

  def on_piece_downloaded(self, index, data):
    self.storage.write_piece(self.piece_length, index, data)
    self.have.add(index)
    # TODO: handle receiving a piece that was not pending
    self.pending.remove(index)
    logging.info(f'Download progress: {len(self.have) / self.num_pieces * 100:.2f}%')
    self.storage.write_meta_file(self.have)

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
    self.comment = decoded[b'comment']
    self.created_by = decoded[b'created by']
    self.creation_date = decoded[b'creation date']
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
      self.files = info[b'files']
      # TODO: handle multifile mode
    else: # single file mode
      self.length = info[b'length']
      self.name = info[b'name']
      self.piece_length = info[b'piece length']
