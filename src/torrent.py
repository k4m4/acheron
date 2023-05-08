import bencodepy
from tracker import Tracker
from pprint import pprint
from peer import Peer
import logging
from hashlib import sha1
from math import ceil
from peer_manager import PeerManager

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
    self.pieces = None

    self._init_from_metadata(bencoded_metadata)
    self.client = client
    self.tracker = Tracker(self)
    self.have = set()

    logging.debug(f'Found {len(self.tracker.peers_info)} peers:')

    self.peer_manager = PeerManager(self, self.tracker.peers_info)
    self.peer_manager.connect()

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
