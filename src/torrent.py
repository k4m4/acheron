import bencodepy
from pathlib import Path
from tracker import Tracker
from pprint import pprint
from peer import Peer
import logging
from hashlib import sha1
from math import ceil
from peer_manager import PeerManager
import json
import os

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
    self.data_file = None
    self.meta_file = None

    self._init_from_metadata(bencoded_metadata)
    self._init_data_file()
    self._init_meta_file()
    self.client = client
    self.tracker = Tracker(self)
    self.have = set()

    logging.debug(f'Found {len(self.tracker.peers_info)} peers:')

    self.peer_manager = PeerManager(self, self.tracker.peers_info, self._on_piece_download)
    self.peer_manager.connect()

  def _init_data_file(self):
    self.data_file = "downloads/ubuntu.iso"
    os.makedirs(os.path.dirname(self.data_file), exist_ok=True)
    Path(self.data_file).touch()

  def _init_meta_file(self):
    self.meta_file = "downloads/ubuntu.metadata"
    os.makedirs(os.path.dirname(self.meta_file), exist_ok=True)
    Path(self.meta_file).touch()

  def _on_piece_download(self, index, data):
    self._write_piece_to_disk(index, data)
    self.have.add(index)
    self._write_meta_file()

  def _write_piece_to_disk(self, index, data):
    with open(self.data_file, 'wb') as f:
      f.seek(index * self.piece_length)
      f.write(data)
  
  def _write_meta_file(self):
    with open(self.meta_file, 'w') as f:
      f.write(json.dumps({
        'have': list(self.have)
      }))
  
  def _read_meta_file(self):
    with open(self.meta_file, 'r') as f:
      # TODO: handle parse/read error
      self.have = set(json.loads(f.read())['have'])

  def _read_piece_from_disk(self, index):
    assert 0 <= index < self.num_pieces
    assert index in self.have
    with open(self.data_file, 'rb') as f:
      f.seek(index * self.piece_length)
      return f.read(self.piece_length)

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
