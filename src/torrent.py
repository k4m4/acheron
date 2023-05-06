import bencodepy
from tracker import Tracker
from pprint import pprint
from peer import Peer
import logging

class Torrent:
  def __init__(self, client, bencoded_metadata):
    self.__init_from_metadata(bencoded_metadata)
    self.client = client
    self.tracker = Tracker(self)

    logging.debug(f'Found {len(self.tracker.peers_info)} peers')

    peers = []
    for peer_info in self.tracker.peers_info:
      peer = Peer(self, peer_info)
      peer.connect()
      peers.append(peer)

  def __init_from_metadata(self, bencoded_metadata):
    decoded = bencodepy.decode(bencoded_metadata)
    self.announce_url = decoded[b'announce']
    self.comment = decoded[b'comment']
    self.created_by = decoded[b'created by']
    self.creation_date = decoded[b'creation date']
    info = decoded[b'info']
    self.info_value = bencodepy.encode(info)
    if b'files' in info: # multifile mode
      self.files = info[b'files']
      # TODO: handle multifile mode
    else: # single file mode
      self.length = info[b'length']
      self.name = info[b'name']
      self.piece_length = info[b'piece length']
      self.pieces = info[b'pieces']
