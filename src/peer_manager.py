import logging
from random import shuffle
from peer import Peer

MAX_ACTIVE_CONNECTIONS = 1

class PeerManager:
  def __init__(self, torrent, peers_info, on_piece_download=None):
    self.torrent = torrent
    self.peers = set()

    for i, peer_info in enumerate(peers_info):
      logging.debug(f'Peer {i}: {peer_info}')

      this = self

      class PeerPanicHandler:
        def __init__(self):
          self.peer = None

        def update_peer(self, peer):
          self.peer = peer

        def handle_panic(self, reason):
          this.peers.remove(self.peer)
          this.connect_to_new_peer()

      panic_handler = PeerPanicHandler()

      peer = Peer(torrent, peer_info, panic_handler.handle_panic, on_piece_download)
      panic_handler.update_peer(peer)
      self.peers.add(peer)

  def connect_to_new_peer(self):
    prioritized_peers = list(self.peers)
    shuffle(prioritized_peers)

    for peer in prioritized_peers:
      if not peer.is_connected:
        peer.connect()
        return True
    return False

  def connect(self):
    for _ in range(MAX_ACTIVE_CONNECTIONS):
      has_connected = self.connect_to_new_peer()
      if not has_connected:
        break
