import logging
from random import shuffle
from peer import Peer

MAX_ACTIVE_CONNECTIONS = 1

def capture(*args1, **kwargs1):
  def decorator(fn):
    def wrapper(*args2, **kwargs2):
      return fn(*[*args1, *args2], **{**kwargs1, **kwargs2})
    return wrapper
  return decorator

class PeerManager:
  def __init__(self, torrent, peers_info, on_piece_download=None):
    self.torrent = torrent
    self.peers = set()

    for i, peer_info in enumerate(peers_info):
      logging.debug(f'Peer {i}: {peer_info}')

      this = self

      peer = Peer(torrent, peer_info)

      @capture(peer=peer)
      def on_panic(peer, reason):
        this.peers.remove(peer)
        this.connect_to_new_peer()

      @capture(peer=peer)
      def on_available(peer):
        logging.debug(f'Peer {peer} is available')

      peer.on('panic', on_panic)
      peer.on('piece_download', on_piece_download)
      peer.on('available', on_available)

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
