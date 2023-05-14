import logging
from random import shuffle
from peer import Peer
from event_emitter import EventEmitter
from message import HaveMessage
import asyncio

# TODO: adjust these limits
MAX_ACTIVE_CONNECTIONS = 1
MAX_DOWNLOADING_FROM = 1
MAX_UPLOADING_TO = 1

def capture(*args1, **kwargs1):
  def decorator(fn):
    def wrapper(*args2, **kwargs2):
      return fn(*[*args1, *args2], **{**kwargs1, **kwargs2})
    return wrapper
  return decorator

class PeerManager(EventEmitter):
  def __init__(self, torrent, peers_info):
    EventEmitter.__init__(self)

    self.torrent = torrent
    self.peers = set()
    self.downloading_from = set()
    self.uploading_to = set()

    for i, peer_info in enumerate(peers_info):
      logging.debug(f'Peer {i}: {peer_info}')

      this = self

      peer = Peer(torrent, peer_info)

      @capture(peer=peer)
      async def on_panic(peer, reason):
        this.peers.remove(peer)
        await this.connect_to_new_peer()

      @capture(peer=peer)
      async def on_available(peer):
        logging.debug(f'{peer} is available')
        matching_pieces = self.torrent.want & peer.has
        if matching_pieces:
          # piece_to_request = matching_pieces.pop()
          piece_to_request = max(matching_pieces)
          self.torrent.on_piece_downloading(piece_to_request)
          await peer.schedule_piece_download(piece_to_request)
        else:
          logging.debug(f'No matching pieces between what we want and what {peer} has')

      @capture(peer=peer)
      async def on_connect(peer):
        # TODO: we are not always interested
        logging.debug(f'Connection event for {peer}')
        await peer.make_interested(True)
        await peer.main_loop()

      async def on_piece_downloaded(piece_index, data):
        await self.emit('piece_downloaded', piece_index, data)
        await self.broadcast(HaveMessage(piece_index=piece_index))

      peer.on('panic', on_panic)
      peer.on('piece_downloaded', on_piece_downloaded)
      peer.on('available', on_available)
      peer.on('connect', on_connect)

      self.peers.add(peer)

  async def broadcast(self, message):
    for peer in self.peers:
      if peer.is_connected:
        await peer.send(message)

  async def connect_to_new_peer(self):
    prioritized_peers = list(self.peers)
    shuffle(prioritized_peers)

    for peer in prioritized_peers:
      if not peer.is_connected and not peer.is_connecting:
        await peer.connect()
        return True
    return False

  async def connect(self):
    peer_tasks = []
    for _ in range(MAX_ACTIVE_CONNECTIONS):
      peer_tasks.append(self.connect_to_new_peer())
      # has_connected = await self.connect_to_new_peer()
      # if not has_connected:
      #   break
    await asyncio.gather(*peer_tasks)
