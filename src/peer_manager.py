import logging
from random import shuffle
from peer import Peer
from event_emitter import EventEmitter
from message import HaveMessage
from capture import capture
import asyncio
from collections import deque

class PeerManager(EventEmitter):
  def __init__(
    self,
    torrent,
    peers_info,
    max_active_connections,
    max_downloading_from,
    max_uploading_to
  ):
    EventEmitter.__init__(self)

    logging.info('Initializing peer manager with:')
    logging.info(f'  max_active_connections: {max_active_connections}')
    logging.info(f'  max_downloading_from: {max_downloading_from}')
    logging.info(f'  max_uploading_to: {max_uploading_to}')

    self.torrent = torrent
    self.connected_peers = set()
    self.downloading_from = set()
    self.uploading_to = set()

    self.max_active_connections = max_active_connections
    self.max_downloading_from = max_downloading_from
    self.max_uploading_to = max_uploading_to

    self.end_game = False

    peers = []
    for i, peer_info in enumerate(peers_info):
      logging.debug(f'Peer {i}: {peer_info}')

      peer = Peer(torrent, peer_info)

      @capture(peer)
      async def on_panic(peer, reason):
        logging.warn(f'[{peer}] on_panic: {reason}')
        self.connected_peers.discard(peer)
        self.downloading_from.discard(peer)
        self.uploading_to.discard(peer)
        assert not peer.is_connecting and not peer.is_connected
        # Re-initialize peer to clean up any state
        self.candidate_peers.appendleft(Peer(torrent, peer.peer_info))
        await self.connect_to_new_peer()
        await self.find_peer_to_download_from()
        await self.find_peer_to_upload_to()

      @capture(peer)
      async def on_available(peer):
        logging.debug(f'{peer} is available')
        if not peer.am_interested:
          logging.debug(f'{peer} unchoked us even though we were not interested')
          return
        if self.torrent.want:
          want = self.torrent.want
        else:
          # end game
          if not self.end_game:
            self.end_game = True
            logging.info('Entering end game mode')
          want = self.torrent.pending
        matching_pieces = want & peer.has
        if matching_pieces:
          piece_to_request = matching_pieces.pop()
          self.torrent.on_piece_downloading(piece_to_request)
          await peer.schedule_piece_download(piece_to_request)
        else:
          await peer.make_interested(False)
          self.downloading_from.discard(peer)
          await self.find_peer_to_download_from()
          logging.debug(f'No matching pieces between what we want and what {peer} has')

      @capture(peer)
      async def on_connect(peer):
        logging.info(f'Connected to: {peer}')
        assert peer.is_connected and not peer.is_connecting
        self.connected_peers.add(peer)
        await self.find_peer_to_download_from()
        await self.find_peer_to_upload_to()
        await peer.main_loop()

      @capture(peer)
      async def on_not_interested(peer):
        self.uploading_to.discard(peer)
        await self.find_peer_to_upload_to()

      @capture(peer)
      async def on_interested(peer):
        await self.find_peer_to_upload_to()

      async def on_piece_downloaded(piece_index, data):
        had = piece_index in self.torrent.have
        if not had:
          await self.emit('piece_downloaded', piece_index, data)
          await self.broadcast(HaveMessage(piece_index=piece_index))

      peer.on('panic', on_panic)
      peer.on('piece_downloaded', on_piece_downloaded)
      peer.on('available', on_available)
      peer.on('connect', on_connect)
      peer.on('interested', on_interested)
      peer.on('not_interested', on_not_interested)

      peers.append(peer)

    shuffle(peers)
    self.candidate_peers = deque(peers)

  async def find_peer_to_download_from(self):
    if len(self.downloading_from) >= self.max_downloading_from:
      return

    found = False
    for peer in self.connected_peers.copy():
      assert peer.is_connected
      if peer in self.downloading_from:
        continue
      if not peer.has & self.torrent.want:
        continue
      found = True
      self.downloading_from.add(peer)
      await peer.make_interested(True)
      if len(self.downloading_from) >= self.max_downloading_from:
        break
    if not found:
      logging.debug('No peers to download from')

    logging.debug(f'Currently downloading from {len(self.downloading_from)} peers')

  async def find_peer_to_upload_to(self):
    if len(self.uploading_to) >= self.max_uploading_to:
      return

    new_peer_count = self.max_uploading_to - len(self.uploading_to)

    for peer in list(self.downloading_from) + list(self.connected_peers):
      assert peer.is_connected
      if peer in self.uploading_to:
        continue
      if not peer.peer_interested:
        continue
      self.uploading_to.add(peer)
      await peer.make_choking(False)
      if len(self.uploading_to) >= self.max_uploading_to:
        break

    logging.debug(f'Currently uploading to {len(self.uploading_to)} peers')

  async def broadcast(self, message):
    for peer in self.connected_peers.copy():
      assert peer.is_connected
      await peer.send(message)

  async def connect_to_new_peer(self):
    logging.info(f'Number of candidate peers left: {len(self.candidate_peers)}')
    if not self.candidate_peers:
      logging.warn('Exhausted candidate peers')
      # TODO: handle this case
      return False

    peer = self.candidate_peers.pop()
    assert not peer.is_connected and not peer.is_connecting
    logging.info(f'Connecting to {peer}')
    await peer.connect()
    return True

  async def connect(self):
    peer_tasks = []
    for _ in range(self.max_active_connections):
      peer_tasks.append(self.connect_to_new_peer())
    await asyncio.gather(*peer_tasks)
