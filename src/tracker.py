# TODO: implement our own HTTP library
import requests
from pprint import pprint
import logging
import bencodepy

class Tracker:
  def __init__(self, torrent):
    self.torrent = torrent
    self.__request()

  def __request(self):
    params = {
      'info_hash': self.torrent.info_hash,
      'peer_id': self.torrent.client.peer_id,
      'port': self.torrent.client.listen_port,
      'uploaded': 0,
      'downloaded': 0,
      'left': self.torrent.length,
      'compact': 0
    }
    logging.info(f'Requesting from tracker {self.torrent.announce_url}')
    r = requests.get(self.torrent.announce_url, params=params)
    if r.status_code != 200:
      # TODO: gracefully handle error
      logging.error(f'Error requesting from tracker: {r.status_code}')
      logging.error(r.content)
      return
    logging.debug('Received tracker response')
    decoded = bencodepy.decode(r.content)
    self.parse_tracker_response(decoded)

  def parse_tracker_response(self, response):
    self.interval = response[b'interval']
    self.seeders = response[b'complete']
    self.leechers = response[b'incomplete']

    logging.debug(f'Seeders: {self.seeders}')
    logging.debug(f'Leechers: {self.leechers}')

    self.peers_info = response[b'peers']
