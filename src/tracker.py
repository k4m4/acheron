# TODO: implement our own HTTP library
import requests
import logging
import bencodepy
import socket
import struct

# TODO: re-request data from tracker periodically
# and ensure delays/timeouts are respected, and 'event' is reported
# (generally, make sure tracker is informed of our intentions)
class Tracker:
  def __init__(self, torrent):
    self.torrent = torrent
    self._request()

  def _request(self):
    params = {
      'info_hash': self.torrent.info_hash,
      'peer_id': self.torrent.client.peer_id,
      'port': self.torrent.client.listen_port,
      'uploaded': 0,
      'downloaded': 0,
      'left': self.torrent.length,
      'numwant': len(self.torrent.want),
      'key': self.torrent.client.key,
      'compact': 1,
      'event': 'started'
    }
    logging.info(f'Requesting from tracker {self.torrent.announce_url}')
    r = requests.get(self.torrent.announce_url, params=params)
    if r.status_code != 200:
      # TODO: gracefully handle error
      logging.error(f'Error requesting from tracker: {r.status_code}')
      logging.error(r.content)
      raise Exception(f'Error requesting from tracker: {r.status_code}')
    logging.debug('Received tracker response')
    decoded = bencodepy.decode(r.content)
    self.parse_tracker_response(decoded)

  def parse_tracker_response(self, response):
    # TODO: Use a default interval
    self.interval = response.get(b'interval')
    self.seeders = response.get(b'complete')
    self.leechers = response.get(b'incomplete')

    logging.debug(f'Seeders: {self.seeders}')
    logging.debug(f'Leechers: {self.leechers}')

    if type(response[b'peers']) == list:
      # dictionary model (non-compact response)
      self.peers_info = [
        {
          'ip': peer[b'ip'].decode('utf-8'),
          'port': peer[b'port'],
          'peer id': peer[b'peer id']
        }
        for peer in response[b'peers']
      ]
    else:
      # binary model (compact response)
      assert type(response[b'peers']) == bytes
      assert len(response[b'peers']) % 6 == 0
      self.peers_info = []
      for i in range(0, len(response[b'peers']), 6):
        ip = socket.inet_ntoa(response[b'peers'][i:i+4])
        port, = struct.unpack('!H', response[b'peers'][i+4:i+6])
        self.peers_info.append({
          'ip': ip,
          'port': port,
          'peer id': None
        })

    logging.debug(f'Peers: {self.peers_info}')
