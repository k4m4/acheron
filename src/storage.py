import json
import os
from pathlib import Path

class Storage:
  def __init__(self, data_file, meta_file):
    self.data_file = data_file
    self.meta_file = meta_file

    os.makedirs(os.path.dirname(self.data_file), exist_ok=True)
    Path(self.data_file).touch()

    os.makedirs(os.path.dirname(self.meta_file), exist_ok=True)
    if not os.path.exists(self.meta_file):
      self.write_meta_file(set())

  def read_piece(self, piece_length, index):
    with open(self.data_file, 'rb') as f:
      f.seek(index * piece_length)
      return f.read(piece_length)

  def write_piece(self, piece_length, index, data):
    with open(self.data_file, 'wb') as f:
      f.seek(index * piece_length)
      f.write(data)

  def write_meta_file(self, have):
    with open(self.meta_file, 'w') as f:
      f.write(json.dumps({
        'have': list(have)
      }))

  def read_meta_file(self):
    with open(self.meta_file, 'r') as f:
      # TODO: handle parse/read error
      return set(json.loads(f.read())['have'])
