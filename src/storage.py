import json
import os
from pathlib import Path
import re

DATA_DIR = 'downloads'

class Storage:
  def __init__(self, name, info_hash_hex):
    name = name.decode('utf-8')
    self.name = name
    self.info_hash_hex = info_hash_hex
    # TODO: handle multiple files with the same name
    # TODO: handle files with weird names
    # TODO: handle files that contain "/"
    assert re.fullmatch(r'[a-zA-Z0-9. _-]+', name)

    data_file = os.path.join(DATA_DIR, name)
    meta_file = os.path.join(DATA_DIR, f'{name}.meta')

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
    file_exists = os.path.exists(self.data_file)
    write_mode = 'r+b'
    if not file_exists:
      write_mode = 'wb'
    with open(self.data_file, write_mode) as f:
      print(f'Offset: {index * piece_length}')
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
