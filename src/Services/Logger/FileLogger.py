import os
from Interfaces import ILogger

class FileLogger(ILogger):
    _filename: str

    def __init__(self, filename: str):
        self._filename = filename

    def init(self):
        if os.path.exists(self._filename):
            os.remove(self._filename)
        open(self._filename, 'w').close()

    def log(self, chunk: str):
        with open(self._filename, 'a') as f:
            for line in chunk.split('\n'):
                f.write(line + '\n')
