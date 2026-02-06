import os
from Interfaces import ILogger

class FileLogger(ILogger):

    def __init__(self, filename: str):
        self._filename = filename

    def init(self):
        if os.path.exists(self._filename):
            os.remove(self._filename)
        open(self._filename, 'w').close()

    def log(self, chunk: str):
        with open(self._filename, 'a') as f:
            f.write(chunk + '\n')
