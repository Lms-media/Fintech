import os
import matplotlib.pyplot as plt
from Interfaces import ILogger

class GraphLogger2D(ILogger):
    _filename: str
    _x: list[float]
    _y: list[float]

    def __init__(self, filename: str):
        self._filename = filename
        self._x = list()
        self._y = list()

    def init(self):
        if os.path.exists(self._filename):
            os.remove(self._filename)
        open(self._filename, 'w').close()

    def log(self, chunk: str):
        parts = chunk.split(':')

        if not len(parts) == 2:
            raise ValueError("Incorrect chunk format")

        self._x.append(float(parts[0]))
        self._y.append(float(parts[1]))

        plt.plot(self._x, self._y)
        plt.xlabel("x")
        plt.ylabel("y")
        plt.savefig(self._filename)
