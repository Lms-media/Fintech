import os
import matplotlib.pyplot as plt
from Interfaces import ILogger

class GraphLogger2D(ILogger):
    _filename: str
    _xs: dict[str, list[float]]
    _ys: dict[str, list[float]]

    def __init__(self, filename: str):
        self._filename = filename
        self._xs = dict()
        self._ys = dict()

    def init(self):
        if os.path.exists(self._filename):
            os.remove(self._filename)
        open(self._filename, 'w').close()

    def log(self, chunk: str):
        parts = chunk.split(';')

        if not len(parts) == 2:
            raise ValueError("Incorrect chunk format")

        xParts = parts[0].split(':')
        yParts = parts[1].split(':')

        if not len(xParts) == 2 or not len(yParts) == 2:
            raise ValueError("Incorrect chunk format")

        xName, xValue = xParts
        yName, yValue = yParts

        if not xName == 'x':
            raise ValueError("Incorrect chunk format")

        if yName in self._ys:
            self._xs[yName].append(float(xValue))
            self._ys[yName].append(float(yValue))
        else:
            self._xs[yName] = [float(xValue)]
            self._ys[yName] = [float(yValue)]

        plt.clf()
        for y in self._ys:
            plt.plot(self._xs[y], self._ys[y], label=y)
        plt.legend()

        plt.savefig(self._filename, dpi=300)
