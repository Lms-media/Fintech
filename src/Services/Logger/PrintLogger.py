from Interfaces import ILogger

class PrintLogger(ILogger):

    def init(self):
        pass

    def log(self, line: str):
        print(line)
