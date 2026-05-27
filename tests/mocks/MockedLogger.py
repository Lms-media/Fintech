from Interfaces import ILogger

class MockedLogger(ILogger):
    def __init__(self):
        self.messages = []
        self.inited = False

    def init(self):
        self.inited = True

    def log(self, chunk):
        self.messages.append(chunk)
