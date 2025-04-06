from QUIK.QuikPy import QuikPy

class Robot:
    def __init__(self, clientCode: str, accountId: str, classCode: str, ticketCode: str):
        self._provider = QuikPy()
        self._clientCode = clientCode
        self.account = accountId
        self.classCode = classCode
        self.ticketCode = ticketCode
        
    def __del__(self):
        self._provider.close_connection_and_thread()