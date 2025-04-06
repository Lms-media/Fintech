from QUIK.QuikPy import QuikPy

class Robot:
    def __init__(self, clientCode: str, accountId: str, classCode: str, tickerCode: str):
        self._provider = QuikPy()
        self._clientCode = clientCode
        self._account = accountId
        self._classCode = classCode
        self._tickerCode = tickerCode
        
    def close_connection(self):
        self._provider.close_connection_and_thread()
        
    def get_candles(self, interval: int = 1, count: int = 5):
        return self._provider.get_candles_from_data_source(
            class_code=self._classCode,
            sec_code=self._tickerCode,
            interval=interval,
            count=count
        )