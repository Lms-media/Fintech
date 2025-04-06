from QUIK.QuikPy import QuikPy

class Robot:
    def __init__(self, clientCode: str, accountId: str, classCode: str, tickerCode: str):
        self._provider = QuikPy()
        self._clientCode = clientCode
        self._account = accountId
        self._classCode = classCode
        self._tickerCode = tickerCode
        
        self._provider.on_new_candle = self._newCandleHandler
        
    def closeConnection(self):
        self._provider.close_connection_and_thread()
        
    def getCandles(self, interval: int = 1, count: int = 5):
        return self._provider.get_candles_from_data_source(
            class_code=self._classCode,
            sec_code=self._tickerCode,
            interval=interval,
            count=count
        )
        
    def subscribe(self, interval: int = 1):
        self._provider.subscribe_to_candles(
            class_code=self._classCode,
            sec_code=self._tickerCode,
            interval=interval
        )
    
    def _newCandleHandler(self, data: dict):
        candle = data["data"]
        datetime = candle["datetime"]
        print(f"{datetime['hour']}:{datetime['min']}:{datetime['sec']} Open: {candle['open']}; Close: {candle['close']}; High: {candle['high']}; Low: {candle['low']}; Volume: {candle['volume']}")