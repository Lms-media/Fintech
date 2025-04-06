import time
from QUIK.QuikPy import QuikPy
from typing import Callable

class Robot:
    def __init__(self, clientCode: str, accountId: str, classCode: str, tickerCode: str):
        self._provider = QuikPy()
        self._clientCode = clientCode
        self._account = accountId
        self._classCode = classCode
        self._tickerCode = tickerCode
        
        self._subscriptions = dict()
        
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
        
    def subscribe(self, callback: Callable[[dict], int], interval: int = 1):
        self._provider.subscribe_to_candles(
            class_code=self._classCode,
            sec_code=self._tickerCode,
            interval=interval
        )
        self._subscriptions[interval] = callback
        
    def createOrder(self, quantity: int):
        if quantity == 0:
            return
        operation = "B"
        if quantity < 0:
            operation = "S"
            quantity = -quantity
        transaction = {
            'TRANS_ID': str(int(time.time())),
            'CLIENT_CODE': self._clientCode,
            'ACCOUNT': self._account,
            'ACTION': 'NEW_ORDER',
            'CLASSCODE': self._classCode,
            'SECCODE': self._tickerCode,
            'OPERATION': operation,
            'PRICE': "0",
            'QUANTITY': str(quantity),
            'TYPE': 'M'
        }
        self._provider.send_transaction(transaction)
        
    
    def _newCandleHandler(self, data: dict):
        interval = data["data"]["interval"]
        if interval == 0:
            return
        quantity = self._subscriptions[interval](data)
        self.createOrder(quantity)
        