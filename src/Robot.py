import time
from datetime import datetime

from QUIK.QuikPy import QuikPy
from typing import Callable
from src.utils.chunkSize import getChunkSize
from src.utils.fetchMoex import fetchMoex

class Robot:
    def __init__(self, clientCode: str, accountId: str, classCode: str, tickerCode: str, contentType: str):
        self._provider = None
        self._clientCode = clientCode
        self._account = accountId
        self._classCode = classCode
        self._tickerCode = tickerCode
        self._contentType = contentType
        self._subscriptions = dict()

    def connectToQuik(self):
        self._provider = QuikPy()
        self._provider.on_new_candle = self._newCandleHandler

    def closeConnection(self):
        if self._provider is not  None:
            self._provider.close_connection_and_thread()
        else: print("Not connected to QUIK")

    def getLastPrice(self) -> str:
        if self._provider is None:
            print("Not connected to QUIK")
            return '0'

        return self._provider.get_param_ex(self._classCode, self._tickerCode, "PREVPRICE")['data']["param_value"]

    def getCandles(self, interval: int = 1, count: int = 5):
        if self._provider is None:
            print("Not connected to QUIK")
            return

        return self._provider.get_candles_from_data_source(
            class_code=self._classCode,
            sec_code=self._tickerCode,
            interval=interval,
            count=count
        )
        
    def subscribe(self, callback: Callable[[dict], int], interval: int = 1):
        if self._provider is None:
            print("Not connected to QUIK")
            return

        self._provider.subscribe_to_candles(
            class_code=self._classCode,
            sec_code=self._tickerCode,
            interval=interval
        )
        self._subscriptions[interval] = callback
    
    def subscribeHistorical(self, callback: Callable[[dict], None], start: int, end: int, interval: int = 1):
        chunkSize = getChunkSize(interval)
        
        while start < end:
            chunk = fetchMoex(self._tickerCode, start, min(start + chunkSize, end), interval, self._contentType)
            for candle in chunk:
                callback(candle)
            start += chunkSize
        
    def createOrder(self, quantity: int):
        if self._provider is None:
            print("Not connected to QUIK")
            return

        if quantity == 0:
            return
        operation = "B"
        if quantity < 0:
            operation = "S"
            quantity = -quantity
        price = self.getLastPrice()
        transaction = {
            'TRANS_ID': str(int(time.time())),
            'CLIENT_CODE': self._clientCode,
            'ACCOUNT': self._account,
            'ACTION': 'NEW_ORDER',
            'CLASSCODE': self._classCode,
            'SECCODE': self._tickerCode,
            'OPERATION': operation,
            'PRICE': price[0:price.index('.')] if self._contentType == "currency" else '0',
            'QUANTITY': str(quantity),
            'TYPE': 'L' if self._contentType == "currency" else 'M'
        }
        self._provider.send_transaction(transaction)
    
    def _newCandleHandler(self, data: dict):
        interval = data["data"]["interval"]
        if interval == 0:
            return
        dt = data["data"]["datetime"]
        args = {
            "open": data["data"]["open"],
            "close": data["data"]["close"],
            "high": data["data"]["high"],
            "low": data["data"]["low"],
            "volume": data["data"]["volume"],
            "time": datetime(dt["year"], dt["month"], dt["day"], dt["hour"], dt["min"], dt["sec"]).strftime("%Y-%m-%d %H:%M:%S")
        }
        quantity = self._subscriptions[interval](args)
        self.createOrder(quantity)
    
    