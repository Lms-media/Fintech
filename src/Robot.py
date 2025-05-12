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

class TestRobot:
    def __init__(self):
        # Тестовый режим
        self._testMode = False
        self._testWallet = 0.0  # Виртуальный баланс
        self._testPosition = 0  # Количество активов
        self._testHistory = []  # История операций
        self._testCurrentIndex = 0  # Текущий индекс в тестовых данных
        self._testData = []  # Массив исторических свечей для теста

    def enableTestMode(self, initialBalance: float = 100000.0):
        """Активировать тестовый режим с начальным балансом"""
        self._testMode = True
        self._testWallet = initialBalance
        self._testPosition = 0
        self._testHistory = []

    def disableTestMode(self):
        """Деактивировать тестовый режим"""
        self._testMode = False

    def loadTestData(self, data: list[dict]):
        """Загрузить исторические данные для тестирования"""
        self._testData = sorted(data, key=lambda x: x['time'])
        self._testCurrentIndex = 0

    def setTestDate(self, dateStr: str):
        """Установить текущую дату в тестовом режиме"""
        if not self._testMode:
            print("Test mode is not activated")
            return

        for i, candle in enumerate(self._testData):
            if candle['time'] >= dateStr:
                self._testCurrentIndex = i
                print(f"Date set: {candle['time']}")
                return

        print("The specified date was not found in the test data.")

    def getTestBalance(self) -> dict:
        """Получить текущий баланс в тестовом режиме"""
        return {
            'wallet': self._testWallet,
            'position': self._testPosition,
            'current_price': self._testData[self._testCurrentIndex]['close'] if self._testData else 0,
            'total_value': self._testWallet + self._testPosition * (
                self._testData[self._testCurrentIndex]['close'] if self._testData else 0
            )
        }

    def processTestStep(self, quantity: int):
        """Обработать один шаг тестирования (купить/продать по текущей цене)"""
        if not self._testMode or not self._testData:
            print("Test mode is not activated or data is not loaded")
            return

        if self._testCurrentIndex >= len(self._testData):
            print("End of test data reached")
            return

        currentCandle = self._testData[self._testCurrentIndex]
        price = currentCandle['close']

        if quantity == 0:
            return

        operation = "BUY" if quantity > 0 else "SELL"
        absQuantity = abs(quantity)
        cost = absQuantity * price

        if operation == "BUY":
            if cost > self._testWallet:
                print("Insufficient funds to purchase")
                return
            self._testWallet -= cost
            self._testPosition += absQuantity
        else:
            if absQuantity > self._testPosition:
                print("Not enough assets to sell")
                return
            self._testWallet += cost
            self._testPosition -= absQuantity

        # Записываем операцию в историю
        self._testHistory.append({
            'time': currentCandle['time'],
            'operation': operation,
            'quantity': absQuantity,
            'price': price,
            'wallet_after': self._testWallet,
            'position_after': self._testPosition
        })

        self._testCurrentIndex += 1

    def runTestToDate(self, endDate: str, decisionFunc: Callable[[dict], int]):
        """Запустить тестирование до указанной даты, используя функцию принятия решений"""
        if not self._testMode or not self._testData:
            print("Test mode is not activated or data is not loaded")
            return

        while self._testCurrentIndex < len(self._testData):
            currentCandle = self._testData[self._testCurrentIndex]

            if currentCandle['time'] > endDate:
                break

            quantity = decisionFunc(currentCandle)
            self.processTestStep(quantity)

    def getTestResults(self) -> dict:
        """Получить результаты тестирования"""
        if not self._testData:
            return {}

        finalPrice = self._testData[-1]['close']

        initialValue = self._testWallet  # Начальный баланс
        finalValue = self._testWallet + self._testPosition * finalPrice

        profit = finalValue - initialValue
        profitPercent = (profit / initialValue) * 100 if initialValue != 0 else 0

        return {
            'initial_balance': initialValue,
            'final_balance': finalValue,
            'profit': profit,
            'profit_percent': profitPercent,
            'final_position': self._testPosition,
            'final_price': finalPrice,
            'operations_count': len(self._testHistory),
            'operations': self._testHistory
        }