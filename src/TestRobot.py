from typing import Callable

class TestRobot:
    def __init__(self, initialBalance: int = 100000.0, initialPositions: int = 0, data: list[dict] = None, dateStr: str = ""):
        self._startWallet = initialBalance
        self._testWallet = initialBalance  # Виртуальный баланс
        self._testPosition = initialPositions  # Количество активов
        self._testHistory = []  # История операций
        self._testCurrentIndex = -1  # Текущий индекс в тестовых данных
        self._testData = sorted(data, key=lambda x: x['time'])  # Массив исторических свечей для теста

        for i, candle in enumerate(self._testData):
            if candle['time'] >= dateStr:
                self._testCurrentIndex = i
                print(f"Date set: {candle['time']}")
                break
        if self._testCurrentIndex == -1:
            self._testCurrentIndex = 0
            print("The specified date was not found in the test data.")

    def processTestStep(self, quantity: int):
        """Обработать один шаг тестирования (купить/продать по текущей цене)"""
        if self._testCurrentIndex >= len(self._testData):
            print("End of test data reached")
            return

        currentCandle = self._testData[self._testCurrentIndex]
        price = currentCandle['close']

        if quantity == 0:
            self._testCurrentIndex += 1
            return

        operation = "BUY" if quantity > 0 else "SELL"
        absQuantity = abs(quantity)
        cost = absQuantity * price

        if operation == "BUY":
            if cost > self._testWallet:
                print("Insufficient funds to purchase")
                self._testCurrentIndex += 1
                return
            self._testWallet -= cost
            self._testPosition += absQuantity
        else:
            if absQuantity > self._testPosition:
                print("Not enough assets to sell")
                self._testCurrentIndex += 1
                return
            self._testWallet += cost
            self._testPosition -= absQuantity

        # Записываем операцию в историю
        self._testHistory.append({
            'time': currentCandle['time'],
            'operation': operation,
            'quantity': absQuantity,
            'price': price,
            'wallet': self._testWallet,
            'position': self._testPosition,
            'current_price': self._testData[self._testCurrentIndex]['close'] if self._testData else 0,
            'total_value': self._testWallet + self._testPosition * (
                self._testData[self._testCurrentIndex]['close'] if self._testData else 0)
        })

        self._testCurrentIndex += 1

    def runTestToDate(self, endDate: str, decisionFunc: Callable[[dict], int]):
        """Запустить тестирование до указанной даты, используя функцию принятия решений"""
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

        balance = self._testWallet  # Начальный баланс
        totalBalance = self._testWallet + self._testPosition * finalPrice

        profit = totalBalance - self._startWallet
        profitPercent = (profit / self._startWallet) * 100 if self._startWallet != 0 else 0

        return {
            'balance': balance,
            'final_balance': totalBalance,
            'profit': profit,
            'profit_percent': profitPercent,
            'final_position': self._testPosition,
            'final_price': finalPrice,
            'operations_count': len(self._testHistory),
            'operations': self._testHistory
        }