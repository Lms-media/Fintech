from src.Robot import Robot
from src.TestRobot import TestRobot
from src.configLocal import clientCode
from src.configLocal import accountId
from src.MLModule import MLModule
import time

state = "BUY"

testState = "BUY"
candles = []
futureCandles = []

def strategyHistorical(data: dict):
    candles.append(data)
    print(data)

def strategy(data: dict):
    global state
    print(f"Candle came, operation is {state}")
    print(data)
    if state == "BUY":
        state = "NONE"
        return 1
    if state == "NONE":
        state = "SELL"
        return 0
    if state == "SELL":
        state = "BUY"
        return -1

def testStrategy(data: dict):
    global testState
    print(f"Candle came, operation is {testState}")
    print(data)
    print(futureCandles[0])
    testState = "NONE"
    answer = 0
    if data['close'] < futureCandles[0]['close']:
        testState = "BUY"
        answer = 1
    elif data['close'] > futureCandles[0]['close']:
        testState = "SELL"
        answer = -1
    futureCandles.pop(0)
    print(len(futureCandles))
    return answer

if __name__ == "__main__":
    robot = Robot(
        clientCode = clientCode,
        accountId = accountId,
        classCode = "CETS",
        tickerCode = "USD000000TOD",
        contentType = "currency"
    )
    robot.subscribeHistorical(strategyHistorical, 1262304000, int(time.time()), 24)
    futureCandles = MLModule.getNextCandle(candles[0:-100], 100)
    print(candles[0:-100][-1])

    testRobot = TestRobot(
        initialBalance = 100000,
        initialPositions = 0,
        data = candles,
        dateStr = '2024-01-09 00:00:00'
    )
    testRobot.runTestToDate('2024-06-10 00:00:00', testStrategy)
    results = testRobot.getTestResults()
    print(f"Profit: {results['profit']}")
