from src.Robot import Robot
from src.configLocal import clientCode
from src.configLocal import accountId
from src.MLModule import MLModule
import time

state = "BUY"
candles = []

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

if __name__ == "__main__":
    robot = Robot(
        clientCode=clientCode,
        accountId=accountId,
        classCode="CETS",
        tickerCode="USD000000TOD",
        contentType="currency"
    )
    # robot.connectToQuik()
    # robot.subscribe(strategy, 1)
    #robot.subscribeHistorical(strategyHistorical, 1262304000, int(time.time()), 1)
    robot.subscribeHistorical(strategyHistorical, 1704067200, int(time.time()), 1)
    print(MLModule.getNextCandle(candles[0:-1]))
    # robot.createOrder(1)
