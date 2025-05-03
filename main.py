import time
from src.Robot import Robot

state = "BUY"

def strategyHistorical(data: dict):
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
    # NL0011100043
    # 10088
    robot = Robot(
        clientCode="10088",
        accountId="NL0011100043",
        # classCode="CETS",
        # tickerCode="USD000TODTOM",
        classCode="QJSIM",
        tickerCode="SBER",
        moexTickerCode="USD000TODTOM",
        contentType="currency"
    )
    robot.subscribe(strategy, 1)
    robot.subscribeHistorical(strategyHistorical, 1704067200, int(time.time()), 1)
