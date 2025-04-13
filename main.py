from src.Robot import Robot
from src.utils.fetchMoex import fetchMoex

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
    robot = Robot(
        clientCode="10134",
        accountId="NL0011100043",
        classCode="QJSIM",
        tickerCode="SBER"
    )
    robot.subscribe(strategy, 1)
    robot.subscribeHistorical(strategyHistorical, 1744460000, 1744470000, 1)