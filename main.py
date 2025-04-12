from src.Robot import Robot

state = "BUY"

def strategy(data: dict):
    global state
    print(f"Candle came, operation is {state}")
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
    robot._getHistoryData(1744480000, 1744496365)
    # robot.subscribe(strategy, 1)