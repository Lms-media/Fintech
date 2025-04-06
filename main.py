from src.Robot import Robot

if __name__ == "__main__":
    robot = Robot(
        clientCode="10134",
        accountId="NL0011100043",
        classCode="QJSIM",
        tickerCode="SBER"
    )
    print(robot.get_candles(count=3))
    robot.close_connection()