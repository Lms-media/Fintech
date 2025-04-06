from src.Robot import Robot

def test(data: dict):
    print(data["data"]["open"])

if __name__ == "__main__":
    robot = Robot(
        clientCode="10134",
        accountId="NL0011100043",
        classCode="QJSIM",
        tickerCode="SBER"
    )
    robot.subscribe(test, 1)