import time
import matplotlib.pyplot as plt
import json
import argparse
from datetime import datetime
from src.data.MoexDataSource import MoexDataSource
from src.data.SavedDataSource import SavedDataSource
from src.managers.HistoricalTradingManager import HistoricalTradingManager
from src.strategies.SimpleStrategy import SimpleStrategy
from src.strategies.MLIndividualStrategy import MLIndividualStrategy
from src.managers.TrainingManagers.IndividualTrainingManager import InvidualTrainingManager
from src.data.Candle import Candle
from dataclasses import asdict
from typing import List
from pathlib import Path


def load_config(config_path):
    with open(config_path, "r", encoding="utf-8") as f:
        return json.load(f)


def candleListToJson(candles: List[Candle], filename: str):
    def default_serializer(obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        raise TypeError(f"Object of type {type(obj)} is not JSON serializable")

    with open(filename, "w", encoding="utf-8") as f:
        json.dump([asdict(c) for c in candles], f, default=default_serializer, indent=2)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", type=str, required=True, help="config path")
    parser.add_argument("--saved", type=str, help="save or use saved data in path")

    args = parser.parse_args()
    config = load_config(args.config)
    tickerCode = config["tickerCode"]
    contentType = config["contentType"]

    if args.saved and Path(args.saved).exists:
        data = SavedDataSource(24, tickerCode, args.saved)
    else:
        data = MoexDataSource(
            datetime.fromtimestamp(1262304000),
            datetime.fromtimestamp(int(time.time())),
            24,
            tickerCode,
            contentType,
        )
        if args.saved:
            Path(args.saved).parent.mkdir(parents=True, exist_ok=True)
            candleListToJson(data.candles, "/home/Данил/Desktop/Fintech/candles/candles.json")

    # simpleStrategy = SimpleStrategy(5)
    trainingManager = InvidualTrainingManager({config["tickerCode"]: data}, {config["tickerCode"]: {}}, 5, False)
    trainingManager.train_on_tickers()
    trainingManager.evaluate_model()
    filename = "IndividualModel.pkl"
    # Path(path).parent.mkdir(parents=True, exist_ok=True)
    trainingManager.save_model(filename)

    mlStrategy = MLIndividualStrategy(30, config["tickerCode"], data, filename)
    # mlStrategy.load_model(filename)
    instruments = {tickerCode: mlStrategy}
    dataSources = {tickerCode: data}
    date = datetime.strptime("2024-01-09 00:00:00", "%Y-%m-%d %H:%M:%S")

    print("", flush=True)
    print(100000)
    manager = HistoricalTradingManager(instruments, 600, dataSources, 100000, date)
    manager.start()
    print(manager.virtualPortfolio.getCurrentState().getCapitalization())

    fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(nrows=2, ncols=2, figsize=(8, 6))
    dates = [state.datetime for state in manager.virtualPortfolio._history]
    capitals = [
        state.getCapitalization() for state in manager.virtualPortfolio._history
    ]
    assets = [
        list(state.assets.values())[0] for state in manager.virtualPortfolio._history
    ]
    rates = [
        list(state.exchangeRates.values())[0]
        for state in manager.virtualPortfolio._history
    ]
    amounts = [state.amount for state in manager.virtualPortfolio._history]
    ax1.plot(dates, capitals)
    ax1.set_title("Capitalization")
    ax2.plot(dates, assets)
    ax2.set_title("Assets")
    ax3.plot(dates, rates)
    ax3.set_title("Exchange rate")
    ax4.plot(dates, amounts)
    ax4.set_title("Amount")
    plt.tight_layout()
    plt.show()
    
