import time
import matplotlib.pyplot as plt
import json
import argparse
from datetime import datetime
from src.data.MoexDataSource import MoexDataSource
from src.managers.HistoricalTradingManager import HistoricalTradingManager
from src.strategies.SimpleStrategy import SimpleStrategy


def load_config(config_path):
    with open(config_path, 'r', encoding='utf-8') as f:
        return json.load(f)


if __name__ == "__main__":
    
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--config',
        type=str,
        required=True,
        help='config path'
    )

    args = parser.parse_args()
    config = load_config(args.config)
    tickerCode = config['tickerCode']
    contentType = config['contentType']
    
    data = MoexDataSource(
        datetime.fromtimestamp(1262304000),
        datetime.fromtimestamp(int(time.time())),
        24,
        tickerCode,
        contentType,
    )
    
    simpleStrategy = SimpleStrategy(5)
    
    instruments = {tickerCode: simpleStrategy}
    dataSources = {tickerCode: data}
    date = datetime.strptime("2024-01-09 00:00:00", "%Y-%m-%d %H:%M:%S")
    
    print("", flush=True)
    print(100000)
    manager = HistoricalTradingManager(instruments, 5, dataSources, 100000, date)
    manager.start()
    print(manager.virtualPortfolio.getCurrentState().getCapitalization())

    fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(nrows=2, ncols=2, figsize=(8, 6))
    dates = [state.datetime for state in manager.virtualPortfolio._history]
    amounts = [state.getCapitalization() for state in manager.virtualPortfolio._history]
    assets = [list(state.assets.values())[0] for state in manager.virtualPortfolio._history]
    rates = [list(state.exchangeRates.values())[0] for state in manager.virtualPortfolio._history]
    ax1.plot(dates, amounts)
    ax1.set_title('Capitalization')
    ax2.plot(dates, assets)
    ax2.set_title('Assets')
    ax3.plot(dates, rates)
    ax3.set_title('Exchange rate')
    plt.tight_layout()
    plt.show()
