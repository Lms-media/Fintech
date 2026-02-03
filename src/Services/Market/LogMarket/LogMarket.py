from Interfaces import IMarket, ITask, IPortfolio, IContextProvider, TaskType

class LogMarket(IMarket):
    _portfolio: IPortfolio
    _contextProvider: IContextProvider

    def __init__(self, portfolio: IPortfolio, contextProvider: IContextProvider):
        self._portfolio = portfolio
        self._contextProvider = contextProvider

    def execute(self, task: ITask) -> None:
        assetPair = task.getAssetPair()
        quoteAsset = assetPair.getQuoteAsset()
        lotCount = task.getLotCount()
        context = self._contextProvider.getContext()
        price = context.getPrice(assetPair)

        if task.getType() == TaskType.Buy:
            self._portfolio.buyAsset(quoteAsset, lotCount, price)

        if task.getType() == TaskType.Sell:
            self._portfolio.sellAsset(quoteAsset, lotCount, price)

        print(f"Executing task... Type: {task.getType()}, Asset Pair: {task.getAssetPair()}, Lot Count: {task.getLotCount()}")
