from Interfaces import IMarket, ITask, IPortfolio, IContextProvider, TaskType

class PortfolioSyncMarket(IMarket):
    _portfolio: IPortfolio
    _contextProvider: IContextProvider

    def __init__(self, portfolio: IPortfolio, contextProvider: IContextProvider):
        self._portfolio = portfolio
        self._contextProvider = contextProvider

    def execute(self, task: ITask) -> None:
        assetPair = task.getAssetPair()
        quoteAsset = assetPair.getQuoteAsset()
        lotCount = task.getLotCount()
        context = self._contextProvider.getContext(task.getTimestamp())
        openPrice = context.getPrice(assetPair)
        closePrice = context.getClosePrice(assetPair)
        hightPrice = context.getHightPrice(assetPair)
        lowPrice = context.getLowPrice(assetPair)

        if task.getType() == TaskType.Buy and openPrice:
            print("market buying", openPrice)
            self._portfolio.buyAsset(quoteAsset, lotCount, openPrice)

        if task.getType() == TaskType.Sell and openPrice:
            print("market selling", openPrice)
            self._portfolio.sellAsset(quoteAsset, lotCount, openPrice)
        
        if task.getType() == TaskType.BuyWithLimit and closePrice and lowPrice and hightPrice:
            if lowPrice < task.getLowLimit():    
                print("market low buying", task.getLowLimit())
                self._portfolio.buyAsset(quoteAsset, lotCount, task.getLowLimit())
            elif hightPrice > task.getHightLimit():    
                print("market hight buying", task.getHightLimit())
                self._portfolio.buyAsset(quoteAsset, lotCount, task.getHightLimit())
            else:
                print("market close buying", closePrice)
                self._portfolio.buyAsset(quoteAsset, lotCount, closePrice)
        
        if task.getType() == TaskType.SellWithLimit and closePrice and lowPrice and hightPrice:
            if lowPrice < task.getLowLimit():    
                print("market low selling", task.getLowLimit())
                self._portfolio.sellAsset(quoteAsset, lotCount, task.getLowLimit())
            elif hightPrice > task.getHightLimit():    
                print("market hight selling", task.getHightLimit())
                self._portfolio.sellAsset(quoteAsset, lotCount, task.getHightLimit())
            else:
                print("market close selling", closePrice)
                self._portfolio.sellAsset(quoteAsset, lotCount, closePrice)
