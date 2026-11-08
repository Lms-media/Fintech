from math import isfinite
from Interfaces import IMarket, ITask, IPortfolio, IContextProvider, TaskType

class PortfolioSyncMarket(IMarket):
    _portfolio: IPortfolio
    _contextProvider: IContextProvider

    def __init__(self, portfolio: IPortfolio, contextProvider: IContextProvider,
                 commissionRate: float = 0.0, slippageRate: float = 0.0):
        if any(not isfinite(v) or not 0 <= v < 1 for v in [commissionRate, slippageRate]):
            raise ValueError("Cost rates must be finite and in [0, 1)")
        self._portfolio = portfolio
        self._contextProvider = contextProvider
        self._commissionRate = commissionRate
        self._slippageRate = slippageRate
        self._fills = []

    def getFills(self):
        return tuple(dict(fill) for fill in self._fills)

    def _fill(self, task, price, buy, reason):
        price *= 1 + self._slippageRate if buy else 1 - self._slippageRate
        asset = task.getAssetPair().getQuoteAsset()
        quantity = task.getLotCount() * asset.getLotSize()
        operation = self._portfolio.buyAsset if buy else self._portfolio.sellAsset
        operation(asset, task.getLotCount(), price)
        fee = quantity * price * self._commissionRate
        self._portfolio.withdraw(fee)
        self._fills.append(dict(timestamp=task.getTimestamp(), buy=buy, price=price,
                                lots=task.getLotCount(), quantity=quantity, fee=fee, reason=reason))

    def _executeCandleCorridor(self, task, context):
        pair = task.getAssetPair()
        opening = context.getPrice(pair)
        high, low, close = context.getHightPrice(pair), context.getLowPrice(pair), context.getClosePrice(pair)
        upper, lower = task.getHightLimit(), task.getLowLimit()
        if (any(v is None or not isfinite(v) or v <= 0 for v in [opening, high, low, close, upper, lower])
                or not lower < opening < upper or not low <= min(opening, close) <= max(opening, close) <= high):
            raise ValueError("Invalid current candle or corridor bounds")
        buy = task.getType() == TaskType.BuyCandleCorridor  # buy closes a short
        # OHLC cannot establish touch order. Always choose the adverse stop first.
        stop_hit = high >= upper if buy else low <= lower
        profit_hit = low <= lower if buy else high >= upper
        if stop_hit:
            price, reason = (upper if buy else lower), "stop"
        elif profit_hit:
            price, reason = (lower if buy else upper), "take_profit"
        else:
            price, reason = close, "close"
        self._fill(task, price, buy, reason)

    def execute(self, task: ITask) -> None:
        assetPair = task.getAssetPair()
        quoteAsset = assetPair.getQuoteAsset()
        lotCount = task.getLotCount()
        if lotCount == 0:
            return
        
        context = self._contextProvider.getContext(task.getTimestamp())
        if task.getType() in (TaskType.BuyCandleCorridor, TaskType.SellCandleCorridor):
            self._executeCandleCorridor(task, context)
            return
        if task.getType() in (TaskType.Buy, TaskType.Sell):
            price = context.getPrice(assetPair)
            if price is None or not isfinite(price) or price <= 0:
                raise ValueError("Missing positive execution price")
            self._fill(task, price, task.getType() == TaskType.Buy, "open")
            return
        previousCandle = self._contextProvider.getPreviousCandle(task.getTimestamp())
        hightPrice = lowPrice = None
        
        openPrice = context.getPrice(assetPair)
        closePrice = context.getClosePrice(assetPair)
        if previousCandle:
            hightPrice = previousCandle.getHighPrice()
            lowPrice = previousCandle.getLowPrice()

        if task.getType() == TaskType.BuyClose and closePrice:
            print("market buying by close", closePrice)
            self._portfolio.buyAsset(quoteAsset, lotCount, closePrice)

        if task.getType() == TaskType.SellClose and closePrice:
            print("market selling by close", closePrice)
            self._portfolio.sellAsset(quoteAsset, lotCount, closePrice)
        
        if task.getType() == TaskType.BuyWithLimit and openPrice and lowPrice and hightPrice:
            if lowPrice < task.getLowLimit():    
                print("market low buying", task.getLowLimit())
                self._portfolio.buyAsset(quoteAsset, lotCount, task.getLowLimit())
            elif hightPrice > task.getHightLimit():    
                print("market hight buying", task.getHightLimit())
                self._portfolio.buyAsset(quoteAsset, lotCount, task.getHightLimit())
            else:
                print("market close buying", closePrice)
                self._portfolio.buyAsset(quoteAsset, lotCount, openPrice)
        
        if task.getType() == TaskType.SellWithLimit and openPrice and lowPrice and hightPrice:
            if lowPrice < task.getLowLimit():    
                print("market low selling", task.getLowLimit())
                self._portfolio.sellAsset(quoteAsset, lotCount, task.getLowLimit())
            elif hightPrice > task.getHightLimit():    
                print("market hight selling", task.getHightLimit())
                self._portfolio.sellAsset(quoteAsset, lotCount, task.getHightLimit())
            else:
                print("market close selling", closePrice)
                self._portfolio.sellAsset(quoteAsset, lotCount, openPrice)
