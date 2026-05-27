from Interfaces import IPortfolio, ILogger, IAsset, IExecutionContext

class LoggedPortfolio(IPortfolio):
    _portfolio: IPortfolio
    _logger: ILogger

    def __init__(self, portfolio: IPortfolio, logger: ILogger):
        self._portfolio = portfolio
        self._logger = logger
        self._logger.log(f"Init portfolio; Base Asset: {str(self._portfolio.getBaseAsset())}; Base Amount: {self._portfolio.getBaseAmount()}")

    def getBaseAsset(self) -> IAsset:
        return self._portfolio.getBaseAsset()

    def getAssetAmount(self, asset: IAsset) -> float:
        return self._portfolio.getAssetAmount(asset)

    def getBaseAmount(self) -> float:
        return self._portfolio.getBaseAmount()

    def getCapitalization(self, context: IExecutionContext) -> float:
        return self._portfolio.getCapitalization(context)

    def buyAsset(self, asset: IAsset, lotCount: int, price: float) -> None:
        self._portfolio.buyAsset(asset, lotCount, price)
        self._logger.log(f"Buy; Asset: {str(asset)}; Lot Count: {lotCount}; Price: {price}\nBase Amount: {self.getBaseAmount()}; AssetAmount: {self.getAssetAmount(asset)}")

    def sellAsset(self, asset: IAsset, lotCount: int, price: float) -> None:
        self._portfolio.sellAsset(asset, lotCount, price)
        self._logger.log(f"Sell; Asset: {str(asset)}; Lot Count: {lotCount}; Price: {price}\nBase Amount: {self.getBaseAmount()}; AssetAmount: {self.getAssetAmount(asset)}")

    def deposit(self, amount: float) -> None:
        self._portfolio.deposit(amount)
        self._logger.log(f"Deposit; Amount: {amount}\nBase Amount: {self.getBaseAmount()}")

    def withdraw(self, amount: float) -> None:
        self._portfolio.withdraw(amount)
        self._logger.log(f"Withdraw; Amount: {amount}\nBase Amount: {self.getBaseAmount()}")
