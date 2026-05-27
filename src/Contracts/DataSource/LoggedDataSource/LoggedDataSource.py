from Interfaces import IDataSource, ILogger

class LoggedDataSource(IDataSource):
    _dataSource: IDataSource
    _logger: ILogger

    def __init__(self, dataSource: IDataSource, logger: ILogger):
        self._dataSource = dataSource
        self._logger = logger

    def init(self) -> None:
        self._logger.log("Data Source initializing...")
        self._dataSource.init()
        self._logger.log("Data Source initialized")

    def getSeries(self):
        series = self._dataSource.getSeries()
        self._logger.log("Got candle series from Data Source")
        self._logger.log(str(series))

        return series
