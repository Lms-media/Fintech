from Interfaces import IDataSource


class SeriesDataSource(IDataSource):
    """Expose an already loaded series or slice through the existing adapter."""
    def __init__(self, series):
        self._series = series

    def init(self):
        pass

    def getSeries(self):
        return self._series
