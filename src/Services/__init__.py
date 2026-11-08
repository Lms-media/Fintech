"""Public exports loaded on demand, keeping optional ML/plotting dependencies optional."""
from importlib import import_module

_EXPORTS = {
    'RuntimePortfolio': ('.Portfolio', 'RuntimePortfolio'),
    'LoggedPortfolio': ('.Portfolio', 'LoggedPortfolio'),
    'PortfolioSyncMarket': ('.Market', 'PortfolioSyncMarket'),
    'LoggedMarket': ('.Market', 'LoggedMarket'),
    'PrintLogger': ('.Logger', 'PrintLogger'),
    'FileLogger': ('.Logger', 'FileLogger'),
    'TimestampedLogger': ('.Logger', 'TimestampedLogger'),
    'CompositeLogger': ('.Logger', 'CompositeLogger'),
    'GraphLogger2D': ('.Logger', 'GraphLogger2D'),
    'ParamLogger': ('.Logger', 'ParamLogger'),
}

__all__ = list(_EXPORTS)


def __getattr__(name):
    if name not in _EXPORTS:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    module, attribute = _EXPORTS[name]
    value = getattr(import_module(module, __name__), attribute)
    globals()[name] = value
    return value
