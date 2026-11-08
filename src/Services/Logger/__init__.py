"""Public exports loaded on demand, keeping optional ML/plotting dependencies optional."""
from importlib import import_module

_EXPORTS = {
    'PrintLogger': ('.PrintLogger', 'PrintLogger'),
    'FileLogger': ('.FileLogger', 'FileLogger'),
    'TimestampedLogger': ('.TimestampedLogger', 'TimestampedLogger'),
    'CompositeLogger': ('.CompositeLogger', 'CompositeLogger'),
    'GraphLogger2D': ('.GraphLogger2D', 'GraphLogger2D'),
    'ParamLogger': ('.ParamLogger', 'ParamLogger'),
}

__all__ = list(_EXPORTS)


def __getattr__(name):
    if name not in _EXPORTS:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    module, attribute = _EXPORTS[name]
    value = getattr(import_module(module, __name__), attribute)
    globals()[name] = value
    return value

# Preserve class exports when a lightweight child package is imported directly.
for _name in ['PrintLogger', 'FileLogger', 'TimestampedLogger', 'CompositeLogger', 'ParamLogger']:
    globals()[_name] = __getattr__(_name)
