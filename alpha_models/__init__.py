# alpha_models/__init__.py
from .base_strategy import BaseStrategy
from importlib import import_module
import pkgutil

# discover all strategy modules
_strategy_classes = {}
for finder, name, ispkg in pkgutil.iter_modules(__path__):
    if name == "base_strategy" or name.startswith("_"):
        continue
    module = import_module(f"alpha_models.{name}")
    # find any subclass of BaseStrategy in the module
    for obj in vars(module).values():
        if isinstance(obj, type) and issubclass(obj, BaseStrategy) and obj is not BaseStrategy:
            _strategy_classes[obj.__name__.lower()] = obj

def get_strategy(name: str) -> BaseStrategy:
    cls = _strategy_classes.get(name.lower())
    if not cls:
        raise KeyError(f"Strategy '{name}' not found. Available: {list(_strategy_classes)}")
    return cls
