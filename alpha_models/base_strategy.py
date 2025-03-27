from abc import ABC, abstractmethod

class BaseStrategy(ABC):
    @abstractmethod
    def generate_signals(self, data):
        """Generate trading signals based on input data."""
        pass

    @abstractmethod
    def backtest(self, data):
        """Run a backtest on the input data."""
        pass