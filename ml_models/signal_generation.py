from abc import ABC, abstractmethod
from typing import List, Optional

class SignalStrategy(ABC):
    """
    Abstract base class for signal generation strategies.
    Follows the Open/Closed Principle (OCP) allowing new strategies 
    to be added without modifying the generator.
    """
    @abstractmethod
    def evaluate(self, prediction: float) -> str:
        """Evaluate a prediction to generate a signal."""
        pass

class ThresholdSignalStrategy(SignalStrategy):
    """
    Standard strategy that generates signals based on a fixed threshold.
    """
    def __init__(self, threshold: float = 0.05):
        self.threshold = threshold

    def evaluate(self, prediction: float) -> str:
        if prediction > self.threshold:
            return "Buy"
        elif prediction < -self.threshold:
            return "Sell"
        else:
            return "Hold"

class SignalGenerator:
    def __init__(self, strategy: Optional[SignalStrategy] = None, threshold: float = None):
        """
        Initialize the SignalGenerator.
        
        Args:
            strategy: A concrete SignalStrategy instance.
            threshold: Legacy support for threshold-based initialization. 
                       If provided, creates a ThresholdSignalStrategy.
        """
        if strategy:
            self.strategy = strategy
        elif threshold is not None:
            self.strategy = ThresholdSignalStrategy(threshold)
        else:
            self.strategy = ThresholdSignalStrategy()

    def generate_signal(self, prediction: float) -> str:
        """
        Generate a signal based on a single prediction using the active strategy.
        """
        return self.strategy.evaluate(prediction)

    def generate_signals_for_array(self, predictions: List[float]) -> List[str]:
        """
        Generate signals for an array of predictions.
        """
        return [self.generate_signal(pred) for pred in predictions]
