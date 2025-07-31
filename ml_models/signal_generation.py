class SignalGenerator:
    def __init__(self, threshold=0.05):
        """
        Initialize the SignalGenerator with a specified threshold.
        """
        self.threshold = threshold

    def generate_signal(self, prediction):
        """
        Generate a signal based on a single prediction.
        Returns:
            "Buy" if prediction > threshold,
            "Sell" if prediction < -threshold,
            "Hold" otherwise.
        """
        if prediction > self.threshold:
            return "Buy"
        elif prediction < -self.threshold:
            return "Sell"
        else:
            return "Hold"

    def generate_signals_for_array(self, predictions):
        """
        Generate signals for an array of predictions.
        Returns a list of signals.
        """
        return [self.generate_signal(pred) for pred in predictions]
