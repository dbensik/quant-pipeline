import itertools
from abc import ABC, abstractmethod
from typing import Dict, Iterator, Tuple

import numpy as np


class BaseParameterGenerator(ABC):
    """
    An abstract base class for strategy parameter generators.

    Its purpose is to encapsulate the logic for creating combinations
    of parameters for use in optimization routines.
    """

    def __init__(self, params: Dict):
        """
        Initializes the generator with the parameter ranges from the UI.

        Args:
            params (Dict): A dictionary containing the user's selections,
                           e.g., {'mac_short_range': [10, 20], 'mac_short_step': 2}.
        """
        self.params = params

    @abstractmethod
    def generate_combinations(self) -> Iterator[Tuple[Dict, Dict]]:
        """
        A generator method that yields parameter combinations for a strategy.

        Yields:
            Tuple[Dict, Dict]: A tuple containing two dictionaries:
                - The first dict is for instantiating the model (e.g., {'short_window': 10, 'long_window': 40}).
                - The second dict is for display in the results table (e.g., {'short_window': 10, 'long_window': 40}).
        """
        raise NotImplementedError("Subclasses must implement generate_combinations().")


class MACrossoverParameterGenerator(BaseParameterGenerator):
    """Generates parameter combinations for the Moving Average Crossover strategy."""

    @staticmethod
    def generate_grid(short_window_range, long_window_range) -> list:
        """Generates a list of parameter dictionaries for the grid search."""
        grid = []
        for s, l in itertools.product(short_window_range, long_window_range):
            if s >= l:
                continue
            grid.append({"short_window": s, "long_window": l})
        return grid


class MeanReversionParameterGenerator(BaseParameterGenerator):
    """Generates parameter combinations for the Mean Reversion strategy."""


    @staticmethod
    def generate_grid(window_range, threshold_range) -> list:
        """Generates a list of parameter dictionaries for the grid search."""
        grid = []
        for w, t in itertools.product(window_range, threshold_range):
            grid.append({"window": w, "threshold": t})
        return grid
