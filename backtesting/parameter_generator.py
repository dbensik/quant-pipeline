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

    def generate_combinations(self) -> Iterator[Tuple[Dict, Dict]]:
        s_range = self.params.get("mac_short_range", [10, 20])
        s_step = self.params.get("mac_short_step", 2)
        l_range = self.params.get("mac_long_range", [30, 50])
        l_step = self.params.get("mac_long_step", 5)

        shorts = range(s_range[0], s_range[1] + 1, s_step)
        longs = range(l_range[0], l_range[1] + 1, l_step)

        for s, l in itertools.product(shorts, longs):
            if s >= l:
                continue
            model_params = {"short_window": s, "long_window": l}
            display_params = model_params.copy()
            yield model_params, display_params


class MeanReversionParameterGenerator(BaseParameterGenerator):
    """Generates parameter combinations for the Mean Reversion strategy."""

    def generate_combinations(self) -> Iterator[Tuple[Dict, Dict]]:
        w_range = self.params.get("mr_window_range", [15, 30])
        w_step = self.params.get("mr_window_step", 5)
        t_range = self.params.get("mr_threshold_range", [1.0, 2.0])
        t_step = self.params.get("mr_threshold_step", 0.5)

        windows = range(w_range[0], w_range[1] + 1, w_step)
        thresholds = [
            round(t, 2) for t in np.arange(t_range[0], t_range[1] + t_step, t_step)
        ]

        for w, t in itertools.product(windows, thresholds):
            model_params = {"window": w, "threshold": t}
            display_params = model_params.copy()
            yield model_params, display_params
