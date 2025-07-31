import json
import os
from typing import Any, Dict

from config.settings import RESULTS_DIR


class ConfigManager:
    """Manages saving and loading of strategy and backtest configurations."""

    def __init__(self, config_dir: str = None):
        """
        Initializes the ConfigManager.

        Args:
            config_dir: The directory to store configuration files.
                        Defaults to a 'configs' subdirectory within RESULTS_DIR.
        """
        self.config_dir = config_dir or os.path.join(RESULTS_DIR, "configs")
        if not os.path.exists(self.config_dir):
            os.makedirs(self.config_dir)

    def save_config(self, config_name: str, config_data: Dict[str, Any]):
        """
        Saves a configuration dictionary to a JSON file.

        Args:
            config_name: The name for the configuration file (without extension).
            config_data: The dictionary of settings to save.
        """
        filepath = os.path.join(self.config_dir, f"{config_name}.json")
        try:
            with open(filepath, "w") as f:
                json.dump(config_data, f, indent=4)
            return True, f"Configuration '{config_name}' saved successfully."
        except Exception as e:
            return False, f"Failed to save configuration: {e}"

    def load_config(self, config_name: str) -> Dict[str, Any] | None:
        """
        Loads a configuration dictionary from a JSON file.

        Args:
            config_name: The name of the configuration file to load.

        Returns:
            The loaded configuration dictionary, or None if an error occurs.
        """
        filepath = os.path.join(self.config_dir, config_name)
        try:
            with open(filepath, "r") as f:
                return json.load(f)
        except FileNotFoundError:
            return None
        except Exception:
            return None

    def list_configs(self) -> list[str]:
        """Returns a list of all available configuration files."""
        return [f for f in os.listdir(self.config_dir) if f.endswith(".json")]
