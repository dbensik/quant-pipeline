import json
import logging
from pathlib import Path
from typing import Dict, List

# --- Project Imports ---
from config.settings import WATCHLISTS_FILE_PATH

# --- Setup Logger ---
logger = logging.getLogger(__name__)


class WatchlistManager:
    """
    Manages the lifecycle (CRUD) of user-created watchlists, persisting them
    to a simple and human-readable JSON file.
    """

    def __init__(self, file_path: str = WATCHLISTS_FILE_PATH):
        """
        Initializes the WatchlistManager.

        Args:
            file_path: The path to the JSON file where watchlists are stored.
        """
        self.file_path = Path(file_path)
        self.watchlists = self.load()

    def load(self) -> Dict[str, List[str]]:
        """
        Loads the watchlists from the JSON file. If the file doesn't exist,
        it returns an empty dictionary.
        """
        if not self.file_path.exists():
            logger.info(
                f"Watchlist file not found at {self.file_path}. Initializing with empty watchlists."
            )
            return {}
        try:
            with open(self.file_path, "r") as f:
                content = f.read()
                if not content:
                    return {}
                return json.loads(content)
        except (json.JSONDecodeError, IOError) as e:
            logger.error(
                f"Failed to load or parse watchlists from {self.file_path}: {e}"
            )
            return {}

    def _save(self):
        """
        Private helper to save the current state of all watchlists to the JSON file.
        This is called internally by methods that modify the watchlists.
        """
        try:
            # Ensure parent directory exists
            self.file_path.parent.mkdir(parents=True, exist_ok=True)
            with open(self.file_path, "w") as f:
                json.dump(self.watchlists, f, indent=4)
            logger.info(f"Successfully saved watchlists to {self.file_path}")
        except IOError as e:
            logger.error(f"Failed to save watchlists to {self.file_path}: {e}")

    def add_or_update(self, name: str, tickers: List[str]):
        """
        Adds a new watchlist or updates an existing one by name.

        Args:
            name: The unique name of the watchlist.
            tickers: A list of ticker symbols for the watchlist.
        """
        if not name or not isinstance(tickers, list):
            logger.warning(
                "Invalid input for add_or_update. Name must be a non-empty string and tickers a list."
            )
            return

        self.watchlists[name] = sorted(list(set(tickers)))  # Ensure unique and sorted
        self._save()
        logger.info(f"Added/Updated watchlist: '{name}' with {len(tickers)} tickers.")

    def delete(self, name: str):
        """
        Deletes a watchlist by its name.

        Args:
            name: The name of the watchlist to delete.
        """
        if name in self.watchlists:
            del self.watchlists[name]
            self._save()
            logger.info(f"Deleted watchlist: {name}")
        else:
            logger.warning(f"Attempted to delete non-existent watchlist: {name}")

    def get_all_watchlists(self) -> Dict[str, List[str]]:
        """
        Returns all currently loaded watchlists.
        """
        return self.watchlists
