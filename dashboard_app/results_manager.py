import logging
import os
import pickle
from typing import Any, List

# --- Centralized Configuration Import ---
from config.settings import RESULTS_DIR

logger = logging.getLogger(__name__)


class ResultsManager:
    """
    Handles all file I/O for saving and loading backtest results.
    Ensures that results are stored in a consistent, configured location.
    """

    def __init__(self, results_dir: str = RESULTS_DIR):
        self.results_dir = results_dir
        # Ensure the directory exists when the manager is initialized.
        self._create_directory_if_not_exists()

    def _create_directory_if_not_exists(self):
        """Creates the results directory if it doesn't already exist."""
        try:
            os.makedirs(self.results_dir, exist_ok=True)
        except OSError as e:
            logger.error(
                f"Failed to create results directory at {self.results_dir}: {e}"
            )
            # Depending on the app's needs, you might want to raise the exception.
            # For now, we log the error and continue.

    def save(self, filename: str, data: Any) -> bool:
        """
        Saves data to a file in the results directory using pickle.

        Args:
            filename: The name of the file (e.g., 'my_backtest.pkl').
            data: The Python object to save.

        Returns:
            True if successful, False otherwise.
        """
        if not filename.endswith(".pkl"):
            filename += ".pkl"

        filepath = os.path.join(self.results_dir, filename)
        logger.info(f"Saving results to {filepath}...")
        try:
            with open(filepath, "wb") as f:
                pickle.dump(data, f)
            logger.info("✅ Results saved successfully.")
            return True
        except (pickle.PicklingError, IOError) as e:
            logger.error(f"❌ Failed to save results to {filepath}: {e}")
            return False

    def load(self, filename: str) -> Any | None:
        """
        Loads data from a file in the results directory.

        Args:
            filename: The name of the file to load.

        Returns:
            The loaded Python object, or None if an error occurs.
        """
        filepath = os.path.join(self.results_dir, filename)
        logger.info(f"Loading results from {filepath}...")
        try:
            with open(filepath, "rb") as f:
                data = pickle.load(f)
            logger.info("✅ Results loaded successfully.")
            return data
        except (pickle.UnpicklingError, FileNotFoundError, IOError) as e:
            logger.error(f"❌ Failed to load results from {filepath}: {e}")
            return None

    def get_saved_files(self) -> List[str]:
        """Returns a list of all .pkl files in the results directory."""
        if not os.path.isdir(self.results_dir):
            return []
        try:
            return sorted(
                [f for f in os.listdir(self.results_dir) if f.endswith(".pkl")]
            )
        except OSError as e:
            logger.error(f"Could not read files from {self.results_dir}: {e}")
            return []
