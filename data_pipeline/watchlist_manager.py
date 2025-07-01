import sqlite3
import logging
from typing import List, Optional

logger = logging.getLogger(__name__)

class WatchlistManager:
    """
    Manage user-defined watchlists stored in a SQLite database.
    """

    def __init__(self, db_path: str):
        self.db_path = db_path
        self._ensure_tables()

    def _get_connection(self):
        return sqlite3.connect(self.db_path)

    def _ensure_tables(self):
        """
        Create the watchlists and watchlist_symbols tables if they don't exist.
        """
        with self._get_connection() as conn:
            c = conn.cursor()
            # Table of watchlists
            c.execute(
                """
                CREATE TABLE IF NOT EXISTS watchlists (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT UNIQUE NOT NULL
                )
                """
            )
            # Table of symbols per watchlist
            c.execute(
                """
                CREATE TABLE IF NOT EXISTS watchlist_symbols (
                    watchlist_id INTEGER NOT NULL,
                    symbol TEXT NOT NULL,
                    PRIMARY KEY (watchlist_id, symbol),
                    FOREIGN KEY (watchlist_id) REFERENCES watchlists(id) ON DELETE CASCADE
                )
                """
            )
            conn.commit()
        logger.info("Watchlist tables ensured in %s", self.db_path)


    def create_watchlist(self, name: str) -> bool:
        """
        Create a new watchlist.
        Returns True if created, False if it already exists.
        """
        try:
            with self._get_connection() as conn:
                conn.execute("INSERT INTO watchlists(name) VALUES (?)", (name,))
                conn.commit()
            logger.info("Created watchlist '%s'", name)
            return True
        except sqlite3.IntegrityError:
            logger.warning("Watchlist '%s' already exists", name)
            return False

    def delete_watchlist(self, name: str) -> bool:
        """
        Delete a watchlist and its symbols.
        Returns True if deleted, False if not found.
        """
        with self._get_connection() as conn:
            c = conn.cursor()
            c.execute("DELETE FROM watchlists WHERE name = ?", (name,))
            deleted = c.rowcount
            conn.commit()
        if deleted:
            logger.info("Deleted watchlist '%s'", name)
            return True
        else:
            logger.warning("Watchlist '%s' not found", name)
            return False

    def list_watchlists(self) -> List[str]:
        """
        Return a list of all watchlist names.
        """
        with self._get_connection() as conn:
            c = conn.cursor()
            c.execute("SELECT name FROM watchlists ORDER BY name")
            rows = c.fetchall()
        names = [row[0] for row in rows]
        logger.debug("Existing watchlists: %s", names)
        return names

    def add_symbol(self, watchlist: str, symbol: str) -> bool:
        """
        Add a symbol to a specified watchlist.
        Returns True if added, False if already present or watchlist missing.
        """
        with self._get_connection() as conn:
            c = conn.cursor()
            # fetch watchlist id
            c.execute("SELECT id FROM watchlists WHERE name = ?", (watchlist,))
            row = c.fetchone()
            if not row:
                logger.error("Watchlist '%s' does not exist", watchlist)
                return False
            wid = row[0]
            try:
                c.execute(
                    "INSERT INTO watchlist_symbols(watchlist_id, symbol) VALUES (?, ?)" ,
                    (wid, symbol)
                )
                conn.commit()
                logger.info("Added symbol '%s' to watchlist '%s'", symbol, watchlist)
                return True
            except sqlite3.IntegrityError:
                logger.warning("Symbol '%s' already in watchlist '%s'", symbol, watchlist)
                return False

    def remove_symbol(self, watchlist: str, symbol: str) -> bool:
        """
        Remove a symbol from a watchlist.
        Returns True if removed, False if not present.
        """
        with self._get_connection() as conn:
            c = conn.cursor()
            # fetch watchlist id
            c.execute("SELECT id FROM watchlists WHERE name = ?", (watchlist,))
            row = c.fetchone()
            if not row:
                logger.error("Watchlist '%s' does not exist", watchlist)
                return False
            wid = row[0]
            c.execute(
                "DELETE FROM watchlist_symbols WHERE watchlist_id = ? AND symbol = ?",
                (wid, symbol)
            )
            deleted = c.rowcount
            conn.commit()
        if deleted:
            logger.info("Removed symbol '%s' from watchlist '%s'", symbol, watchlist)
            return True
        else:
            logger.warning("Symbol '%s' not in watchlist '%s'", symbol, watchlist)
            return False

    def get_symbols(self, watchlist: str) -> Optional[List[str]]:
        """
        Return the list of symbols in a given watchlist.
        Returns None if watchlist does not exist.
        """
        with self._get_connection() as conn:
            c = conn.cursor()
            c.execute("SELECT id FROM watchlists WHERE name = ?", (watchlist,))
            row = c.fetchone()
            if not row:
                logger.error("Watchlist '%s' does not exist", watchlist)
                return None
            wid = row[0]
            c.execute(
                "SELECT symbol FROM watchlist_symbols WHERE watchlist_id = ? ORDER BY symbol",
                (wid,)
            )
            rows = c.fetchall()
        symbols = [r[0] for r in rows]
        logger.debug("Symbols in '%s': %s", watchlist, symbols)
        return symbols
