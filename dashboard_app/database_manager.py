import logging
import sqlite3
from typing import Any, Dict, List, Tuple

import pandas as pd

# --- Project Imports ---
from config.settings import DB_PATH, DEFAULT_START_DATE

logger = logging.getLogger(__name__)


class DatabaseManager:
    """
    A dedicated manager for all database interactions. It handles creating tables,
    writing data, and fetching data, acting as the single source of truth
    for the application's database schema and operations.
    """

    def __init__(self, db_path: str = DB_PATH, conn: sqlite3.Connection = None):
        """
        Initializes the DatabaseManager.

        Args:
            db_path: The path to the SQLite database file.
            conn: An optional existing database connection. If not provided,
                  a new one will be created when needed.
        """
        self.db_path = db_path
        # Allow using an existing connection, essential for the pipeline orchestrator
        self._conn = conn
        self._connection_owner = conn is None
        # Ensure the schema is ready when the orchestrator starts
        if self._connection_owner:
            self.create_tables()

    def _get_connection(self) -> sqlite3.Connection:
        """Establishes and returns a database connection."""
        if self._conn and not self._connection_owner:
            return self._conn
        try:
            return sqlite3.connect(self.db_path, timeout=10)
        except sqlite3.Error as e:
            logger.exception(f"Database connection failed: {e}")
            raise

    def create_tables(self):
        """
        Creates all necessary tables for the application if they don't exist.
        This schema is designed to support multiple data granularities.
        """
        logger.info("Ensuring all database tables exist...")
        try:
            with self._get_connection() as conn:
                # Table for universe metadata (unchanged)
                conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS universe_metadata (
                                                                     Ticker TEXT PRIMARY KEY,
                                                                     AssetType TEXT NOT NULL,
                                                                     Sector TEXT
                    );
                    """
                )

                # --- REFACTOR: Create separate tables for each granularity ---
                # Table for daily historical price and technical indicator data
                conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS price_data_daily (
                                                                    Timestamp DATETIME NOT NULL,
                                                                    Ticker TEXT NOT NULL,
                                                                    Open REAL, High REAL, Low REAL, Close REAL, Volume INTEGER,
                                                                    volatility_90d REAL, beta REAL, sharpe_ratio_90d REAL, rsi_14d REAL,
                                                                    PRIMARY KEY (Timestamp, Ticker),
                                                                    FOREIGN KEY (Ticker) REFERENCES universe_metadata (Ticker) ON DELETE CASCADE
                    );
                    """
                )

                # Table for hourly historical price data
                conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS price_data_hourly (
                                                                     Timestamp DATETIME NOT NULL,
                                                                     Ticker TEXT NOT NULL,
                                                                     Open REAL, High REAL, Low REAL, Close REAL, Volume INTEGER,
                        -- Note: Indicators would have different windows for hourly data
                                                                     rsi_24h REAL,
                                                                     PRIMARY KEY (Timestamp, Ticker),
                                                                     FOREIGN KEY (Ticker) REFERENCES universe_metadata (Ticker) ON DELETE CASCADE
                    );
                    """
                )
                # Table for user-generated research notes
                conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS research_notes (
                                                                  Ticker TEXT PRIMARY KEY,
                                                                  Notes TEXT,
                                                                  LastUpdated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                                                                  FOREIGN KEY (Ticker) REFERENCES universe_metadata (Ticker) ON DELETE CASCADE
                    );
                    """
                )
            logger.info("✅ Database schema is ready.")
        except sqlite3.Error as e:
            logger.exception(f"❌ Failed to create database tables: {e}")
            raise

    def get_universe_tickers(self) -> List[str]:
        """Fetches the complete list of unique tickers from the metadata table."""
        try:
            with self._get_connection() as conn:
                df = pd.read_sql_query(
                    "SELECT Ticker FROM universe_metadata ORDER BY Ticker ASC", conn
                )
                return df["Ticker"].tolist()
        except pd.io.sql.DatabaseError:
            return []  # Return empty if table doesn't exist yet

    def get_tickers_by_asset_type(self, asset_type: str) -> List[str]:
        """Fetches tickers filtered by a specific asset type (e.g., 'Equity')."""
        try:
            with self._get_connection() as conn:
                query = "SELECT Ticker FROM universe_metadata WHERE AssetType = ? ORDER BY Ticker ASC"
                df = pd.read_sql_query(query, conn, params=(asset_type,))
                return df["Ticker"].tolist()
        except pd.io.sql.DatabaseError:
            return []

    def get_latest_date(self, granularity: str = "daily") -> str:
        """
        Finds the most recent timestamp in the specified granularity table.
        """
        table_name = f"price_data_{granularity}"
        try:
            with self._get_connection() as conn:
                # Use pragma_table_info to see if the table exists first
                cursor = conn.execute(f"PRAGMA table_info({table_name});")
                if cursor.fetchone() is None:
                    return DEFAULT_START_DATE

                latest_date = conn.execute(
                    f"SELECT MAX(Timestamp) FROM {table_name}"
                ).fetchone()[0]
                return latest_date or DEFAULT_START_DATE
        except (sqlite3.OperationalError, TypeError):
            return DEFAULT_START_DATE

    def write_price_data(self, df: pd.DataFrame, granularity: str = "daily"):
        """
        Writes a DataFrame of price data to the correct granularity table
        using an efficient and safe "upsert" method.
        """
        if df.empty:
            logger.warning(
                f"Attempted to write an empty DataFrame to price_data_{granularity}. Skipping."
            )
            return

        table_name = f"price_data_{granularity}"
        logger.info(f"Writing {len(df)} rows to '{table_name}' table...")

        # --- FIX: Robustly handle index-to-column conversion ---
        # The previous logic could fail if a column name conflicted with the index name.
        # This implementation is more robust. It identifies the new column created
        # from the index and reliably renames it to 'Timestamp'.
        original_cols = df.columns
        df_to_write = df.reset_index()
        # Find the name of the column that was created from the index.
        new_cols = df_to_write.columns.difference(original_cols)
        if len(new_cols) == 1:
            index_col_name = new_cols[0]
            df_to_write.rename(columns={index_col_name: "Timestamp"}, inplace=True)
        else:
            logger.error(
                f"Expected 1 new column after reset_index, but found {len(new_cols)}. "
                "Could not determine index column. Aborting write."
            )
            return
        try:
            with self._get_connection() as conn:
                df_to_write.to_sql(
                    table_name,
                    conn,
                    if_exists="append",
                    index=False,
                    method=self._upsert_method,
                )
            logger.info(
                f"✅ Successfully wrote {len(df_to_write)} rows to {table_name}."
            )
        except Exception as e:
            logger.exception(f"❌ Failed to write to {table_name}: {e}")
            raise

    def _upsert_method(self, table, conn, keys, data_iter):
        """
        A dynamic, schema-aware custom method for pandas `to_sql` to perform
        an 'INSERT ... ON CONFLICT ... DO UPDATE'.
        """
        # 1. Introspect the table to find its primary keys
        # FIX: The 'conn' object provided by pandas' fallback sqlite engine is
        # actually a cursor, not a connection. We use it directly.
        conn.execute(f"PRAGMA table_info({table.name})")
        primary_keys = [
            info[1] for info in conn.fetchall() if info[5] > 0
        ]  # Column 5 is 'pk'

        if not primary_keys:
            raise ValueError(
                f"Cannot perform upsert on table '{table.name}' because it has no primary key."
            )

        # 2. Dynamically build the SQL query
        # Columns to update are all columns except the primary keys
        update_cols = [key for key in keys if key not in primary_keys]

        # Create the 'col=excluded.col' string for the UPDATE clause
        update_clause = ", ".join([f"{col}=excluded.{col}" for col in update_cols])

        sql = f"""
            INSERT INTO {table.name} ({', '.join(keys)})
            VALUES ({', '.join(['?'] * len(keys))})
            ON CONFLICT({', '.join(primary_keys)}) DO UPDATE SET
                {update_clause}
        """

        # 3. Execute the query
        conn.executemany(sql, data_iter)

    def update_universe(
        self, source: str, tickers: List[str], metadata: Dict[str, Any]
    ) -> int:
        """Updates the universe_metadata table from a fetched source."""
        asset_type = "Crypto" if "crypto" in source.lower() else "Equity"
        data_to_insert = []
        for ticker in tickers:
            # The metadata from the fetcher is now just the sector string
            sector = metadata.get(ticker, "Unknown")
            data_to_insert.append((ticker, asset_type, sector))

        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.executemany(
                """
                INSERT INTO universe_metadata (Ticker, AssetType, Sector)
                VALUES (?, ?, ?)
                ON CONFLICT(Ticker) DO UPDATE SET
                                                  AssetType = excluded.AssetType,
                                                  Sector = excluded.Sector;
                """,
                data_to_insert,
            )
            conn.commit()
            return cursor.rowcount

    def add_ticker_to_universe(self, ticker: str, asset_type: str) -> Tuple[bool, str]:
        """Manually adds a single ticker to the universe."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(
                    """
                    INSERT INTO universe_metadata (Ticker, AssetType, Sector)
                    VALUES (?, ?, ?);
                    """,
                    (ticker, asset_type, "Unknown"),
                )
                conn.commit()
                return True, f"Ticker '{ticker}' added to the database."
            except sqlite3.IntegrityError:
                return False, f"Ticker '{ticker}' already exists in the database."

    def save_research_notes(self, ticker: str, notes: str):
        """Saves or updates research notes for a specific ticker."""
        sql = """
              INSERT INTO research_notes (Ticker, Notes, LastUpdated)
              VALUES (?, ?, CURRENT_TIMESTAMP)
              ON CONFLICT(Ticker) DO UPDATE SET
                                                Notes = excluded.Notes,
                                                LastUpdated = CURRENT_TIMESTAMP; \
              """
        try:
            with self._get_connection() as conn:
                conn.execute(sql, (ticker, notes))
                conn.commit()
        except sqlite3.Error as e:
            logger.exception(f"❌ Failed to save notes for {ticker}: {e}")

    def load_research_notes(self, ticker: str) -> str:
        """Loads research notes for a specific ticker."""
        sql = "SELECT Notes FROM research_notes WHERE Ticker = ?"
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(sql, (ticker,))
                result = cursor.fetchone()
                return result[0] if result else ""
        except sqlite3.Error as e:
            logger.exception(f"❌ Failed to load notes for {ticker}: {e}")
            return ""
