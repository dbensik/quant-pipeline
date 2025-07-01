# scripts/init_db.py
import sqlite3
from data_pipeline.watchlist_manager import WatchlistManager

DB_PATH = "../quant_pipeline.db"
RAW_TABLE = "price_data"
NORM_TABLE = "price_data_normalized"
ASSETS_TABLE = "assets"


def initialize_database():
    """
    Create or reset core database tables:
      1) raw price data
      2) normalized price data
      3) asset universe
      4) watchlist metadata via WatchlistManager
    """
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    # 1) raw price table
    c.execute(f"DROP TABLE IF EXISTS {RAW_TABLE};")
    c.execute(f"""
    CREATE TABLE {RAW_TABLE} (
      Date      TEXT    NOT NULL,
      Ticker    TEXT    NOT NULL,
      Open      REAL,
      High      REAL,
      Low       REAL,
      Close     REAL    NOT NULL,
      Volume    REAL,
      PRIMARY KEY (Date, Ticker)
    );
    """
    )

    # 2) normalized price table
    c.execute(f"DROP TABLE IF EXISTS {NORM_TABLE};")
    c.execute(f"""
    CREATE TABLE {NORM_TABLE} (
      Date        TEXT    NOT NULL,
      Ticker      TEXT    NOT NULL,
      Normalized  REAL    NOT NULL,
      PRIMARY KEY (Date, Ticker)
    );
    """
    )

    # 3) assets universe table
    c.execute(f"DROP TABLE IF EXISTS {ASSETS_TABLE};")
    c.execute(f"""
    CREATE TABLE {ASSETS_TABLE} (
      Ticker      TEXT    PRIMARY KEY,
      AssetClass  TEXT    NOT NULL,
      Sector      TEXT,
      MarketCap   REAL
    );
    """
    )

    conn.commit()
    conn.close()
    print(f"✂ Dropped & ✔ Created tables '{RAW_TABLE}', '{NORM_TABLE}', '{ASSETS_TABLE}' in {DB_PATH}")

    # 4) ensure watchlist tables/schema
    wm = WatchlistManager(db_path=DB_PATH)
    # wm.ensure_tables()


if __name__ == "__main__":
    initialize_database()
