# scripts/init_db.py
import sqlite3

DB_PATH = "../quant_pipeline.db"
TABLE_NAME = "price_data"

def initialize_database(db_path=DB_PATH, table_name=TABLE_NAME):
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    # 1) drop the old table
    c.execute(f"DROP TABLE IF EXISTS {table_name};")
    # 2) create the new canonical schema
    c.execute(f"""
    CREATE TABLE {table_name} (
      Date    TEXT    NOT NULL,   -- ISO8601 date string
      Ticker  TEXT    NOT NULL,   -- e.g. "SPY" or "BTC-USD"
      Open    REAL,
      High    REAL,
      Low     REAL,
      Close   REAL    NOT NULL,
      Volume  REAL,
      PRIMARY KEY (Date, Ticker)
    );
    """)
    conn.commit()
    conn.close()
    print(f"✂ Dropped & ✔ Created table '{table_name}' in {db_path}")

if __name__ == "__main__":
    initialize_database()
