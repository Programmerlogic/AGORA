import sqlite3

import pandas as pd


def populate_database_chunked(
    csv_file: str = "log.csv",
    db_name: str = "agora_transactions.db",
    chunk_size: int = 100000,
) -> None:
    """
    Read a large CSV in chunks and migrate it into SQLite.
    """
    print(f"Connecting to SQLite database: {db_name}")
    conn = sqlite3.connect(db_name)

    reader = pd.read_csv(csv_file, chunksize=chunk_size)
    first_chunk = True
    total_rows = 0

    print("Starting chunked migration...")
    for chunk in reader:
        if first_chunk:
            chunk.to_sql("transactions", conn, if_exists="replace", index=False)
            first_chunk = False
        else:
            chunk.to_sql("transactions", conn, if_exists="append", index=False)

        total_rows += len(chunk)
        print(f"Successfully migrated {total_rows} rows...")

    # Indexes speed up both agent tools and chat-style analytics.
    print("Creating database indexes for fast lookups...")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_nameOrig ON transactions(nameOrig)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_nameDest ON transactions(nameDest)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_type_amount ON transactions(type, amount)")

    conn.commit()
    print(f"\nDONE! Migrated {total_rows} rows into '{db_name}'.")
    conn.close()


if __name__ == "__main__":
    populate_database_chunked()
