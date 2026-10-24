import sqlite3

DB_PATH = "market_data.db"

def get_connection():
    conn = sqlite3.connect(DB_PATH, timeout=10.0)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    return conn

def init_db():
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS transactions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            price REAL NOT NULL,
            volume REAL NOT NULL DEFAULT 0.0,
            timestamp TEXT NOT NULL
        )
        """)
        cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_transactions_timestamp
        ON transactions(timestamp)
        """)

def insert_transactions(records):
    if not records: 
        return
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.executemany("INSERT INTO transactions(symbol, price, volume, timestamp) VALUES (?, ?, ?, ?)", records)

def get_recent_transaction(last_id: int, limit:int=50):
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM transactions WHERE id > ? ORDER BY id ASC LIMIT ?",(last_id, limit))
        return cursor.fetchall()

def latest_50_transactions():
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM transactions ORDER BY id DESC LIMIT ?", (50,))
        return cursor.fetchall()[::-1]

def get_latest_id() -> int:
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT MAX(id) FROM transactions")
        val = cursor.fetchone()
        if val and val[0] is None:
            return val[0]
        return 0
if __name__ == "__main__":
    init_db()

    print("\n--5 LATEST TRANSACTION IN DATABASE--")
    recent_data = get_recent_transaction()
    for row in recent_data:
        print(row)