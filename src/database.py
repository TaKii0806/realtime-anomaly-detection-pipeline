import sqlite3

DB_PATH = "market_data.db"

def init_db():
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS transactions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            price REAL NOT NULL,
            timestamp TEXT NOT NULL
        )
        """)

def insert_transaction(symbol:str, price:float, timestamp:str):
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute("INSERT INTO transactions(symbol, price, timestamp) VALUES (?, ?, ?)", (symbol, price, timestamp))

def get_recent_transaction(limit:int=5):
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM transactions ORDER BY id DESC LIMIT ?",(limit,))
        rows = cursor.fetchall()
        return rows
if __name__ == "__main__":
    init_db()

    print("\n--5 LATEST TRANSACTION IN DATABASE--")
    recent_data = get_recent_transaction()
    for row in recent_data:
        print(row)