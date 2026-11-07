"""Integration tests for Advanced PostgreSQL features: Triggers, Functions, and Procedures."""

import os
import psycopg
import pytest

DB_CONN = os.getenv(
    "TEST_DATABASE_URL",
    "host=localhost port=5432 dbname=crypto_dw user=crypto_user password=crypto_secret_password_2026",
)


def test_trigger_audit_detector_config():
    """Verify that updating detector_config triggers an audit log record."""
    with psycopg.connect(DB_CONN) as conn:
        with conn.cursor() as cur:
            # 1. Reset baseline configuration
            cur.execute(
                """
                INSERT INTO ops.detector_config (config_key, threshold, window_size, updated_at_ms)
                VALUES ('test_key', 3.0000, 60, 1700000000000)
                ON CONFLICT (config_key) DO UPDATE SET threshold = 3.0000;
                """
            )
            cur.execute("DELETE FROM ops.config_audit_log WHERE config_key = 'test_key';")

            # 2. Perform UPDATE to fire trigger
            cur.execute(
                "UPDATE ops.detector_config SET threshold = 2.2500 WHERE config_key = 'test_key';"
            )

            # 3. Verify audit log captured OLD and NEW values
            cur.execute(
                """
                SELECT old_threshold, new_threshold
                FROM ops.config_audit_log
                WHERE config_key = 'test_key'
                ORDER BY log_id DESC
                LIMIT 1;
                """
            )
            row = cur.fetchone()
            assert row is not None
            assert float(row[0]) == 3.0000
            assert float(row[1]) == 2.2500

            # 4. Clean up test record
            cur.execute("DELETE FROM ops.detector_config WHERE config_key = 'test_key';")
            cur.execute("DELETE FROM ops.config_audit_log WHERE config_key = 'test_key';")


def test_window_function_find_trade_gaps():
    """Verify that ops.fn_find_trade_gaps detects missing trade IDs."""
    with psycopg.connect(DB_CONN) as conn:
        with conn.cursor() as cur:
            symbol = "TEST_GAP_COIN"
            # Insert 3 trades: 100, 101, 105 (gap of 3: 102, 103, 104)
            cur.execute(
                """
                INSERT INTO raw.trades (symbol, trade_id, price, quantity, trade_time_ms, is_buyer_maker, ingested_at_ms)
                VALUES 
                    (%s, 100, 50000.0, 0.1, 1000, false, 1000),
                    (%s, 101, 50000.0, 0.1, 2000, false, 2000),
                    (%s, 105, 50000.0, 0.1, 3000, false, 3000);
                """,
                (symbol, symbol, symbol),
            )

            # Call gap detection function
            cur.execute("SELECT prev_trade_id, curr_trade_id, missing_count FROM ops.fn_find_trade_gaps(%s);", (symbol,))
            gaps = cur.fetchall()

            assert len(gaps) == 1
            assert gaps[0][0] == 101
            assert gaps[0][1] == 105
            assert gaps[0][2] == 3

            # Clean up
            cur.execute("DELETE FROM raw.trades WHERE symbol = %s;", (symbol,))


def test_stored_procedure_maintain_raw_trades():
    """Verify that ops.sp_maintain_raw_trades deletes expired trades and validates inputs."""
    import time

    with psycopg.connect(DB_CONN) as conn:
        with conn.cursor() as cur:
            symbol = "TEST_MAINTAIN"
            # Insert 1 old trade (30 days ago) and 1 fresh trade (now)
            now_ms = int(time.time() * 1000)
            old_ms = now_ms - (30 * 24 * 60 * 60 * 1000)

            cur.execute(
                """
                INSERT INTO raw.trades (symbol, trade_id, price, quantity, trade_time_ms, is_buyer_maker, ingested_at_ms)
                VALUES 
                    (%s, 99901, 50000.0, 0.1, %s, false, %s),
                    (%s, 99902, 50000.0, 0.1, %s, false, %s);
                """,
                (symbol, old_ms, old_ms, symbol, now_ms, now_ms),
            )

            # Call stored procedure with retention 7 days -> old trade should be deleted
            cur.execute("CALL ops.sp_maintain_raw_trades(7);")

            # Check that only the fresh trade remains
            cur.execute("SELECT trade_id FROM raw.trades WHERE symbol = %s;", (symbol,))
            rows = cur.fetchall()
            assert len(rows) == 1
            assert rows[0][0] == 99902

            # Clean up
            cur.execute("DELETE FROM raw.trades WHERE symbol = %s;", (symbol,))

            # Verify invalid retention_days <= 0 raises exception
            with pytest.raises(psycopg.Error):
                cur.execute("CALL ops.sp_maintain_raw_trades(0);")



