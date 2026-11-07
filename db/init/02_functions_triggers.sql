-- Trigger Audit & Window Function Gap Detection

-- 1. Detector config saves
CREATE TABLE IF NOT EXISTS ops.detector_config (
    config_key      VARCHAR(50)     PRIMARY KEY,
    threshold       NUMERIC(10, 4)  NOT NULL,
    window_size     INTEGER         NOT NULL,
    updated_at_ms   BIGINT          NOT NULL
);
-- Audit Log
CREATE TABLE IF NOT EXISTS ops.config_audit_log (
    log_id          BIGSERIAL       PRIMARY KEY,
    config_key      VARCHAR(50)     NOT NULL,
    old_threshold   NUMERIC(10, 4),
    new_threshold   NUMERIC(10, 4),
    changed_at_ms   BIGINT          NOT NULL
);

INSERT INTO ops.detector_config(config_key, threshold, window_size, updated_at_ms)
VALUES ('z_score_btc', 3.0000, 60, EXTRACT(EPOCH FROM NOW()) * 1000)
ON CONFLICT (config_key) DO NOTHING;

-- 2. TRIGGER AUDIT: Insert into ops.config_audit_log when UPDATE
CREATE OR REPLACE FUNCTION ops.fn_audit_detector_config()
RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO ops.config_audit_log(config_key, old_threshold, new_threshold, changed_at_ms)
    VALUES (OLD.config_key, OLD.threshold, NEW.threshold, EXTRACT(EPOCH FROM NOW()) * 1000);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER trg_detector_config_audit
AFTER UPDATE ON ops.detector_config
FOR EACH ROW 
WHEN (OLD.threshold IS DISTINCT FROM NEW.threshold)
EXECUTE FUNCTION ops.fn_audit_detector_config();

-- 3. WINDOW FUNCTION: Find missing trade_id's gap
CREATE OR REPLACE FUNCTION ops.fn_find_trade_gaps(p_symbol VARCHAR)
RETURNS TABLE (
    prev_trade_id   BIGINT,
    curr_trade_id   BIGINT,
    missing_count   BIGINT
) AS $$
BEGIN
    RETURN QUERY
    WITH trade_lag AS (
        SELECT
            trade_id AS current_id,
            LAG(trade_id) OVER (ORDER BY trade_time_ms) AS previous_id
        FROM raw.trades
        WHERE symbol = p_symbol
    )
    SELECT
        previous_id,
        current_id,
        (current_id - previous_id - 1)
    FROM trade_lag
    WHERE current_id - previous_id > 1
    ORDER BY current_id ASC;
END;
$$ LANGUAGE plpgsql;

--4. STORED PROCEDURE: Bảo trì và dọn dẹp dữ liệu raw.trades quá hạn

CREATE OR REPLACE PROCEDURE ops.sp_maintain_raw_trades(p_retention_days INT)
AS $$
DECLARE
    v_cutoff_ms  BIGINT;
    v_deleted    BIGINT := 0;
BEGIN
    IF p_retention_days <= 0 THEN
        RAISE EXCEPTION 'retention_days must be greater than 0, got: %', p_retention_days;
    END IF;
    v_cutoff_ms := (EXTRACT(EPOCH FROM (NOW() - (p_retention_days || ' days')::INTERVAL)) * 1000)::BIGINT;
    
    DELETE FROM raw.trades
    WHERE trade_time_ms < v_cutoff_ms;

    GET DIAGNOSTICS v_deleted = ROW_COUNT;
    RAISE NOTICE 'Maintenance completed: deleted % old trades before cutoff_ms=%', v_deleted, v_cutoff_ms;
END;
$$ LANGUAGE plpgsql;