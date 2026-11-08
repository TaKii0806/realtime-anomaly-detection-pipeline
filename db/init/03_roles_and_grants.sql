CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS marts;

-- Create 3 roles
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'app_writer') THEN
        CREATE ROLE app_writer WITH LOGIN PASSWORD 'appwriter69';
    END IF;
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'dbt_runner') THEN
        CREATE ROLE dbt_runner WITH LOGIN PASSWORD 'dbtrunner96';
    END IF;
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'bi_reader') THEN
        CREATE ROLE bi_reader WITH LOGIN PASSWORD 'bireader6996';
    END IF;
END;
$$;

GRANT USAGE ON SCHEMA raw, ops TO app_writer;
GRANT SELECT, INSERT ON ALL TABLES IN SCHEMA raw, ops TO app_writer;
ALTER DEFAULT PRIVILEGES IN SCHEMA raw, ops GRANT SELECT, INSERT ON TABLES TO app_writer;

GRANT USAGE ON SCHEMA raw, ops TO dbt_runner;
GRANT SELECT ON ALL TABLES IN SCHEMA raw, ops TO dbt_runner;
ALTER DEFAULT PRIVILEGES IN SCHEMA raw, ops GRANT SELECT ON TABLES TO dbt_runner;

GRANT ALL ON SCHEMA staging, marts TO dbt_runner;
GRANT ALL ON ALL TABLES IN SCHEMA staging, marts TO dbt_runner;
ALTER DEFAULT PRIVILEGES IN SCHEMA staging, marts GRANT ALL ON TABLES TO dbt_runner;

GRANT USAGE ON SCHEMA marts TO bi_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA marts TO bi_reader;
ALTER DEFAULT PRIVILEGES IN SCHEMA marts GRANT SELECT ON TABLES TO bi_reader;