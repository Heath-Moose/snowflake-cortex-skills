-- Ad-Hoc Column Quality Assessment
-- Uses SNOWFLAKE.CORE.* system DMFs called inline — no pre-attached DMFs required.
-- Replace <db>, <schema>, <table>, <col> with actual values before executing.

-- ============================================================================
-- IMPORTANT: _PERCENT DMF functions return values on the 0–100 scale.
-- 0.7968 means 0.7968%, NOT 79.68%. Always cross-validate:
--   NULL_COUNT / total_rows should equal NULL_PERCENT / 100
-- ============================================================================


-- ── 1. FRESHNESS ─────────────────────────────────────────────────────────────
-- Use for DATE, TIMESTAMP_*, TIMESTAMP_LTZ, TIMESTAMP_TZ columns.
-- Returns seconds since the most recent non-NULL value.

SELECT
    '<table>'            AS table_name,
    '<col>'              AS column_name,
    'FRESHNESS'          AS metric,
    SNOWFLAKE.CORE.FRESHNESS(
        SELECT <col> FROM <db>.<schema>.<table>
    )                    AS value_seconds,
    ROUND(SNOWFLAKE.CORE.FRESHNESS(
        SELECT <col> FROM <db>.<schema>.<table>
    ) / 86400, 2)        AS value_days
;


-- ── 2. NULL ANALYSIS ─────────────────────────────────────────────────────────
-- Use for all columns EXCEPT BOOLEAN (DMFs not supported on BOOLEAN).
-- NULL_PERCENT is on 0–100 scale (e.g., 5.0 = 5%).

SELECT
    '<table>'                AS table_name,
    '<col>'                  AS column_name,
    SNOWFLAKE.CORE.NULL_COUNT(
        SELECT <col> FROM <db>.<schema>.<table>
    )                        AS null_count,
    SNOWFLAKE.CORE.NULL_PERCENT(
        SELECT <col> FROM <db>.<schema>.<table>
    )                        AS null_pct       -- 0–100 scale; 5.0 means 5%
;

-- BOOLEAN fallback (no DMF support — use raw SQL):
SELECT
    COUNT(*) - COUNT(<col>) AS null_count,
    (COUNT(*) - COUNT(<col>)) * 100.0 / NULLIF(COUNT(*), 0) AS null_pct
FROM <db>.<schema>.<table>
;


-- ── 3. BLANK DETECTION ───────────────────────────────────────────────────────
-- Use for VARCHAR / TEXT columns only.
-- BLANK_PERCENT is on 0–100 scale.

SELECT
    '<table>'                AS table_name,
    '<col>'                  AS column_name,
    SNOWFLAKE.CORE.BLANK_COUNT(
        SELECT <col> FROM <db>.<schema>.<table>
    )                        AS blank_count,
    SNOWFLAKE.CORE.BLANK_PERCENT(
        SELECT <col> FROM <db>.<schema>.<table>
    )                        AS blank_pct      -- 0–100 scale; 1.5 means 1.5%
;


-- ── 4. UNIQUENESS / DUPLICATE DETECTION ──────────────────────────────────────
-- Use for Critical columns (ID columns, primary keys, deduplication keys).
-- Indicates cardinality and integrity of key columns.

SELECT
    '<table>'                AS table_name,
    '<col>'                  AS column_name,
    SNOWFLAKE.CORE.DUPLICATE_COUNT(
        SELECT <col> FROM <db>.<schema>.<table>
    )                        AS duplicate_count,
    SNOWFLAKE.CORE.UNIQUE_COUNT(
        SELECT <col> FROM <db>.<schema>.<table>
    )                        AS unique_count
;


-- ── 5. ACCEPTED VALUES (Ad-Hoc) ─────────────────────────────────────────────
-- Use SYSTEM$DATA_METRIC_SCAN to run ACCEPTED_VALUES without attaching a DMF.
-- Returns the actual violating rows, not just a count.
-- Supports: comparison operators, logical operators, LIKE, RLIKE, IN, IS [NOT] NULL.

-- Categorical: find rows where status is not in the allowed set
SELECT *
FROM TABLE(SYSTEM$DATA_METRIC_SCAN(
    REF_ENTITY_NAME  => '<db>.<schema>.<table>',
    METRIC_NAME      => 'SNOWFLAKE.CORE.ACCEPTED_VALUES',
    ARGUMENT_NAME    => '<col>',
    ARGUMENT_EXPRESSION => '<col> IN (''Value1'', ''Value2'', ''Value3'')'
));

-- Numeric range: find rows where price is not positive
SELECT *
FROM TABLE(SYSTEM$DATA_METRIC_SCAN(
    REF_ENTITY_NAME  => '<db>.<schema>.<table>',
    METRIC_NAME      => 'SNOWFLAKE.CORE.ACCEPTED_VALUES',
    ARGUMENT_NAME    => '<col>',
    ARGUMENT_EXPRESSION => '<col> > 0'
));

-- Email format (RLIKE): find rows with invalid email format
SELECT *
FROM TABLE(SYSTEM$DATA_METRIC_SCAN(
    REF_ENTITY_NAME  => '<db>.<schema>.<table>',
    METRIC_NAME      => 'SNOWFLAKE.CORE.ACCEPTED_VALUES',
    ARGUMENT_NAME    => '<col>',
    ARGUMENT_EXPRESSION => '<col> RLIKE ''^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$'''
));


-- ── 6. NUMERIC STATISTICS ─────────────────────────────────────────────────────
-- Use for NUMBER, FLOAT, DECIMAL columns only.
-- Helps detect anomalous distributions or broken numeric ranges.

SELECT
    '<table>'                AS table_name,
    '<col>'                  AS column_name,
    SNOWFLAKE.CORE.AVG(
        SELECT <col> FROM <db>.<schema>.<table>
    )                        AS avg_value,
    SNOWFLAKE.CORE.MIN(
        SELECT <col> FROM <db>.<schema>.<table>
    )                        AS min_value,
    SNOWFLAKE.CORE.MAX(
        SELECT <col> FROM <db>.<schema>.<table>
    )                        AS max_value,
    SNOWFLAKE.CORE.STDDEV(
        SELECT <col> FROM <db>.<schema>.<table>
    )                        AS stddev_value
;


-- ── 7. LISTING CONTEXT: Objects Granted to a Share ────────────────────────────
-- Use for provider listings to enumerate tables/views included in the listing.

SHOW GRANTS TO SHARE <share_name>;

-- Then query table metadata from the underlying database (after parsing share objects):
SELECT TABLE_NAME, TABLE_TYPE, ROW_COUNT, LAST_ALTERED
FROM <underlying_db>.INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA NOT IN ('INFORMATION_SCHEMA')
  AND TABLE_NAME NOT LIKE '%_HISTORY'
  AND TABLE_NAME NOT LIKE '%_PIT'
ORDER BY TABLE_NAME
;


-- ── 8. CONSUMER LISTING: Imported Databases ──────────────────────────────────
-- For consumers, the listing appears as a shared/imported database.
-- Use SHOW DATABASES to identify which databases came from shares.

SHOW DATABASES;
-- Look for rows where ORIGIN is not empty — these are imported from listings.

-- Access columns from an imported listing database normally:
SELECT TABLE_NAME, TABLE_TYPE, ROW_COUNT
FROM <imported_db>.INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA NOT IN ('INFORMATION_SCHEMA')
ORDER BY TABLE_NAME
;


-- ── 9. DMF ATTACH TEMPLATES (Continuous Monitoring Setup) ─────────────────────
-- If user wants to switch from ad-hoc to continuous monitoring.
-- Requires OWNERSHIP or ALTER privilege on the table.
-- ⚠️ CONFIRM with user before executing — state-changing operation.

-- Set schedule (TRIGGER_ON_CHANGES or a cron, e.g., '60 MINUTE')
ALTER TABLE <db>.<schema>.<table>
  SET DATA_METRIC_SCHEDULE = 'TRIGGER_ON_CHANGES';

-- Attach null count to a VARCHAR column
ALTER TABLE <db>.<schema>.<table>
  ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.NULL_COUNT ON (<column>);

-- Attach blank percent to a VARCHAR column
ALTER TABLE <db>.<schema>.<table>
  ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.BLANK_PERCENT ON (<column>);

-- Attach duplicate count to an ID/key column
ALTER TABLE <db>.<schema>.<table>
  ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.DUPLICATE_COUNT ON (<id_column>);

-- Attach freshness to a timestamp column
ALTER TABLE <db>.<schema>.<table>
  ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.FRESHNESS ON (<timestamp_column>);


/*
Quick Reference — SNOWFLAKE.CORE.* System DMFs:
  FRESHNESS(<col>)        → seconds since latest non-NULL value (timestamp/date columns)
  NULL_COUNT(<col>)       → absolute count of NULL rows
  NULL_PERCENT(<col>)     → % NULL rows (0–100 scale)
  BLANK_COUNT(<col>)      → count of rows with empty string '' (VARCHAR only)
  BLANK_PERCENT(<col>)    → % blank rows (0–100 scale)
  DUPLICATE_COUNT(<col>)  → count of rows with a non-unique value
  UNIQUE_COUNT(<col>)     → count of distinct non-NULL values
  AVG(<col>)              → average (numeric only)
  MIN(<col>)              → minimum (numeric/date)
  MAX(<col>)              → maximum (numeric/date)
  STDDEV(<col>)           → standard deviation (numeric only)
  ACCEPTED_VALUES(<col>, <col> -> <expr>) → count of rows failing a Boolean check (via ALTER TABLE)
    Ad-hoc: use SYSTEM$DATA_METRIC_SCAN with ARGUMENT_EXPRESSION to get violating rows

Note: Not all DMFs work on all data types. Boolean columns fall back to SQL.
Listing consumers may have read-only access — DDL operations (ALTER TABLE) will fail.
*/
