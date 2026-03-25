-- Preflight Check
-- Validates the environment before running any data quality workflow.
-- Checks: tables exist, DMFs are attached, DMF results are available.
-- This should be the FIRST query run for any DQ workflow to avoid query failures.

-- Replace <database> and <schema> with your target database and schema names

-- Step 1: Check if schema has tables
SELECT
    '<database>' AS database_name,
    '<schema>' AS schema_name,
    COUNT(*) AS table_count
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_CATALOG = '<database>'
  AND TABLE_SCHEMA = '<schema>'
  AND TABLE_TYPE = 'BASE TABLE';

-- Step 2: Check if DMFs are attached to any tables
-- Uses INFORMATION_SCHEMA.DATA_METRIC_FUNCTION_REFERENCES table function
-- Correct columns: REF_DATABASE_NAME, REF_SCHEMA_NAME, REF_ENTITY_NAME, METRIC_NAME, SCHEDULE, SCHEDULE_STATUS
WITH table_list AS (
    SELECT TABLE_NAME
    FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_CATALOG = '<database>'
      AND TABLE_SCHEMA = '<schema>'
      AND TABLE_TYPE = 'BASE TABLE'
),
dmf_refs AS (
    SELECT
        t.TABLE_NAME,
        r.METRIC_NAME,
        r.SCHEDULE,
        r.SCHEDULE_STATUS
    FROM table_list t,
    LATERAL (
        SELECT *
        FROM TABLE(INFORMATION_SCHEMA.DATA_METRIC_FUNCTION_REFERENCES(
            REF_ENTITY_NAME => '<database>.<schema>.' || t.TABLE_NAME,
            REF_ENTITY_DOMAIN => 'TABLE'
        ))
    ) r
)
SELECT
    '<database>' AS database_name,
    '<schema>' AS schema_name,
    COUNT(DISTINCT TABLE_NAME) AS tables_with_dmfs,
    COUNT(*) AS total_dmfs_attached,
    COUNT(DISTINCT METRIC_NAME) AS distinct_metrics,
    LISTAGG(DISTINCT METRIC_NAME, ', ') WITHIN GROUP (ORDER BY METRIC_NAME) AS metrics_used,
    LISTAGG(DISTINCT SCHEDULE_STATUS, ', ') WITHIN GROUP (ORDER BY SCHEDULE_STATUS) AS schedule_statuses
FROM dmf_refs;

-- Step 3: Check if DMF results exist (using SNOWFLAKE.LOCAL — the correct location)
-- NOTE: SNOWFLAKE.ACCOUNT_USAGE.DATA_QUALITY_MONITORING_RESULTS does NOT exist.
-- DMF results are available via SNOWFLAKE.LOCAL.DATA_QUALITY_MONITORING_RESULTS() table function.
-- Correct columns: MEASUREMENT_TIME, TABLE_NAME, TABLE_SCHEMA, TABLE_DATABASE,
--                  METRIC_NAME, METRIC_SCHEMA, METRIC_DATABASE, VALUE, REFERENCE_ID
WITH table_list AS (
    SELECT TABLE_NAME
    FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_CATALOG = '<database>'
      AND TABLE_SCHEMA = '<schema>'
      AND TABLE_TYPE = 'BASE TABLE'
    LIMIT 3  -- Check a sample of tables for speed
),
sample_results AS (
    SELECT
        t.TABLE_NAME,
        r.METRIC_NAME,
        r.VALUE,
        r.MEASUREMENT_TIME
    FROM table_list t,
    LATERAL (
        SELECT *
        FROM TABLE(SNOWFLAKE.LOCAL.DATA_QUALITY_MONITORING_RESULTS(
            REF_ENTITY_NAME => '<database>.<schema>.' || t.TABLE_NAME,
            REF_ENTITY_DOMAIN => 'TABLE'
        ))
    ) r
    LIMIT 10
)
SELECT
    '<database>' AS database_name,
    '<schema>' AS schema_name,
    COUNT(*) AS results_available,
    COUNT(DISTINCT TABLE_NAME) AS tables_with_results,
    MIN(MEASUREMENT_TIME) AS earliest_result,
    MAX(MEASUREMENT_TIME) AS latest_result,
    CASE
        WHEN COUNT(*) = 0 THEN 'NO_RESULTS - DMFs may not have run yet. Wait 1-2 minutes.'
        WHEN COUNT(DISTINCT MEASUREMENT_TIME) = 1 THEN 'LIMITED - Only 1 measurement. Regression detection needs 2+.'
        ELSE 'READY - Sufficient data for all workflows.'
    END AS readiness_status
FROM sample_results;

/*
Interpretation:

Step 1 - table_count:
  0     → Schema is empty or doesn't exist. Stop here.
  > 0   → Proceed to Step 2.

Step 2 - total_dmfs_attached:
  0     → No DMFs attached. User needs to set up DMF monitoring first.
  > 0   → DMFs are configured. Check schedule_statuses for STARTED/SUSPENDED.

Step 3 - readiness_status:
  NO_RESULTS  → DMFs haven't executed yet. Wait and retry.
  LIMITED     → Only 1 run available. Health check works, regression does not.
  READY       → All workflows available.

IMPORTANT - Correct View/Function Locations:
  DMF Results:      SNOWFLAKE.LOCAL.DATA_QUALITY_MONITORING_RESULTS() (table function)
  DMF References:   INFORMATION_SCHEMA.DATA_METRIC_FUNCTION_REFERENCES() (table function)
                    or SNOWFLAKE.ACCOUNT_USAGE.DATA_METRIC_FUNCTION_REFERENCES (view)
  DMF Expectations: SNOWFLAKE.ACCOUNT_USAGE.DATA_METRIC_FUNCTION_EXPECTATIONS (view)
  DMF Usage/Cost:   SNOWFLAKE.ACCOUNT_USAGE.DATA_QUALITY_MONITORING_USAGE_HISTORY (view)

  *** SNOWFLAKE.ACCOUNT_USAGE.DATA_QUALITY_MONITORING_RESULTS does NOT exist ***
  Always use SNOWFLAKE.LOCAL.DATA_QUALITY_MONITORING_RESULTS() for metric values.
*/
