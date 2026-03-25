-- DQ Coverage Gaps Summary
-- Computes monitoring coverage % and identifies critical unmonitored tables.
--
-- Replace <database> and <schema> with target database and schema names.
--
-- Data sources:
--   INFORMATION_SCHEMA.TABLES                            — total table count
--   INFORMATION_SCHEMA.DATA_METRIC_FUNCTION_REFERENCES  — DMF attachment
--   SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY               — access frequency
--   SNOWFLAKE.ACCOUNT_USAGE.OBJECT_DEPENDENCIES          — downstream count

-- Step 1: All base tables in scope
WITH all_tables AS (
    SELECT
        TABLE_NAME,
        ROW_COUNT,
        CREATED,
        LAST_ALTERED
    FROM <database>.INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = '<schema>'
      AND TABLE_TYPE = 'BASE TABLE'
),

-- Step 2: Tables with at least one DMF attached
monitored_tables AS (
    SELECT DISTINCT t.TABLE_NAME
    FROM all_tables t,
    LATERAL (
        SELECT COUNT(*) AS dmf_cnt
        FROM TABLE(INFORMATION_SCHEMA.DATA_METRIC_FUNCTION_REFERENCES(
            REF_ENTITY_NAME => '<database>.<schema>.' || t.TABLE_NAME,
            REF_ENTITY_DOMAIN => 'TABLE'
        ))
        HAVING dmf_cnt > 0
    ) d
),

-- Step 3: Access frequency (last 90 days)
access_freq AS (
    SELECT
        SPLIT_PART(obj.value:objectName::STRING, '.', 3) AS table_name,
        COUNT(*) AS queries_90d,
        COUNT(DISTINCT ah.user_name) AS unique_users_90d
    FROM SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY ah,
        LATERAL FLATTEN(input => ah.base_objects_accessed) obj
    WHERE ah.query_start_time >= DATEADD('day', -90, CURRENT_TIMESTAMP())
      AND obj.value:objectName::STRING ILIKE '<database>.<schema>.%'
      AND obj.value:objectDomain::STRING = 'Table'
    GROUP BY 1
),

-- Step 4: Downstream dependency count
downstream_counts AS (
    SELECT
        REFERENCED_OBJECT_NAME AS table_name,
        COUNT(DISTINCT REFERENCING_OBJECT_NAME) AS downstream_count
    FROM SNOWFLAKE.ACCOUNT_USAGE.OBJECT_DEPENDENCIES
    WHERE REFERENCED_DATABASE = '<database>'
      AND REFERENCED_SCHEMA = '<schema>'
      AND REFERENCED_OBJECT_DOMAIN = 'Table'
    GROUP BY 1
)

-- Final: Unified coverage view
SELECT
    t.TABLE_NAME,
    CASE WHEN m.TABLE_NAME IS NOT NULL THEN 'MONITORED' ELSE 'UNMONITORED' END AS monitoring_status,
    COALESCE(af.queries_90d, 0) AS queries_90d,
    COALESCE(af.unique_users_90d, 0) AS unique_users_90d,
    COALESCE(dc.downstream_count, 0) AS downstream_count,
    t.ROW_COUNT AS approx_row_count,
    t.LAST_ALTERED,
    -- Risk level for unmonitored tables
    CASE
        WHEN m.TABLE_NAME IS NULL AND COALESCE(af.queries_90d, 0) >= 100 THEN 'CRITICAL'
        WHEN m.TABLE_NAME IS NULL AND COALESCE(af.queries_90d, 0) >= 20  THEN 'HIGH'
        WHEN m.TABLE_NAME IS NULL AND COALESCE(dc.downstream_count, 0) >= 3 THEN 'HIGH'
        WHEN m.TABLE_NAME IS NULL THEN 'MEDIUM'
        ELSE 'MONITORED'
    END AS unmonitored_risk
FROM all_tables t
LEFT JOIN monitored_tables m ON t.TABLE_NAME = m.TABLE_NAME
LEFT JOIN access_freq af ON UPPER(t.TABLE_NAME) = UPPER(af.table_name)
LEFT JOIN downstream_counts dc ON UPPER(t.TABLE_NAME) = UPPER(dc.table_name)
ORDER BY
    CASE WHEN m.TABLE_NAME IS NULL THEN 0 ELSE 1 END,  -- unmonitored first
    COALESCE(af.queries_90d, 0) DESC;

-- Coverage summary statistics
SELECT
    COUNT(*) AS total_tables,
    COUNT(DISTINCT m.TABLE_NAME) AS monitored_tables,
    COUNT(*) - COUNT(DISTINCT m.TABLE_NAME) AS unmonitored_tables,
    ROUND(COUNT(DISTINCT m.TABLE_NAME) * 100.0 / NULLIF(COUNT(*), 0), 1) AS coverage_pct
FROM <database>.INFORMATION_SCHEMA.TABLES t
LEFT JOIN (
    SELECT DISTINCT t2.TABLE_NAME
    FROM <database>.INFORMATION_SCHEMA.TABLES t2,
    LATERAL (
        SELECT COUNT(*) AS c
        FROM TABLE(INFORMATION_SCHEMA.DATA_METRIC_FUNCTION_REFERENCES(
            REF_ENTITY_NAME => '<database>.<schema>.' || t2.TABLE_NAME,
            REF_ENTITY_DOMAIN => 'TABLE'
        ))
        HAVING c > 0
    ) d
    WHERE t2.TABLE_SCHEMA = '<schema>' AND t2.TABLE_TYPE = 'BASE TABLE'
) m ON t.TABLE_NAME = m.TABLE_NAME
WHERE t.TABLE_SCHEMA = '<schema>'
  AND t.TABLE_TYPE = 'BASE TABLE';

/*
Query 1 columns:
  TABLE_NAME           — table name
  MONITORING_STATUS    — MONITORED | UNMONITORED
  QUERIES_90D          — access queries in last 90 days (0 if ACCESS_HISTORY unavailable)
  UNIQUE_USERS_90D     — distinct users who accessed the table
  DOWNSTREAM_COUNT     — objects that depend on this table
  APPROX_ROW_COUNT     — row count from INFORMATION_SCHEMA
  LAST_ALTERED         — last DDL change timestamp
  UNMONITORED_RISK     — CRITICAL | HIGH | MEDIUM | MONITORED

Query 2 columns:
  TOTAL_TABLES         — total tables in schema
  MONITORED_TABLES     — tables with ≥1 DMF
  UNMONITORED_TABLES   — tables with 0 DMFs
  COVERAGE_PCT         — percentage of tables monitored
*/
