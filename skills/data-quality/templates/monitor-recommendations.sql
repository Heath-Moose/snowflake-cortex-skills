-- Monitor Recommendations
-- Profiles all tables/columns in a schema to identify DMF coverage gaps
-- and generate ranked recommendations by criticality.
--
-- Replace <database> and <schema> with target database and schema names.
--
-- Data sources:
--   INFORMATION_SCHEMA.TABLES / COLUMNS   — column metadata and types
--   INFORMATION_SCHEMA.DATA_METRIC_FUNCTION_REFERENCES — existing DMF coverage
--   SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY — table access frequency (criticality)

-- Step 1: Get all tables and their columns with type classification
WITH all_columns AS (
    SELECT
        c.TABLE_NAME,
        c.COLUMN_NAME,
        c.DATA_TYPE,
        c.IS_NULLABLE,
        t.ROW_COUNT AS table_row_count,
        CASE
            WHEN c.DATA_TYPE IN ('DATE', 'TIMESTAMP_NTZ', 'TIMESTAMP_LTZ', 'TIMESTAMP_TZ', 'TIMESTAMP') THEN 'TIMESTAMP'
            WHEN c.COLUMN_NAME ILIKE '%_id' OR c.COLUMN_NAME ILIKE 'id' OR c.COLUMN_NAME ILIKE '%_key' THEN 'ID'
            WHEN c.DATA_TYPE IN ('NUMBER', 'FLOAT', 'DECIMAL', 'INTEGER', 'BIGINT', 'INT') THEN 'NUMERIC'
            WHEN c.DATA_TYPE IN ('VARCHAR', 'TEXT', 'STRING', 'CHAR') AND c.IS_NULLABLE = 'YES' THEN 'NULLABLE_VARCHAR'
            WHEN c.DATA_TYPE IN ('VARCHAR', 'TEXT', 'STRING', 'CHAR') THEN 'VARCHAR'
            ELSE 'OTHER'
        END AS column_class
    FROM <database>.INFORMATION_SCHEMA.COLUMNS c
    JOIN <database>.INFORMATION_SCHEMA.TABLES t
        ON c.TABLE_NAME = t.TABLE_NAME AND c.TABLE_SCHEMA = t.TABLE_SCHEMA
    WHERE c.TABLE_SCHEMA = '<schema>'
      AND t.TABLE_TYPE = 'BASE TABLE'
),

-- Step 2: Get existing DMF coverage per table/column
existing_dmfs AS (
    SELECT DISTINCT
        t.TABLE_NAME,
        r.METRIC_NAME,
        r.ARGUMENT_NAMES,
        r.SCHEDULE_STATUS
    FROM <database>.INFORMATION_SCHEMA.TABLES t,
    LATERAL (
        SELECT *
        FROM TABLE(INFORMATION_SCHEMA.DATA_METRIC_FUNCTION_REFERENCES(
            REF_ENTITY_NAME => '<database>.<schema>.' || t.TABLE_NAME,
            REF_ENTITY_DOMAIN => 'TABLE'
        ))
    ) r
    WHERE t.TABLE_SCHEMA = '<schema>'
      AND t.TABLE_TYPE = 'BASE TABLE'
),

-- Step 3: Table-level DMF count (to identify zero-coverage tables)
table_dmf_counts AS (
    SELECT
        t.TABLE_NAME,
        COUNT(DISTINCT r.METRIC_NAME) AS dmf_count
    FROM <database>.INFORMATION_SCHEMA.TABLES t
    LEFT JOIN existing_dmfs r ON t.TABLE_NAME = r.TABLE_NAME
    WHERE t.TABLE_SCHEMA = '<schema>'
      AND t.TABLE_TYPE = 'BASE TABLE'
    GROUP BY t.TABLE_NAME
),

-- Step 4: Access frequency from ACCOUNT_USAGE (last 90 days)
-- Note: requires ACCOUNT_USAGE access; gracefully returns 0 if unavailable
access_freq AS (
    SELECT
        SPLIT_PART(obj.value:objectName::STRING, '.', 3) AS table_name,
        COUNT(*) AS queries_90d
    FROM SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY ah,
        LATERAL FLATTEN(input => ah.base_objects_accessed) obj
    WHERE ah.query_start_time >= DATEADD('day', -90, CURRENT_TIMESTAMP())
      AND obj.value:objectName::STRING ILIKE '<database>.<schema>.%'
      AND obj.value:objectDomain::STRING = 'Table'
    GROUP BY 1
)

-- Final: Join everything and output recommendation profile
SELECT
    ac.TABLE_NAME,
    tc.dmf_count AS existing_dmf_count,
    COALESCE(af.queries_90d, 0) AS queries_90d,
    ac.COLUMN_NAME,
    ac.DATA_TYPE,
    ac.IS_NULLABLE,
    ac.column_class,
    ac.table_row_count,
    -- Is this column/metric combination already covered?
    CASE
        WHEN ac.column_class = 'TIMESTAMP'
         AND EXISTS (SELECT 1 FROM existing_dmfs d
                     WHERE d.TABLE_NAME = ac.TABLE_NAME
                       AND d.METRIC_NAME ILIKE '%FRESHNESS%') THEN TRUE
        WHEN ac.column_class = 'ID'
         AND EXISTS (SELECT 1 FROM existing_dmfs d
                     WHERE d.TABLE_NAME = ac.TABLE_NAME
                       AND d.METRIC_NAME ILIKE '%DUPLICATE%') THEN TRUE
        WHEN ac.column_class IN ('NULLABLE_VARCHAR', 'VARCHAR', 'NUMERIC')
         AND EXISTS (SELECT 1 FROM existing_dmfs d
                     WHERE d.TABLE_NAME = ac.TABLE_NAME
                       AND d.METRIC_NAME ILIKE '%NULL_COUNT%'
                       AND d.ARGUMENT_NAMES ILIKE '%' || ac.COLUMN_NAME || '%') THEN TRUE
        ELSE FALSE
    END AS already_covered,
    -- Recommended DMFs for this column type
    CASE ac.column_class
        WHEN 'TIMESTAMP'       THEN 'SNOWFLAKE.CORE.FRESHNESS'
        WHEN 'ID'              THEN 'SNOWFLAKE.CORE.DUPLICATE_COUNT, SNOWFLAKE.CORE.UNIQUE_COUNT'
        WHEN 'NULLABLE_VARCHAR'THEN 'SNOWFLAKE.CORE.NULL_COUNT, SNOWFLAKE.CORE.BLANK_COUNT'
        WHEN 'VARCHAR'         THEN 'SNOWFLAKE.CORE.NULL_COUNT'
        WHEN 'NUMERIC'         THEN 'SNOWFLAKE.CORE.NULL_COUNT'
        ELSE NULL
    END AS recommended_dmfs,
    -- Priority tier based on coverage and criticality
    CASE
        WHEN tc.dmf_count = 0 AND COALESCE(af.queries_90d, 0) >= 50 THEN 'CRITICAL'
        WHEN tc.dmf_count = 0 AND COALESCE(af.queries_90d, 0) < 50  THEN 'HIGH'
        WHEN tc.dmf_count > 0                                         THEN 'MEDIUM'
        ELSE 'LOW'
    END AS priority_tier
FROM all_columns ac
JOIN table_dmf_counts tc ON ac.TABLE_NAME = tc.TABLE_NAME
LEFT JOIN access_freq af ON UPPER(ac.TABLE_NAME) = UPPER(af.table_name)
WHERE ac.column_class != 'OTHER'
ORDER BY
    CASE
        WHEN tc.dmf_count = 0 AND COALESCE(af.queries_90d, 0) >= 50 THEN 1
        WHEN tc.dmf_count = 0 AND COALESCE(af.queries_90d, 0) < 50  THEN 2
        WHEN tc.dmf_count > 0                                         THEN 3
        ELSE 4
    END,
    COALESCE(af.queries_90d, 0) DESC,
    ac.TABLE_NAME,
    CASE ac.column_class WHEN 'TIMESTAMP' THEN 1 WHEN 'ID' THEN 2 ELSE 3 END;

/*
Columns returned:
  TABLE_NAME         — table name
  EXISTING_DMF_COUNT — number of DMFs already attached to this table
  QUERIES_90D        — access frequency (0 if ACCESS_HISTORY unavailable)
  COLUMN_NAME        — column name
  DATA_TYPE          — Snowflake data type
  IS_NULLABLE        — YES / NO
  COLUMN_CLASS       — TIMESTAMP | ID | NUMERIC | NULLABLE_VARCHAR | VARCHAR
  TABLE_ROW_COUNT    — approximate row count
  ALREADY_COVERED    — TRUE if a relevant DMF is already attached
  RECOMMENDED_DMFS   — comma-separated DMF name(s) to attach
  PRIORITY_TIER      — CRITICAL | HIGH | MEDIUM | LOW

Usage in monitor-recommendations.md:
  Filter WHERE already_covered = FALSE to find gaps.
  Group by TABLE_NAME for the per-table recommendation summary.
  Also run a ROW_COUNT DMF recommendation for every table regardless of column type.
*/
