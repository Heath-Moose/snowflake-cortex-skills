-- Impact Analysis: Downstream Dependencies with Risk Scoring (Primary: GET_LINEAGE)
-- Uses SNOWFLAKE.CORE.GET_LINEAGE() for object + data-movement lineage (no account admin).
-- Replace <database>, <schema>, <table> with actual values BEFORE executing.
-- Use impact-analysis-object-deps-fallback.sql only if this query fails (e.g. privilege error) or object should have dependents but returned 0 rows.

WITH lineage_raw AS (
    SELECT
        gl.TARGET_OBJECT_DATABASE AS dep_database,
        gl.TARGET_OBJECT_SCHEMA AS dep_schema,
        gl.TARGET_OBJECT_NAME AS dep_object,
        gl.TARGET_OBJECT_DOMAIN AS dep_type,
        gl.DISTANCE
    FROM TABLE(SNOWFLAKE.CORE.GET_LINEAGE('<database>.<schema>.<table>', 'TABLE', 'DOWNSTREAM', 5)) gl
    WHERE gl.TARGET_OBJECT_DOMAIN IN ('TABLE', 'VIEW', 'DYNAMIC TABLE', 'MATERIALIZED VIEW', 'SEMANTIC_VIEW', 'STAGE')
),
downstream_deps AS (
    -- One row per dependent (min distance if multiple edges)
    SELECT
        dep_database,
        dep_schema,
        dep_object,
        dep_type,
        MIN(DISTANCE) AS distance
    FROM lineage_raw
    GROUP BY dep_database, dep_schema, dep_object, dep_type
),
usage_stats AS (
    SELECT
        base.value:objectName::STRING AS object_name,
        COUNT(DISTINCT ah.query_id) AS query_count_7d,
        COUNT(DISTINCT ah.user_name) AS unique_users_7d,
        MAX(ah.query_start_time) AS last_accessed
    FROM SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY ah,
    LATERAL FLATTEN(input => ah.base_objects_accessed) AS base
    WHERE ah.query_start_time >= DATEADD(day, -7, CURRENT_TIMESTAMP())
    GROUP BY 1
)
SELECT
    d.dep_database || '.' || d.dep_schema || '.' || d.dep_object AS dependent_object,
    d.dep_type AS object_type,
    COALESCE(u.query_count_7d, 0) AS queries_last_7_days,
    COALESCE(u.unique_users_7d, 0) AS unique_users_7_days,
    u.last_accessed,
    0 AS downstream_dependents,
    CASE
        WHEN COALESCE(u.query_count_7d, 0) > 50 THEN 'CRITICAL'
        WHEN /* SCHEMA_RISK_SCORING:d.dep_schema */ IS NOT NULL THEN 'CRITICAL'
        WHEN d.dep_type = 'DYNAMIC TABLE' THEN 'CRITICAL'
        WHEN COALESCE(u.query_count_7d, 0) BETWEEN 10 AND 50 THEN 'MODERATE'
        ELSE 'LOW'
    END AS risk_level,
    d.distance,
    NULL AS dependency_type
FROM downstream_deps d
LEFT JOIN usage_stats u
    ON u.object_name = d.dep_database || '.' || d.dep_schema || '.' || d.dep_object
ORDER BY
    CASE
        WHEN COALESCE(u.query_count_7d, 0) > 50 THEN 1
        WHEN COALESCE(u.query_count_7d, 0) BETWEEN 10 AND 50 THEN 2
        ELSE 3
    END,
    COALESCE(u.query_count_7d, 0) DESC;
