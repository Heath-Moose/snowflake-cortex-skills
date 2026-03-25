-- Impact Analysis: Multi-Level Downstream (Primary: GET_LINEAGE)
-- Uses SNOWFLAKE.CORE.GET_LINEAGE() with distance for cascade. Replace <database>, <schema>, <table> before executing.
-- Use impact-analysis-multi-level-object-deps-fallback.sql only if this query fails or object should have dependents but returned 0 rows.

WITH lineage_raw AS (
    SELECT
        gl.TARGET_OBJECT_DATABASE AS dep_database,
        gl.TARGET_OBJECT_SCHEMA AS dep_schema,
        gl.TARGET_OBJECT_NAME AS dep_object,
        gl.TARGET_OBJECT_DOMAIN AS dep_type,
        gl.DISTANCE AS level
    FROM TABLE(SNOWFLAKE.CORE.GET_LINEAGE('<database>.<schema>.<table>', 'TABLE', 'DOWNSTREAM', 3)) gl
    WHERE gl.TARGET_OBJECT_DOMAIN IN ('TABLE', 'VIEW', 'DYNAMIC TABLE', 'MATERIALIZED VIEW', 'SEMANTIC_VIEW', 'STAGE')
),
dependency_tree AS (
    SELECT
        dep_database,
        dep_schema,
        dep_object,
        dep_type,
        level,
        '<database>.<schema>.<table>' || REPEAT(' → ... ', level - 1) || ' → ' || dep_database || '.' || dep_schema || '.' || dep_object AS lineage_path
    FROM lineage_raw
    WHERE level BETWEEN 1 AND 2
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
    dt.level,
    dt.dep_database || '.' || dt.dep_schema || '.' || dt.dep_object AS dependent_object,
    dt.dep_type AS object_type,
    COALESCE(u.query_count_7d, 0) AS queries_last_7_days,
    COALESCE(u.unique_users_7d, 0) AS unique_users_7_days,
    u.last_accessed,
    CASE
        WHEN COALESCE(u.query_count_7d, 0) > 50 THEN 'CRITICAL'
        WHEN /* SCHEMA_RISK_SCORING:dt.dep_schema */ IS NOT NULL THEN 'CRITICAL'
        WHEN dt.dep_type = 'DYNAMIC TABLE' THEN 'CRITICAL'
        WHEN COALESCE(u.query_count_7d, 0) BETWEEN 10 AND 50 THEN 'MODERATE'
        ELSE 'LOW'
    END AS risk_level,
    dt.lineage_path
FROM dependency_tree dt
LEFT JOIN usage_stats u ON u.object_name = dt.dep_database || '.' || dt.dep_schema || '.' || dt.dep_object
WHERE dt.level > 0
ORDER BY dt.level, risk_level, COALESCE(u.query_count_7d, 0) DESC;
