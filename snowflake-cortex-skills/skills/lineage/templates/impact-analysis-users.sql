-- Impact Analysis: Affected Users (Primary: GET_LINEAGE for downstream list)
-- Uses SNOWFLAKE.CORE.GET_LINEAGE() for dependents. Replace <database>, <schema>, <table> before executing.
-- Use impact-analysis-users-object-deps-fallback.sql only if this query fails or object should have dependents but returned 0 rows.

WITH lineage_raw AS (
    SELECT
        gl.TARGET_OBJECT_DATABASE AS dep_database,
        gl.TARGET_OBJECT_SCHEMA AS dep_schema,
        gl.TARGET_OBJECT_NAME AS dep_object,
        gl.TARGET_OBJECT_DOMAIN AS object_type
    FROM TABLE(SNOWFLAKE.CORE.GET_LINEAGE('<database>.<schema>.<table>', 'TABLE', 'DOWNSTREAM', 5)) gl
    WHERE gl.TARGET_OBJECT_DOMAIN IN ('TABLE', 'VIEW', 'DYNAMIC TABLE', 'MATERIALIZED VIEW', 'SEMANTIC_VIEW')
),
downstream_deps AS (
    SELECT
        dep_database || '.' || dep_schema || '.' || dep_object AS dependent_object,
        object_type
    FROM lineage_raw
    GROUP BY dep_database, dep_schema, dep_object, object_type
),
affected_users AS (
    SELECT
        ah.user_name,
        base.value:objectName::STRING AS accessed_object,
        COUNT(DISTINCT ah.query_id) AS query_count,
        MAX(ah.query_start_time) AS last_access
    FROM SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY ah,
    LATERAL FLATTEN(input => ah.base_objects_accessed) AS base
    WHERE ah.query_start_time >= DATEADD(day, -7, CURRENT_TIMESTAMP())
      AND base.value:objectName::STRING IN (SELECT dependent_object FROM downstream_deps)
    GROUP BY 1, 2
)
SELECT
    user_name,
    accessed_object,
    query_count AS queries_last_7_days,
    last_access,
    d.object_type
FROM affected_users au
JOIN downstream_deps d ON au.accessed_object = d.dependent_object
ORDER BY query_count DESC, last_access DESC;
