-- Provenance Verification: Full Upstream Lineage Path with Trust Indicators (Primary: GET_LINEAGE)
-- Uses SNOWFLAKE.CORE.GET_LINEAGE() for upstream lineage. Replace <database>, <schema>, <table> before executing.
-- Use provenance-verification-object-deps-fallback.sql only if this query fails or object should have upstream lineage but returned 0 rows.

WITH upstream_edges AS (
    SELECT
        gl.SOURCE_OBJECT_DATABASE AS obj_database,
        gl.SOURCE_OBJECT_SCHEMA AS obj_schema,
        gl.SOURCE_OBJECT_NAME AS obj_name,
        gl.SOURCE_OBJECT_DOMAIN AS obj_type,
        gl.DISTANCE AS level
    FROM TABLE(SNOWFLAKE.CORE.GET_LINEAGE('<database>.<schema>.<table>', 'TABLE', 'UPSTREAM', 5)) gl
    WHERE gl.SOURCE_OBJECT_DOMAIN IN ('TABLE', 'VIEW', 'DYNAMIC TABLE', 'MATERIALIZED VIEW', 'STAGE', 'SEMANTIC_VIEW')
),
full_lineage AS (
    SELECT obj_database, obj_schema, obj_name, obj_type, level, 'UPSTREAM' AS direction
    FROM upstream_edges
    UNION ALL
    SELECT '<database>', '<schema>', '<table>', 'TARGET', 0, 'UPSTREAM'
),
object_metadata AS (
    SELECT
        table_catalog || '.' || table_schema || '.' || table_name AS object_name,
        table_type,
        table_schema,
        row_count,
        bytes,
        created,
        last_altered,
        table_owner
    FROM SNOWFLAKE.ACCOUNT_USAGE.TABLES
    WHERE deleted IS NULL
),
usage_stats AS (
    SELECT
        base.value:objectName::STRING AS object_name,
        COUNT(DISTINCT ah.user_name) AS unique_users_30d,
        COUNT(DISTINCT ah.query_id) AS query_count_30d
    FROM SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY ah,
    LATERAL FLATTEN(input => ah.base_objects_accessed) AS base
    WHERE ah.query_start_time >= DATEADD(day, -30, CURRENT_TIMESTAMP())
    GROUP BY 1
),
lineage_with_metadata AS (
    SELECT
        fl.level,
        fl.direction,
        fl.obj_database || '.' || fl.obj_schema || '.' || fl.obj_name AS object_name,
        fl.obj_type AS object_type,
        om.table_owner AS owner,
        om.last_altered,
        om.row_count,
        u.unique_users_30d,
        u.query_count_30d,
        /* SCHEMA_TRUST_TIER:fl.obj_schema */ AS trust_tier,
        CASE
            WHEN om.last_altered > DATEADD(day, -1, CURRENT_TIMESTAMP()) THEN 'FRESH'
            WHEN om.last_altered > DATEADD(day, -7, CURRENT_TIMESTAMP()) THEN 'RECENT'
            WHEN om.last_altered > DATEADD(day, -30, CURRENT_TIMESTAMP()) THEN 'STALE'
            ELSE 'VERY_STALE'
        END AS freshness_status,
        CASE
            WHEN COALESCE(u.unique_users_30d, 0) > 10 THEN 'HIGH_USAGE'
            WHEN COALESCE(u.unique_users_30d, 0) > 0 THEN 'ACTIVE'
            ELSE 'UNUSED'
        END AS usage_status
    FROM full_lineage fl
    LEFT JOIN object_metadata om ON om.object_name = fl.obj_database || '.' || fl.obj_schema || '.' || fl.obj_name
    LEFT JOIN usage_stats u ON u.object_name = fl.obj_database || '.' || fl.obj_schema || '.' || fl.obj_name
)
SELECT
    level,
    object_name,
    object_type,
    owner,
    trust_tier,
    freshness_status,
    usage_status,
    last_altered,
    row_count,
    unique_users_30d,
    query_count_30d,
    CASE
        WHEN trust_tier = 'PRODUCTION' AND freshness_status IN ('FRESH', 'RECENT') THEN '✅ VERIFIED'
        WHEN trust_tier = 'PRODUCTION' THEN '⚠️ VERIFY_FRESHNESS'
        WHEN trust_tier IN ('STAGING', 'RAW') THEN '⚠️ NON_PRODUCTION'
        WHEN trust_tier = 'DEVELOPMENT' THEN '❌ NOT_TRUSTED'
        ELSE '❓ UNKNOWN'
    END AS trust_status
FROM lineage_with_metadata
ORDER BY level, object_name;
