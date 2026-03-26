-- Root Cause Analysis: Upstream Lineage (Primary: GET_LINEAGE)
-- Uses SNOWFLAKE.CORE.GET_LINEAGE() for object + data-movement lineage (no account admin).
-- Replace <database>, <schema>, <table> with actual values BEFORE executing.
-- Use root-cause-analysis-object-deps-fallback.sql only if this query fails or object should have upstream sources but returned 0 rows.

WITH upstream_edges AS (
    SELECT
        gl.SOURCE_OBJECT_DATABASE AS src_database,
        gl.SOURCE_OBJECT_SCHEMA AS src_schema,
        gl.SOURCE_OBJECT_NAME AS src_object,
        gl.SOURCE_OBJECT_DOMAIN AS src_type,
        gl.DISTANCE AS level
    FROM TABLE(SNOWFLAKE.CORE.GET_LINEAGE('<database>.<schema>.<table>', 'TABLE', 'UPSTREAM', 5)) gl
    WHERE gl.SOURCE_OBJECT_DOMAIN IN ('TABLE', 'VIEW', 'DYNAMIC TABLE', 'MATERIALIZED VIEW', 'STAGE', 'STREAM', 'SEMANTIC_VIEW')
),
upstream_lineage AS (
    SELECT
        src_database,
        src_schema,
        src_object,
        src_type,
        level,
        src_database || '.' || src_schema || '.' || src_object || ' → <database>.<schema>.<table>' AS lineage_path
    FROM upstream_edges
),
object_metadata AS (
    SELECT
        table_catalog || '.' || table_schema || '.' || table_name AS object_name,
        table_type,
        row_count,
        bytes,
        created,
        last_altered,
        table_owner
    FROM SNOWFLAKE.ACCOUNT_USAGE.TABLES
    WHERE deleted IS NULL
)
SELECT
    ul.level,
    ul.src_database || '.' || ul.src_schema || '.' || ul.src_object AS source_object,
    ul.src_type AS object_type,
    om.row_count,
    om.last_altered,
    om.table_owner AS owner,
    DATEDIFF(hour, om.last_altered, CURRENT_TIMESTAMP()) AS hours_since_modified,
    ul.lineage_path
FROM upstream_lineage ul
LEFT JOIN object_metadata om
    ON om.object_name = ul.src_database || '.' || ul.src_schema || '.' || ul.src_object
ORDER BY ul.level, ul.src_database, ul.src_schema, ul.src_object;
