# Snowflake APIs Reference

## Lineage: Primary vs Fallback

| API | Description | Use Case | Privileges |
|-----|-------------|----------|------------|
| **`SNOWFLAKE.CORE.GET_LINEAGE()`** | **Primary.** Object and data-movement lineage (upstream/downstream). | All table/column lineage workflows. | Object resolve + **VIEW LINEAGE** (granted to PUBLIC). No account admin. |
| `ACCOUNT_USAGE.OBJECT_DEPENDENCIES` | **Fallback.** Object dependency graph only (target depends on source). | Use when GET_LINEAGE returns no rows or privilege errors. | **Account admin** (e.g. `GRANT IMPORTED PRIVILEGES` on SNOWFLAKE). |

**Object dependency vs data movement:**
- **Object dependency:** Target object’s definition or data *depends on* the source (e.g. view on table). OBJECT_DEPENDENCIES captures this only.
- **Data movement:** Data is copied from source to target (e.g. CTAS, COPY INTO); target does not depend on source still existing. GET_LINEAGE captures both dependency and data movement.

Use GET_LINEAGE first; fall back to OBJECT_DEPENDENCIES when GET_LINEAGE is empty or not allowed.

**GET_LINEAGE usage:**
```sql
-- Downstream from a table (what depends on / is built from this)
SELECT * FROM TABLE(SNOWFLAKE.CORE.GET_LINEAGE('<db>.<schema>.<table>', 'TABLE', 'DOWNSTREAM', 5));

-- Upstream from a table (where this gets data from)
SELECT * FROM TABLE(SNOWFLAKE.CORE.GET_LINEAGE('<db>.<schema>.<table>', 'TABLE', 'UPSTREAM', 5));

-- Column-level: use object_name as db.schema.table.column, domain 'COLUMN'
SELECT * FROM TABLE(SNOWFLAKE.CORE.GET_LINEAGE('<db>.<schema>.<table>.<column>', 'COLUMN', 'DOWNSTREAM', 5));
```
Output columns include: SOURCE_OBJECT_DATABASE/SCHEMA/NAME/DOMAIN, TARGET_OBJECT_*, DISTANCE (1–5), PROCESS (VARIANT). Max 5 levels; max 10M rows.

## Account Usage Views

| API | Description | Use Case | Latency |
|-----|-------------|----------|---------|
| `ACCOUNT_USAGE.OBJECT_DEPENDENCIES` | Object dependency graph (fallback for lineage) | When GET_LINEAGE empty/fails | Near real-time |
| `ACCOUNT_USAGE.ACCESS_HISTORY` | Runtime data access patterns | Usage patterns, user attribution | 45min-3hr |
| `ACCOUNT_USAGE.QUERY_HISTORY` | Query execution details | Change attribution, debugging | 45min-3hr |
| `ACCOUNT_USAGE.TABLES` | Table metadata and timestamps | Schema change detection | 45min-3hr |
| `ACCOUNT_USAGE.COLUMNS` | Column metadata | Schema change detection | 45min-3hr |
| `ACCOUNT_USAGE.TABLE_STORAGE_METRICS` | Storage and freshness metrics | Trust scoring | 45min-3hr |
| `INFORMATION_SCHEMA.OBJECT_DEPENDENCIES` | Real-time deps (current DB only) | Fallback for real-time needs | Real-time |

## Privilege Requirements

```sql
-- GET_LINEAGE (primary): VIEW LINEAGE is granted to PUBLIC by default.
-- Ensure role can resolve the object (USAGE on database/schema, REFERENCE on object).

-- OBJECT_DEPENDENCIES fallback: requires account-level access to SNOWFLAKE
GRANT IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE TO ROLE <role_name>;
```

Without IMPORTED PRIVILEGES, ACCOUNT_USAGE views return empty or access denied.

## Performance Notes

- **GET_LINEAGE:** Table function; use with TABLE() and optional distance (1–5). Up to 10M rows.
- **ACCOUNT_USAGE queries:** Fast for targeted queries, slow for full scans
- **ACCESS_HISTORY:** Limited to 365 days retention
- **OBJECT_DEPENDENCIES:** May have large result sets for heavily-used tables
- **Always filter by time** where applicable
- **Use specific object names** when possible
