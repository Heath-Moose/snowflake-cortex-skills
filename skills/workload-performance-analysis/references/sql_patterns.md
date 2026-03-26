# SQL Patterns Reference

Standard SQL patterns for workload performance analysis. All queries use 7-day default time range.

## Pattern Index

Use this table to find the right SQL pattern for the user's question:

| User Question Keywords | Pattern Section | Jump To |
|----------------------|-----------------|---------|
| "slowest queries", "slow queries" | Query Performance | [Slowest Queries](#slowest-queries) |
| "most partitions", "partition scan" | Query Performance | [Queries Scanning Most Partitions](#queries-scanning-most-partitions) |
| "worst pruning", "pruning efficiency" | Pruning Analysis | [Query-Level Pruning Efficiency](#query-level-pruning-efficiency-individual-queries) |
| "table pruning", "tables with worst pruning" | Pruning Analysis | [Table-Level Pruning](#table-level-pruning-aggregated) |
| "column pruning" | Pruning Analysis | [Column-Level Pruning](#column-level-pruning) |
| "QAS by warehouse", "warehouses QAS" | QAS | [Warehouses with QAS Opportunity](#warehouses-with-qas-opportunity) |
| "queries eligible for QAS", "QAS eligible queries", "query acceleration" | QAS | [Individual QAS-Eligible Queries](#individual-qas-eligible-queries) |
| "spillage by warehouse", "warehouses spillage" | Spillage Analysis | [Warehouses with Most Spillage](#warehouses-with-most-spillage) |
| "queries with spillage", "spillage queries" | Spillage Analysis | [Individual Queries with Most Spillage](#individual-queries-with-most-spillage) |
| "cache hit", "worst cache", "cache rate" | Cache Hit Rate | [Warehouses by Cache Hit Rate](#warehouses-by-cache-hit-rate) |
| "search optimization" | Search Optimization | [Columns with Search Optimization Opportunity](#columns-with-search-optimization-opportunity) |

---

## Query Performance

### Slowest Queries
```sql
SELECT 
    query_id,
    query_type,
    user_name,
    warehouse_name,
    warehouse_size,
    ROUND(execution_time / 1000.0, 2) AS execution_time_seconds,
    ROUND(total_elapsed_time / 1000.0, 2) AS total_elapsed_time_seconds,
    partitions_scanned,
    ROUND(bytes_scanned / 1024.0 / 1024 / 1024, 2) AS gb_scanned,
    start_time,
    LEFT(query_text, 200) AS query_text_preview
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE start_time >= DATEADD('day', -7, CURRENT_DATE())
  AND execution_status = 'SUCCESS'
  AND warehouse_name IS NOT NULL
ORDER BY execution_time DESC
LIMIT 20
```

### Queries Scanning Most Partitions
```sql
SELECT 
    query_id,
    query_type,
    user_name,
    warehouse_name,
    partitions_scanned,
    partitions_total,
    ROUND((partitions_scanned::FLOAT / NULLIF(partitions_total, 0)) * 100, 2) AS pct_partitions_scanned,
    ROUND(execution_time / 1000.0, 2) AS execution_time_seconds,
    ROUND(bytes_scanned / 1024.0 / 1024 / 1024, 2) AS gb_scanned,
    start_time,
    LEFT(query_text, 200) AS query_text_preview
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE start_time >= DATEADD('day', -7, CURRENT_DATE())
  AND partitions_scanned > 0
  AND execution_status = 'SUCCESS'
ORDER BY partitions_scanned DESC
LIMIT 20
```

## Pruning Analysis

### Query-Level Pruning Efficiency (Individual Queries)

Use this when user asks for "queries with worst pruning":

```sql
SELECT 
    query_id,
    query_type,
    user_name,
    warehouse_name,
    warehouse_size,
    partitions_scanned,
    partitions_total,
    ROUND((partitions_scanned::FLOAT / NULLIF(partitions_total, 0)) * 100, 2) AS pct_partitions_scanned,
    ROUND(100 - (partitions_scanned::FLOAT / NULLIF(partitions_total, 0)) * 100, 2) AS pruning_efficiency_pct,
    ROUND(bytes_scanned / 1024.0 / 1024 / 1024, 2) AS gb_scanned,
    ROUND(execution_time / 1000.0, 2) AS execution_seconds,
    start_time,
    LEFT(query_text, 100) AS query_preview
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE start_time >= DATEADD('day', -1, CURRENT_DATE())  -- Adjust time range as needed
  AND partitions_total > 0
  AND execution_status = 'SUCCESS'
  AND warehouse_name IS NOT NULL
ORDER BY pct_partitions_scanned DESC  -- Worst pruning first (scanning most partitions)
LIMIT 20
```

[IMPORTANT] Query-level pruning uses QUERY_HISTORY with partitions_scanned/partitions_total. For more granular table/column analysis, use the aggregated views below.

### Table-Level Pruning (Aggregated)
```sql
SELECT 
    tp.database_name || '.' || tp.schema_name || '.' || tp.table_name AS fully_qualified_table,
    SUM(tp.num_queries) AS query_count,
    SUM(tp.partitions_scanned) AS total_partitions_scanned,
    SUM(tp.partitions_pruned) AS total_partitions_pruned,
    ROUND(CASE 
        WHEN (SUM(tp.partitions_scanned) + SUM(tp.partitions_pruned)) > 0 
        THEN (SUM(tp.partitions_pruned)::FLOAT / (SUM(tp.partitions_scanned) + SUM(tp.partitions_pruned))) * 100 
        ELSE 0 
    END, 2) AS partition_pruning_pct,
    SUM(tp.rows_scanned) - SUM(tp.rows_matched) AS rows_wasted
FROM SNOWFLAKE.ACCOUNT_USAGE.TABLE_QUERY_PRUNING_HISTORY tp
WHERE tp.interval_start_time >= DATEADD('day', -7, CURRENT_DATE())
GROUP BY tp.database_name, tp.schema_name, tp.table_name
ORDER BY rows_wasted DESC
LIMIT 20
```

### Column-Level Pruning
```sql
SELECT 
    cp.database_name || '.' || cp.schema_name || '.' || cp.table_name AS fully_qualified_table,
    cp.column_name,
    SUM(cp.num_queries) AS column_usage_count,
    ROUND(CASE 
        WHEN (SUM(cp.rows_scanned) + SUM(cp.rows_pruned)) > 0 
        THEN 100 - (SUM(cp.rows_matched)::FLOAT / (SUM(cp.rows_scanned) + SUM(cp.rows_pruned))) * 100 
        ELSE 0 
    END, 2) AS unused_rows_percentage,
    SUM(cp.rows_scanned) - SUM(cp.rows_matched) AS rows_wasted
FROM SNOWFLAKE.ACCOUNT_USAGE.COLUMN_QUERY_PRUNING_HISTORY cp
WHERE cp.interval_start_time >= DATEADD('day', -7, CURRENT_DATE())
GROUP BY cp.database_name, cp.schema_name, cp.table_name, cp.column_name
ORDER BY rows_wasted DESC
LIMIT 20
```

## QAS (Query Acceleration Service)

### Warehouses with QAS Opportunity
```sql
SELECT 
    warehouse_name,
    warehouse_size,
    COUNT(*) AS eligible_query_count,
    ROUND(SUM(eligible_query_acceleration_time), 2) AS total_eligible_seconds,
    ROUND(AVG(eligible_query_acceleration_time), 2) AS avg_eligible_seconds_per_query,
    ROUND(AVG(upper_limit_scale_factor), 1) AS avg_scale_factor,
    MAX(upper_limit_scale_factor) AS max_scale_factor
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_ACCELERATION_ELIGIBLE
WHERE start_time >= DATEADD('day', -7, CURRENT_DATE())
  AND warehouse_name IS NOT NULL
GROUP BY warehouse_name, warehouse_size
ORDER BY total_eligible_seconds DESC
LIMIT 20
```

### Individual QAS-Eligible Queries
```sql
SELECT 
    query_id,
    warehouse_name,
    warehouse_size,
    ROUND(eligible_query_acceleration_time, 2) AS eligible_acceleration_seconds,
    upper_limit_scale_factor,
    start_time
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_ACCELERATION_ELIGIBLE
WHERE start_time >= DATEADD('day', -7, CURRENT_DATE())
  AND warehouse_name IS NOT NULL
ORDER BY eligible_query_acceleration_time DESC
LIMIT 20
```

## Spillage Analysis

### Warehouses with Most Spillage
```sql
SELECT 
    warehouse_name,
    warehouse_size,
    COUNT(*) AS query_count,
    ROUND(SUM(COALESCE(bytes_spilled_to_local_storage, 0)) / 1024.0 / 1024 / 1024, 2) AS local_spillage_gb,
    ROUND(SUM(COALESCE(bytes_spilled_to_remote_storage, 0)) / 1024.0 / 1024 / 1024, 2) AS remote_spillage_gb,
    COUNT(CASE WHEN bytes_spilled_to_local_storage > 0 OR bytes_spilled_to_remote_storage > 0 THEN 1 END) AS queries_with_spillage
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE start_time >= DATEADD('day', -7, CURRENT_DATE())
  AND warehouse_name IS NOT NULL
GROUP BY warehouse_name, warehouse_size
ORDER BY local_spillage_gb + remote_spillage_gb DESC
LIMIT 20
```

### Individual Queries with Most Spillage
```sql
SELECT 
    qh.query_id,
    qh.query_type,
    qh.user_name,
    qh.warehouse_name,
    qh.warehouse_size,
    LEFT(qh.query_text, 100) AS query_preview,
    ROUND(qh.bytes_spilled_to_local_storage / 1024.0 / 1024 / 1024, 2) AS local_spill_gb,
    ROUND(qh.bytes_spilled_to_remote_storage / 1024.0 / 1024 / 1024, 2) AS remote_spill_gb,
    ROUND(qh.execution_time / 1000.0, 2) AS execution_seconds,
    qh.start_time
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY qh
WHERE qh.start_time >= DATEADD('day', -7, CURRENT_DATE())
  AND (qh.bytes_spilled_to_local_storage > 0 OR qh.bytes_spilled_to_remote_storage > 0)
ORDER BY qh.bytes_spilled_to_local_storage + qh.bytes_spilled_to_remote_storage DESC
LIMIT 20
```

## Cache Hit Rate Analysis

### Warehouses by Cache Hit Rate
```sql
SELECT 
    warehouse_name,
    warehouse_size,
    COUNT(*) AS query_count,
    ROUND(AVG(percentage_scanned_from_cache), 1) AS avg_cache_hit_pct,
    ROUND(AVG(execution_time) / 1000.0, 2) AS avg_execution_seconds,
    ROUND(AVG(bytes_scanned) / 1024 / 1024 / 1024.0, 2) AS avg_gb_scanned
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE start_time >= DATEADD('day', -7, CURRENT_DATE())
  AND warehouse_name IS NOT NULL
  AND bytes_scanned > 0
GROUP BY warehouse_name, warehouse_size
ORDER BY avg_cache_hit_pct ASC
LIMIT 20
```

## Search Optimization Candidates

### Columns with Search Optimization Opportunity
```sql
SELECT 
    cp.database_name || '.' || cp.schema_name || '.' || cp.table_name AS fully_qualified_table,
    cp.column_name,
    SUM(cp.num_queries) AS query_count,
    SUM(cp.rows_scanned) AS total_rows_scanned,
    ROUND(AVG(cp.rows_matched), 0) AS average_rows_matched,
    ROUND(AVG(cp.partitions_scanned), 0) AS average_partitions_scanned,
    cp.search_optimization_supported_expressions
FROM SNOWFLAKE.ACCOUNT_USAGE.COLUMN_QUERY_PRUNING_HISTORY cp
WHERE cp.interval_start_time >= DATEADD('day', -7, CURRENT_DATE())
  AND cp.search_optimization_supported_expressions IS NOT NULL
GROUP BY cp.database_name, cp.schema_name, cp.table_name, cp.column_name, 
         cp.search_optimization_supported_expressions
ORDER BY total_rows_scanned DESC
LIMIT 30
```

## Hybrid Table Detection

### Check if Table is Hybrid
```sql
SHOW TABLES LIKE '<TABLE_NAME>' IN SCHEMA <DATABASE>.<SCHEMA>;
-- Check "is_hybrid" column in result
```

Or via ACCOUNT_USAGE (may have latency):
```sql
SELECT TABLE_NAME, TABLE_TYPE, IS_HYBRID
FROM SNOWFLAKE.ACCOUNT_USAGE.TABLES
WHERE TABLE_CATALOG = '<DATABASE>'
  AND TABLE_SCHEMA = '<SCHEMA>'
  AND TABLE_NAME = '<TABLE>'
  AND DELETED IS NULL
```
