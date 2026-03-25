---
parent_skill: data-quality
---

# Data Metric Functions (DMF) Concepts

## Overview

Snowflake Data Metric Functions (DMFs) provide built-in data quality monitoring capabilities to automatically track and measure data quality metrics across your tables and schemas. Understanding these concepts is essential before working with schema-level data quality monitoring workflows.

## Key Concepts

### 1. Data Metric Functions (DMFs)

A **Data Metric Function (DMF)** is a Snowflake function that computes a quality metric for a table or column. DMFs run automatically when data changes, enabling continuous data quality monitoring.

**Two types of DMFs:**

| Type | Description | Use Case |
|------|-------------|----------|
| **System DMFs** | Pre-built metrics by Snowflake | Common quality checks (nulls, freshness, uniqueness) |
| **Custom DMFs** | User-defined quality metrics | Domain-specific quality rules |

### 2. System DMFs

Snowflake provides built-in system DMFs for common quality checks:

**Data Freshness:**
```sql
SNOWFLAKE.CORE.FRESHNESS(
  TABLE_NAME => 'schema.table',
  TIMESTAMP_COLUMN => 'updated_at'
)
```
Measures how recent the data is based on a timestamp column.

**Null Count:**
```sql
SNOWFLAKE.CORE.NULL_COUNT(
  TABLE_NAME => 'schema.table',
  COLUMN_NAME => 'customer_id'
)
```
Counts null values in a column.

**Unique Count:**
```sql
SNOWFLAKE.CORE.UNIQUE_COUNT(
  TABLE_NAME => 'schema.table',
  COLUMN_NAME => 'email'
)
```
Counts unique values in a column.

**Duplicate Count:**
```sql
SNOWFLAKE.CORE.DUPLICATE_COUNT(
  TABLE_NAME => 'schema.table',
  COLUMN_NAME => 'email'
)
```
Counts duplicate values in a column.

**Row Count:**
```sql
SNOWFLAKE.CORE.ROW_COUNT(
  TABLE_NAME => 'schema.table'
)
```
Counts total rows in a table.

**Accepted Values:**
```sql
SNOWFLAKE.CORE.ACCEPTED_VALUES ON (
  <column>,
  <column> -> <boolean_expression>
)
```
Returns the number of records where the column value does **not** match the Boolean expression (i.e., violation count). Supports comparison operators, logical operators, `LIKE`, `RLIKE`, `IN`, and `IS [NOT] NULL`. Cannot be called directly — must be associated via `ALTER TABLE ... ADD DATA METRIC FUNCTION`. Works with VARCHAR, NUMBER, FLOAT, DATE, and TIMESTAMP types.

**Attach examples:**
```sql
-- Categorical: status must be in allowed set
ALTER TABLE my_schema.orders
  ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.ACCEPTED_VALUES ON (
    order_status,
    order_status -> order_status IN ('Pending', 'Dispatched', 'Delivered'));

-- Numeric range: price must be positive
ALTER TABLE my_schema.products
  ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.ACCEPTED_VALUES ON (
    price, price -> price > 0);

-- Combined logic: age must be between 0 and 120 (AND operator)
ALTER TABLE my_schema.customers
  ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.ACCEPTED_VALUES ON (
    age, age -> age >= 0 AND age <= 120);

-- Email format: strict regex validation
ALTER TABLE my_schema.customers
  ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.ACCEPTED_VALUES ON (
    email, email -> email RLIKE '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$');
```

**Ad-hoc scan (no attach required):**
Use `SYSTEM$DATA_METRIC_SCAN` to run ACCEPTED_VALUES on-demand and get the actual violating rows:
```sql
SELECT *
FROM TABLE(SYSTEM$DATA_METRIC_SCAN(
    REF_ENTITY_NAME  => 'MY_DATABASE.MY_SCHEMA.MY_TABLE',
    METRIC_NAME      => 'SNOWFLAKE.CORE.ACCEPTED_VALUES',
    ARGUMENT_NAME    => 'order_status',
    ARGUMENT_EXPRESSION => 'order_status IN (''Pending'', ''Dispatched'', ''Delivered'')'
));
```
See [SYSTEM$DATA_METRIC_SCAN](https://docs.snowflake.com/en/sql-reference/functions/system_data_metric_scan).

**When to use ACCEPTED_VALUES vs. Custom DMFs:**
- **Use ACCEPTED_VALUES** for: value-in-set, simple range checks, LIKE patterns, RLIKE/regex patterns, NULL checks, comparison operators
- **Use Custom DMFs** for: cross-column validation, referential integrity (FK checks), statistical outliers, multi-table joins

See [ACCEPTED_VALUES documentation](https://docs.snowflake.com/en/sql-reference/functions/dmf_accepted_values).

### 3. Custom DMFs

For domain-specific quality rules, create **Custom DMFs**:

```sql
CREATE OR REPLACE DATA METRIC FUNCTION my_schema.valid_email_pct()
RETURNS NUMBER
AS
$$
SELECT
  (COUNT_IF(email RLIKE '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Z|a-z]{2,}$') * 100.0) /
  NULLIF(COUNT(*), 0)
FROM TABLE(UPSTREAM_TABLES())
$$;
```

**Use cases:**
- Business rule validation (e.g., price > 0)
- Format validation (e.g., email patterns, phone formats)
- Referential integrity (e.g., foreign key checks)
- Statistical outliers (e.g., values outside 3 standard deviations)
- Cross-column validation (e.g., start_date < end_date)

### 4. Attaching DMFs to Tables

DMFs must be attached to tables to monitor them:

```sql
-- Attach a single DMF to a table
ALTER TABLE my_schema.customers
  ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.NULL_COUNT
  ON (email);

-- Attach multiple DMFs
ALTER TABLE my_schema.customers
  ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.FRESHNESS ON (updated_at),
  ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.DUPLICATE_COUNT ON (email),
  ADD DATA METRIC FUNCTION my_schema.valid_email_pct ON ();
```

**Schema-wide attachment:**
```sql
-- Attach DMF to all tables in a schema
ALTER SCHEMA my_schema
  SET DATA_METRIC_SCHEDULE = 'TRIGGER_ON_CHANGES';
```

### 5. DMF Scheduling

DMFs can run on different schedules:

| Schedule Type | Description | Use Case |
|--------------|-------------|----------|
| `TRIGGER_ON_CHANGES` | Run when data changes | Real-time quality monitoring |
| `CRON` | Run on a schedule (e.g., hourly, daily) | Periodic quality checks |
| `MANUAL` | Run only when explicitly triggered | Ad-hoc quality audits |

```sql
-- Set schedule for a schema
ALTER SCHEMA my_schema
  SET DATA_METRIC_SCHEDULE = 'USING CRON 0 */6 * * * UTC';
```

### 6. Accessing DMF Results

DMF metric results are accessed via the `SNOWFLAKE.LOCAL.DATA_QUALITY_MONITORING_RESULTS()` **table function**.

**IMPORTANT:** `SNOWFLAKE.ACCOUNT_USAGE.DATA_QUALITY_MONITORING_RESULTS` does **NOT** exist. Never query it.

**Correct way to query DMF results:**
```sql
-- Query results for a specific table
SELECT *
FROM TABLE(SNOWFLAKE.LOCAL.DATA_QUALITY_MONITORING_RESULTS(
  REF_ENTITY_NAME => 'MY_DATABASE.MY_SCHEMA.MY_TABLE',
  REF_ENTITY_DOMAIN => 'TABLE'
))
ORDER BY MEASUREMENT_TIME DESC;
```

**Columns returned by `SNOWFLAKE.LOCAL.DATA_QUALITY_MONITORING_RESULTS()`:**

| Column | Type | Description |
|--------|------|-------------|
| `MEASUREMENT_TIME` | TIMESTAMP_LTZ | When the metric was measured |
| `TABLE_NAME` | VARCHAR | Table being monitored |
| `TABLE_SCHEMA` | VARCHAR | Schema of the table |
| `TABLE_DATABASE` | VARCHAR | Database of the table |
| `METRIC_NAME` | VARCHAR | Name of the DMF |
| `METRIC_SCHEMA` | VARCHAR | Schema of the DMF |
| `METRIC_DATABASE` | VARCHAR | Database of the DMF |
| `VALUE` | VARIANT | The metric result value |
| `REFERENCE_ID` | VARCHAR | Unique reference for this metric attachment |
| `ARGUMENT_NAMES` | ARRAY | Column names the metric applies to |
| `ARGUMENT_TYPES` | ARRAY | Data types of the arguments |
| `ARGUMENT_IDS` | ARRAY | IDs of the arguments |

**Key differences from what you might expect:**
- The column for metric values is `VALUE`, not `metric_value`
- There is no `column_name` column — use `ARGUMENT_NAMES[0]` instead
- There is no `threshold` column — thresholds are in `DATA_METRIC_FUNCTION_EXPECTATIONS`
- This is a table function (requires `TABLE()` and per-table calls), not a view

**Related ACCOUNT_USAGE views (different purposes):**

| View | Purpose | Key Columns |
|------|---------|-------------|
| `DATA_QUALITY_MONITORING_USAGE_HISTORY` | Credit/cost tracking | `START_TIME`, `CREDITS_USED`, `TABLE_NAME` |
| `DATA_METRIC_FUNCTION_REFERENCES` | DMF configurations | `REF_DATABASE_NAME`, `REF_SCHEMA_NAME`, `METRIC_NAME`, `SCHEDULE` |
| `DATA_METRIC_FUNCTION_EXPECTATIONS` | DMF thresholds/rules | `REF_DATABASE_NAME`, `REF_SCHEMA_NAME`, `EXPECTATION_NAME`, `EXPECTATION_EXPRESSION` |

### 7. Viewing DMF Results

**Check which DMFs are attached (per table):**
```sql
-- See DMF references for a specific table
SELECT *
FROM TABLE(INFORMATION_SCHEMA.DATA_METRIC_FUNCTION_REFERENCES(
    REF_ENTITY_NAME => 'MY_DATABASE.MY_SCHEMA.MY_TABLE',
    REF_ENTITY_DOMAIN => 'TABLE'
));
```

**Query DMF metric results:**
```sql
-- Get latest results for a specific table
SELECT
    METRIC_NAME,
    VALUE,
    ARGUMENT_NAMES[0]::VARCHAR AS column_name,
    MEASUREMENT_TIME
FROM TABLE(SNOWFLAKE.LOCAL.DATA_QUALITY_MONITORING_RESULTS(
    REF_ENTITY_NAME => 'MY_DATABASE.MY_SCHEMA.MY_TABLE',
    REF_ENTITY_DOMAIN => 'TABLE'
))
QUALIFY ROW_NUMBER() OVER (PARTITION BY METRIC_NAME, REFERENCE_ID ORDER BY MEASUREMENT_TIME DESC) = 1
ORDER BY MEASUREMENT_TIME DESC;
```

### 8. Schema-Level Health Score

A **Schema Health Score** aggregates all DMF results across tables:

```sql
-- Calculate schema health percentage using SNOWFLAKE.LOCAL
WITH table_list AS (
    SELECT TABLE_NAME
    FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_CATALOG = 'MY_DATABASE'
      AND TABLE_SCHEMA = 'MY_SCHEMA'
      AND TABLE_TYPE = 'BASE TABLE'
),
all_metrics AS (
    SELECT t.TABLE_NAME, r.METRIC_NAME, r.VALUE, r.MEASUREMENT_TIME
    FROM table_list t,
    LATERAL (
        SELECT *
        FROM TABLE(SNOWFLAKE.LOCAL.DATA_QUALITY_MONITORING_RESULTS(
            REF_ENTITY_NAME => 'MY_DATABASE.MY_SCHEMA.' || t.TABLE_NAME,
            REF_ENTITY_DOMAIN => 'TABLE'
        ))
        QUALIFY ROW_NUMBER() OVER (PARTITION BY METRIC_NAME ORDER BY MEASUREMENT_TIME DESC) = 1
    ) r
)
SELECT
  ROUND((COUNT_IF(VALUE = 0) * 100.0) / NULLIF(COUNT(*), 0), 1) AS health_pct,
  COUNT_IF(VALUE > 0) AS failing_metrics,
  COUNT(*) AS total_metrics
FROM all_metrics;
```

**Interpretation:**
- 100% = All metrics passing (perfect health)
- 90-99% = Minor issues (good health)
- 75-89% = Moderate issues (needs attention)
- <75% = Significant issues (critical)

### 9. SLA Enforcement

Set quality SLAs and alert when violated:

```sql
-- Alert if schema health drops below 90%
-- Uses SNOWFLAKE.LOCAL.DATA_QUALITY_MONITORING_RESULTS() table function
-- See templates/schema-sla-alert.sql for the full production-ready template
CREATE ALERT my_schema_sla_alert
  WAREHOUSE = compute_wh
  SCHEDULE = '60 MINUTE'
IF (EXISTS (
  WITH table_list AS (
    SELECT TABLE_NAME
    FROM MY_DATABASE.INFORMATION_SCHEMA.TABLES
    WHERE TABLE_CATALOG = 'MY_DATABASE'
      AND TABLE_SCHEMA = 'MY_SCHEMA'
      AND TABLE_TYPE = 'BASE TABLE'
  ),
  all_metrics AS (
    SELECT t.TABLE_NAME, r.VALUE, r.MEASUREMENT_TIME
    FROM table_list t,
    LATERAL (
      SELECT *
      FROM TABLE(SNOWFLAKE.LOCAL.DATA_QUALITY_MONITORING_RESULTS(
        REF_ENTITY_NAME => 'MY_DATABASE.MY_SCHEMA.' || t.TABLE_NAME,
        REF_ENTITY_DOMAIN => 'TABLE'
      ))
      QUALIFY ROW_NUMBER() OVER (PARTITION BY METRIC_NAME ORDER BY MEASUREMENT_TIME DESC) = 1
    ) r
  ),
  health_check AS (
    SELECT ROUND((COUNT_IF(VALUE = 0) * 100.0) / NULLIF(COUNT(*), 0), 1) AS health_pct
    FROM all_metrics
  )
  SELECT 1 FROM health_check WHERE health_pct < 90
))
THEN CALL send_notification('Schema health SLA violated!');
```

### 10. Regression Detection

Compare current quality vs. previous run:

```sql
-- Detect tables with quality degradation
-- Uses SNOWFLAKE.LOCAL.DATA_QUALITY_MONITORING_RESULTS() table function
-- See templates/schema-regression-detection.sql for the full production-ready template
WITH table_list AS (
    SELECT TABLE_NAME
    FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_CATALOG = 'MY_DATABASE'
      AND TABLE_SCHEMA = 'MY_SCHEMA'
      AND TABLE_TYPE = 'BASE TABLE'
),
all_metrics AS (
    SELECT t.TABLE_NAME, r.METRIC_NAME, r.REFERENCE_ID, r.VALUE, r.MEASUREMENT_TIME
    FROM table_list t,
    LATERAL (
        SELECT *
        FROM TABLE(SNOWFLAKE.LOCAL.DATA_QUALITY_MONITORING_RESULTS(
            REF_ENTITY_NAME => 'MY_DATABASE.MY_SCHEMA.' || t.TABLE_NAME,
            REF_ENTITY_DOMAIN => 'TABLE'
        ))
    ) r
),
measurement_times AS (
    SELECT DISTINCT MEASUREMENT_TIME FROM all_metrics ORDER BY MEASUREMENT_TIME DESC LIMIT 2
),
current_run AS (
    SELECT TABLE_NAME, METRIC_NAME, REFERENCE_ID, VALUE
    FROM all_metrics WHERE MEASUREMENT_TIME = (SELECT MAX(MEASUREMENT_TIME) FROM measurement_times)
),
previous_run AS (
    SELECT TABLE_NAME, METRIC_NAME, REFERENCE_ID, VALUE
    FROM all_metrics WHERE MEASUREMENT_TIME = (SELECT MIN(MEASUREMENT_TIME) FROM measurement_times)
)
SELECT
  c.TABLE_NAME,
  c.METRIC_NAME,
  p.VALUE AS previous_value,
  c.VALUE AS current_value,
  c.VALUE - p.VALUE AS change
FROM current_run c
JOIN previous_run p
    ON c.TABLE_NAME = p.TABLE_NAME
    AND c.METRIC_NAME = p.METRIC_NAME
    AND c.REFERENCE_ID = p.REFERENCE_ID
WHERE c.VALUE > p.VALUE  -- Quality degraded
ORDER BY change DESC;
```

## Privilege Requirements

| Operation | Required Privilege |
|-----------|-------------------|
| Create DMF | CREATE DATA METRIC FUNCTION on schema |
| Attach DMF to table | MODIFY on table |
| View DMF references | SELECT on `INFORMATION_SCHEMA.DATA_METRIC_FUNCTION_REFERENCES()` (per table) |
| View DMF results | Access to `SNOWFLAKE.LOCAL.DATA_QUALITY_MONITORING_RESULTS()` |
| View DMF usage/credits | Access to `SNOWFLAKE.ACCOUNT_USAGE.DATA_QUALITY_MONITORING_USAGE_HISTORY` |
| Create alerts | CREATE ALERT on schema + EXECUTE TASK |

## Best Practices

1. **Start with system DMFs** - Use built-in metrics before creating custom ones
2. **Attach at schema level** - Automatically monitor all tables in a schema
3. **Run preflight check first** - Always run `preflight-check.sql` before any workflow
4. **Set appropriate schedules** - Balance freshness vs. compute costs
5. **Define SLAs upfront** - Know what "healthy" means for your data
6. **Test custom DMFs** - Validate logic before attaching to production tables
7. **Monitor compute usage** - DMFs consume warehouse credits (check `DATA_QUALITY_MONITORING_USAGE_HISTORY`)

## DMF Verification (CRITICAL)

**Always verify DMFs are attached and functioning:**

```sql
-- Check if DMFs are attached to a specific table
SELECT *
FROM TABLE(INFORMATION_SCHEMA.DATA_METRIC_FUNCTION_REFERENCES(
    REF_ENTITY_NAME => 'MY_DATABASE.MY_SCHEMA.MY_TABLE',
    REF_ENTITY_DOMAIN => 'TABLE'
));
```

**If no rows returned:**
- No DMFs attached to this table
- Schema health queries will return empty results
- User needs to attach DMFs first

```sql
-- Check if DMF results exist
SELECT COUNT(*) AS result_count
FROM TABLE(SNOWFLAKE.LOCAL.DATA_QUALITY_MONITORING_RESULTS(
    REF_ENTITY_NAME => 'MY_DATABASE.MY_SCHEMA.MY_TABLE',
    REF_ENTITY_DOMAIN => 'TABLE'
));
```

**If result_count = 0:**
- DMFs are attached but haven't run yet
- Wait for the next scheduled run (check SCHEDULE_STATUS)
- Only current snapshot is available after first run

## Workflow Integration

```
                    ┌─────────────────────┐
                    │   Define DMFs       │
                    │  (System + Custom)  │
                    └──────────┬──────────┘
                               │
                               ▼
┌─────────────────┐   ┌─────────────────┐   ┌─────────────────┐
│  Attach DMFs    │──▶│  Set Schedule   │──▶│  Enable Results │
│  to Tables      │   │ (Trigger/Cron)  │   │    Tracking     │
└─────────────────┘   └─────────────────┘   └────────┬────────┘
                                                     │
                                                     ▼
                                            ┌─────────────────┐
                                            │   Monitor &     │
                                            │  Alert on SLAs  │
                                            └─────────────────┘
```

## Common Patterns

### Pattern 1: Schema Health Dashboard
1. Attach DMFs to all tables in schema
2. Run `preflight-check.sql` to verify setup
3. Query schema health score periodically using `SNOWFLAKE.LOCAL`
4. Visualize trends in dashboard

### Pattern 2: Automated Quality Gates
1. Define quality SLAs (e.g., 95% health)
2. Create alerts for SLA violations
3. Integrate with CI/CD pipelines
4. Block deployments if quality degrades

### Pattern 3: Root Cause Analysis
1. Detect schema health drop
2. Query failing tables and metrics
3. Drill down to column-level issues
4. Remediate data quality problems

## Next Steps

After understanding DMF concepts:

1. **For schema health checks**: Use `schema-health-snapshot.sql` template
2. **For root cause analysis**: Use `schema-root-cause.sql` template
3. **For regression detection**: Use `schema-regression-detection.sql` template
4. **For SLA enforcement**: Use `schema-sla-alert.sql` template
5. **For trend analysis**: Use `schema-quality-trends.sql` template
