---
name: dynamic-tables-monitor
description: "Monitor health, status, and refresh performance of Snowflake dynamic tables. Use when: checking DT status, viewing refresh history, assessing target lag compliance, DT health check. Triggers: check status, refresh history, is it healthy, target lag, DT state, DT health."
parent_skill: dynamic-tables
---

# Monitor Dynamic Tables

Workflow for checking health, status, and performance of dynamic tables. This is a READ-ONLY workflow.

## When to Load

Main skill routes here when user wants to:
- Check dynamic table status or health
- View refresh history
- Understand pipeline dependencies
- Assess target lag compliance

---

## Workflow

⛔ **MANDATORY:** Before any `INFORMATION_SCHEMA` query, set database context:
```sql
USE DATABASE <database_name>;
```
Without this, `INFORMATION_SCHEMA` functions will fail with "Invalid identifier" errors.

### Step 1: Check Diary for Historical Context

**Goal:** Load previous analysis if available

**Actions:**

1. **Check connection diary** at `~/.snowflake/cortex/memory/dynamic_tables/<connection>/_connection_diary.md`:
   - Review known DTs in this account
   - Check if target DT is already in inventory

2. **Check DT diary** at `~/.snowflake/cortex/memory/dynamic_tables/<connection>/<database>.<schema>.<dt_name>.md`:
   - If exists: Read most recent entry, note previous metrics for comparison
   - If not exists: Note "First analysis of this DT - no historical baseline available"

---

### Step 2: Get Current State

**Goal:** Get current health status of dynamic table(s)

**Load** [references/dt-state.md](../references/dt-state.md) — `SHOW DYNAMIC TABLES` vs `INFORMATION_SCHEMA.DYNAMIC_TABLES()`, which columns each provides, and `scheduling_state` format differences.

**Actions:**

1. **Get configuration** via SHOW (for all DTs in a schema, or a specific DT):
   ```sql
   SHOW DYNAMIC TABLES IN SCHEMA <database>.<schema>;
   ```
   ```sql
   SHOW DYNAMIC TABLES LIKE '<dynamic_table_name>' IN SCHEMA <database>.<schema>;
   ```

   | Metric | Healthy | Concern |
   |--------|---------|---------|
   | `scheduling_state` | `RUNNING` | `SUSPENDED` |
   | `refresh_mode` | `INCREMENTAL` | `FULL` = may need optimization |

2. **Get lag metrics** via INFORMATION_SCHEMA (for all DTs, or a specific DT):
   ```sql
   SELECT 
     name, 
     scheduling_state,
     last_completed_refresh_state,
     target_lag_sec,
     maximum_lag_sec,
     time_within_target_lag_ratio
   FROM TABLE(INFORMATION_SCHEMA.DYNAMIC_TABLES())
   ORDER BY name;
   ```
   ```sql
   SELECT 
     name,
     scheduling_state,
     last_completed_refresh_state,
     target_lag_sec,
     maximum_lag_sec,
     time_within_target_lag_ratio
   FROM TABLE(INFORMATION_SCHEMA.DYNAMIC_TABLES(name=>'<database>.<schema>.<dynamic_table_name>'));
   ```

   | Metric | Healthy | Concern |
   |--------|---------|---------|
   | `last_completed_refresh_state` | `SUCCEEDED` | `FAILED`, `UPSTREAM_FAILED` |
   | `time_within_target_lag_ratio` | > 0.95 | < 0.90 = not meeting freshness |

---

### Step 3: Check Refresh History

**Goal:** Understand recent refresh behavior

**Actions:**

1. **Get recent refresh history**:
   ```sql
   SELECT 
     name,
     data_timestamp,
     refresh_start_time,
     refresh_end_time,
     DATEDIFF('second', refresh_start_time, refresh_end_time) as duration_sec,
     state,
     state_code,
     state_message,
     refresh_action,
     refresh_trigger,
      query_id,
      graph_history_valid_from,
      statistics:"compilationTimeMs"::INT / 1000 as compilation_sec,
     statistics:"executionTimeMs"::INT / 1000 as execution_sec,
     statistics:"numInsertedRows"::INT as rows_inserted,
     statistics:"numDeletedRows"::INT as rows_deleted,
     statistics:"numCopiedRows"::INT as rows_copied,
     statistics:"numAddedPartitions"::INT as partitions_added,
     statistics:"numRemovedPartitions"::INT as partitions_removed
   FROM TABLE(INFORMATION_SCHEMA.DYNAMIC_TABLE_REFRESH_HISTORY(
     NAME_PREFIX => '<database>.<schema>'
   ))
   ORDER BY refresh_start_time DESC
   LIMIT 10;
   ```

2. **Check for errors only** (last 7 days):
   ```sql
   SELECT 
     name, 
     refresh_start_time,
     state, 
     state_code,
     state_message,
     refresh_action
   FROM TABLE(INFORMATION_SCHEMA.DYNAMIC_TABLE_REFRESH_HISTORY(
     NAME_PREFIX => '<database>.<schema>',
     ERROR_ONLY => TRUE
   ))
   WHERE refresh_start_time > DATEADD('day', -7, CURRENT_TIMESTAMP())
   ORDER BY refresh_start_time DESC
   LIMIT 20;
   ```

3. **Calculate refresh statistics** (last 7 days):
   ```sql
   SELECT 
     name,
     COUNT(*) as total_refreshes,
      AVG(IFF(refresh_action IN ('INCREMENTAL','FULL') AND refresh_trigger != 'CREATION', DATEDIFF('second', refresh_start_time, refresh_end_time), NULL)) as avg_duration_sec,
      MAX(IFF(refresh_action IN ('INCREMENTAL','FULL') AND refresh_trigger != 'CREATION', DATEDIFF('second', refresh_start_time, refresh_end_time), NULL)) as max_duration_sec,
      AVG(IFF(refresh_trigger = 'CREATION' OR refresh_action = 'REINITIALIZE', DATEDIFF('second', refresh_start_time, refresh_end_time), NULL)) as avg_init_duration_sec,
     COUNT_IF(refresh_action = 'INCREMENTAL') as incremental_count,
     COUNT_IF(refresh_action = 'FULL') as full_count,
     COUNT_IF(refresh_action = 'REINITIALIZE') as reinitialize_count,
     COUNT_IF(refresh_action = 'NO_DATA') as no_data_count,
     COUNT_IF(refresh_trigger = 'CREATION') as creation_count,
     COUNT_IF(state = 'FAILED') as failed_count
   FROM TABLE(INFORMATION_SCHEMA.DYNAMIC_TABLE_REFRESH_HISTORY(name=>'<database>.<schema>.<dt_name>'))
   WHERE refresh_start_time > DATEADD('day', -7, CURRENT_TIMESTAMP())
   GROUP BY name;
   ```

   **Interpreting refresh categories:** See [references/dt-refresh-analysis.md](../references/dt-refresh-analysis.md#interpreting-refresh-categories) for how to categorize CREATION/REINITIALIZE vs steady-state refreshes. Exclude both from steady-state performance trends.

   **Comparing across REINITIALIZE boundaries:** See [references/dt-refresh-analysis.md](../references/dt-refresh-analysis.md#comparing-performance-across-reinitialize-boundaries) for methodology. Always report pre-reinit and post-reinit steady-state metrics separately.

---

### Step 4: View Pipeline Dependencies

**Goal:** Understand DAG structure and upstream/downstream relationships

**Load** [references/dt-graph.md](../references/dt-graph.md) — `DYNAMIC_TABLE_GRAPH_HISTORY()` columns, parameters, and schema evolution tracking.

**Actions:**

1. **Get dependency graph**:
   ```sql
   SELECT 
     name,
     inputs,
     scheduling_state,
     target_lag_type,
     target_lag_sec
   FROM TABLE(INFORMATION_SCHEMA.DYNAMIC_TABLE_GRAPH_HISTORY())
   WHERE name = '<dynamic_table_name>'
      OR ARRAY_CONTAINS('<dynamic_table_name>'::VARIANT, inputs);
   ```

2. **Interpret dependencies**:
   - `inputs` array shows upstream tables
   - Tables with `target_lag_type = 'DOWNSTREAM'` refresh when downstream needs them
   - Look for upstream tables with issues that could affect downstream

---

### Step 5: Analyze Refresh Query Performance

**Goal:** Understand compute usage and identify potential bottlenecks

**Actions:**

1. **Get refresh query details** (using query_id from refresh history):
   
   **Load** [references/dt-refresh-analysis.md](../references/dt-refresh-analysis.md) — covers which data source to use, when, and what privileges each requires.
   
   > **Short-circuit:** If the user already provided a `query_id`, skip the `DYNAMIC_TABLE_REFRESH_HISTORY` lookup in Step 3 and use the provided `query_id` directly.
   
   Use the data sources in this order:
   1. **`GET_QUERY_OPERATOR_STATS`** with the `query_id` — provides operator-level breakdowns for identifying bottlenecks. Verify warehouse access first (see reference doc).
   2. **`QUERY_HISTORY_BY_WAREHOUSE`** — if you need query-level I/O metrics (bytes scanned, partition pruning, spill). Use the warehouse name from Step 3 or `SHOW DYNAMIC TABLES`.
   3. **`SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY`** — if the query is outside the 7-day INFORMATION_SCHEMA retention or warehouse privileges are unavailable. Verify access before committing to this fallback (see reference doc).

---

### Step 6: Compare to Historical Baseline

**Goal:** Identify changes from previous analysis

**Actions:**

1. **If diary entry exists**, compare:
   - Refresh duration: increased/decreased?
   - `time_within_target_lag_ratio`: improved/degraded?
   - Refresh mode: changed from INCREMENTAL to FULL?
   - Error frequency: more/fewer failures?

2. **Highlight significant changes**:
   - "Refresh time increased from 45s → 120s (167% increase)"
   - "time_within_target_lag_ratio dropped from 0.98 → 0.72"
   - "Refresh mode changed from INCREMENTAL → FULL"

---

### Step 7: Write Diary Entries

**Goal:** Record current state for future comparison

**⚠️ CHECKPOINT**: Present the health report (see below) before writing diary entries. Proceed with diary writes after presenting findings — no explicit approval needed since this is local file storage only.

**Actions:**

1. **Write/append DT diary entry** to `~/.snowflake/cortex/memory/dynamic_tables/<connection>/<database>.<schema>.<dt_name>.md`:

   ```markdown
   ## Entry: <CURRENT_TIMESTAMP>

   ### Configuration
   - Refresh Mode: <refresh_mode>
   - Target Lag: <target_lag_sec> seconds
   - Warehouse: <warehouse_name>

   ### Health Metrics
   - scheduling_state: <value>
   - last_completed_refresh_state: <value>
   - time_within_target_lag_ratio: <value>
   - maximum_lag_sec: <value>

   ### Refresh Performance (last 7 days)
   - Total refreshes: <count>
   - Avg refresh time: <avg_sec>s
   - Max refresh time: <max_sec>s
   - Incremental refreshes: <count>
   - Full refreshes: <count>
   - Failed refreshes: <count>

   ### Notes
   - <any observations or recommendations>
   ```

2. **Update connection diary** at `~/.snowflake/cortex/memory/dynamic_tables/<connection>/_connection_diary.md`:
   - Update DT entry in "Discovered Dynamic Tables" with latest status
   - Add session history entry noting the health check
   - Add any cross-DT observations to recommendations

---

## Present Health Report

Summarize findings for user:

```
📊 Dynamic Table Health Report: <database>.<schema>.<dt_name>

Status: ✅ HEALTHY | ⚠️ WARNING | 🚨 CRITICAL

Configuration:
- Refresh Mode: INCREMENTAL
- Target Lag: 5 minutes
- Warehouse: COMPUTE_WH

Current Health:
- Scheduling State: RUNNING ✅
- Last Refresh: SUCCESS ✅
- Target Lag Compliance: 98% ✅

Performance (last 7 days):
- Avg Refresh Time: 45s
- Incremental/Full Ratio: 10/0 ✅
- Failed Refreshes: 0 ✅

[If diary exists]
Changes Since Last Check (<previous_date>):
- Refresh time: 45s → 52s (+15%)
- Target lag compliance: 98% → 98% (stable)

Recommendations:
- <any issues or optimization opportunities>
```

---

## Stopping Points

This is a READ-ONLY workflow. One checkpoint:
- ⚠️ Step 7: Present health report before writing diary entries
- If issues found, offer to route to TROUBLESHOOT workflow
- If optimization opportunities found, offer to route to OPTIMIZE workflow

---

## Output

- Health report with key metrics
- Historical comparison (if diary exists)
- Updated diary entry
- Recommendations for next steps (if issues found)
