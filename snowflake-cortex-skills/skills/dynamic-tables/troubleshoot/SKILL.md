---
name: dynamic-tables-troubleshoot
description: "Diagnose and fix dynamic table refresh failures, UPSTREAM_FAILED errors, full-refresh-instead-of-incremental issues, stale/lagging data, suspended DTs, and pipeline errors. Triggers: DT broken, refresh failing, DT error, DT stuck, DT lag, DT suspended, why full refresh, incremental not working."
parent_skill: dynamic-tables
---

# Troubleshoot Dynamic Tables

Comprehensive troubleshooting workflow for diagnosing and resolving dynamic table issues including refresh failures, incremental vs full refresh problems, and performance issues.

## When to Load

Main skill routes here when user reports:
- Refresh failures or errors
- UPSTREAM_FAILED status
- Suspended dynamic tables
- Full refresh instead of expected incremental
- Target lag not being met
- Any DT-related errors

---

## Workflow

### Step 1: Check Diary for Historical Context

**Goal:** Understand previous state to identify when issues started

**Actions:**

1. **Check connection diary** at `~/.snowflake/cortex/memory/dynamic_tables/<connection>/_connection_diary.md`:
   - Review recent session history for related issues
   - Check if other DTs in the account have similar problems

2. **Check DT diary** at `~/.snowflake/cortex/memory/dynamic_tables/<connection>/<database>.<schema>.<dt_name>.md`:
   - When was last successful state recorded?
   - What changed since then?
   - Was it previously working with INCREMENTAL?

3. **Note**: "Was this working before? When did it break?"

---

### Step 2: Run Initial Diagnostics

**Goal:** Gather comprehensive state information

⛔ **MANDATORY:** Before any `INFORMATION_SCHEMA` query, set database context:
```sql
USE DATABASE <database_name>;
```
Without this, `INFORMATION_SCHEMA` functions will fail with "Invalid identifier" errors.

Two sources provide different information — **Load** [references/dt-state.md](../references/dt-state.md) for full column listings and `scheduling_state` format differences.

- **`SHOW DYNAMIC TABLES`** — configuration: `refresh_mode`, `refresh_mode_reason`, `warehouse`, `scheduling_state` (plain string: `RUNNING` or `SUSPENDED`), `target_lag`.
- **`INFORMATION_SCHEMA.DYNAMIC_TABLES()`** — lag metrics: `scheduling_state` (JSON object), `last_completed_refresh_state`, `time_within_target_lag_ratio`.

**Actions:**

1. **Get configuration and state**:
   ```sql
   SHOW DYNAMIC TABLES LIKE '<dt_name>' IN SCHEMA <database>.<schema>;
   -- Key columns: refresh_mode, refresh_mode_reason, warehouse, scheduling_state, target_lag, data_timestamp
   ```

   | Metric | Healthy | Concern |
   |--------|---------|---------|
   | `scheduling_state` | `RUNNING` | `SUSPENDED` |
   | `refresh_mode` | `INCREMENTAL` | `FULL` = may need optimization |

2. **Get lag metrics**:
   ```sql
   SELECT 
     name,
     scheduling_state,
     last_completed_refresh_state,
     target_lag_sec,
     maximum_lag_sec,
     time_within_target_lag_ratio
   FROM TABLE(INFORMATION_SCHEMA.DYNAMIC_TABLES(name=>'<fully_qualified_name>'));
   ```

   | Metric | Healthy | Concern |
   |--------|---------|---------|
   | `last_completed_refresh_state` | `SUCCEEDED` | `FAILED`, `UPSTREAM_FAILED` |
   | `time_within_target_lag_ratio` | > 0.95 | < 0.90 = not meeting freshness |

3. **Get recent refresh history** (including errors and `statistics` JSON):
   
   See [references/dt-refresh-analysis.md](../references/dt-refresh-analysis.md) for full `DYNAMIC_TABLE_REFRESH_HISTORY` syntax.
   
   ```sql
   SELECT 
     name,
     refresh_start_time,
     refresh_end_time,
     DATEDIFF('second', refresh_start_time, refresh_end_time) as duration_sec,
     state,
     state_code,
     state_message,
      refresh_action,
      refresh_trigger,
      reinit_reason,
      query_id,
     statistics:"compilationTimeMs"::INT / 1000 as compilation_sec,
     statistics:"executionTimeMs"::INT / 1000 as execution_sec,
     statistics:"numInsertedRows"::INT as rows_inserted,
     statistics:"numDeletedRows"::INT as rows_deleted,
     statistics:"numCopiedRows"::INT as rows_copied,
     statistics:"numAddedPartitions"::INT as partitions_added,
     statistics:"numRemovedPartitions"::INT as partitions_removed
   FROM TABLE(INFORMATION_SCHEMA.DYNAMIC_TABLE_REFRESH_HISTORY(name=>'<fully_qualified_name>'))
   ORDER BY refresh_start_time DESC
   LIMIT 10;
   ```

4. **Get DT definition**:
   ```sql
   SELECT GET_DDL('DYNAMIC_TABLE', '<fully_qualified_name>');
   ```

5. **Check pipeline dependencies**:
   ```sql
   SELECT name, inputs, scheduling_state
   FROM TABLE(INFORMATION_SCHEMA.DYNAMIC_TABLE_GRAPH_HISTORY())
   WHERE name = '<fully_qualified_name>'
      OR ARRAY_CONTAINS('<fully_qualified_name>'::VARIANT, inputs);
   ```

**⚠️ MANDATORY STOPPING POINT**: Present diagnostic findings before proceeding.

---

### Step 3: Identify Issue Type

**Goal:** Determine the root cause category

Based on diagnostics, identify the issue type:

| Symptom | Source | Issue Type | Go To |
|---------|--------|-----------|-------|
| `scheduling_state` = `SUSPENDED` | SHOW | Suspended Table | Step 4A |
| `last_completed_refresh_state` = `UPSTREAM_FAILED` | INFORMATION_SCHEMA | Upstream Failure | Step 4B |
| `refresh_mode` = `FULL` but expected INCREMENTAL | SHOW | Incremental Not Supported | Step 4C |
| `time_within_target_lag_ratio` < 0.9 | INFORMATION_SCHEMA | Target Lag Not Met | Step 4D |
| `state` = `FAILED` in refresh history | DYNAMIC_TABLE_REFRESH_HISTORY | Refresh Error | Step 4E |
| Change tracking errors | state_message | Change Tracking Issue | Step 4F |

---

### Step 4A: Troubleshoot Suspended Table

**Goal:** Diagnose and resume suspended dynamic table

**Diagnostic Queries:**

See [references/dt-refresh-analysis.md](../references/dt-refresh-analysis.md) for full `DYNAMIC_TABLE_REFRESH_HISTORY` syntax.

```sql
-- Check why suspended (consecutive failures trigger auto-suspend)
SELECT state, state_code, state_message, refresh_start_time
FROM TABLE(INFORMATION_SCHEMA.DYNAMIC_TABLE_REFRESH_HISTORY(name=>'<fully_qualified_name>', ERROR_ONLY => TRUE))
ORDER BY refresh_start_time DESC
LIMIT 5;
```

**Common Causes:**
- 5 consecutive refresh failures → auto-suspend
- Manual suspension by user
- Upstream table issues

**Resolution:**

1. **Fix the underlying cause** (see error messages)
2. **Resume the table**:
   ```sql
   ALTER DYNAMIC TABLE <dt_name> RESUME;
   ```

**⚠️ MANDATORY STOPPING POINT**: Present diagnosis and proposed fix before executing RESUME.

---

### Step 4B: Troubleshoot UPSTREAM_FAILED

**Goal:** Identify and fix upstream table causing failure

**Diagnostic Queries:**

```sql
-- Find upstream tables
SELECT name, inputs
FROM TABLE(INFORMATION_SCHEMA.DYNAMIC_TABLE_GRAPH_HISTORY())
WHERE name = '<fully_qualified_name>';

-- Check status of each upstream table
SELECT name, scheduling_state, last_completed_refresh_state
FROM TABLE(INFORMATION_SCHEMA.DYNAMIC_TABLES())
WHERE name IN (<list of inputs>);
```

**Resolution:**

1. **Identify the failing upstream table**
2. **Fix upstream table first** (recursive troubleshooting)
3. **After upstream is fixed, the downstream will recover automatically**
4. **If stuck, force resume**:
   ```sql
   ALTER DYNAMIC TABLE <dt_name> RESUME;
   ```

**⚠️ MANDATORY STOPPING POINT**: Present upstream analysis before any fixes.

---

### Step 4C: Troubleshoot Full Refresh Instead of Incremental

**Goal:** Understand why incremental refresh is not being used

**Diagnostic Queries:**

```sql
-- Check refresh_mode and refresh_mode_reason (only available via SHOW, not INFORMATION_SCHEMA)
SHOW DYNAMIC TABLES LIKE '<dt_name>' IN SCHEMA <database>.<schema>;
-- Look at refresh_mode and refresh_mode_reason columns in the output
```

```sql
-- Check actual refresh actions
SELECT refresh_action, COUNT(*) as count
FROM TABLE(INFORMATION_SCHEMA.DYNAMIC_TABLE_REFRESH_HISTORY(name=>'<fully_qualified_name>'))
WHERE refresh_start_time > DATEADD('day', -7, CURRENT_TIMESTAMP())
GROUP BY refresh_action;
```

**Common `refresh_mode_reason` values:**
- `QUERY_NOT_SUPPORTED_FOR_INCREMENTAL` - Query uses unsupported constructs
- `USER_SPECIFIED_FULL_REFRESH` - Created with REFRESH_MODE = FULL
- `UPSTREAM_USES_FULL_REFRESH` - Upstream DT uses FULL mode

**Load** [references/incremental-operators.md](../references/incremental-operators.md) to check supported constructs.

**Constructs that force FULL refresh:**
- Outer joins with non-equality predicates (e.g., `ON a.id > b.id`)
- Outer self-joins (same table on both sides)
- Outer joins with GROUP BY subqueries on both sides
- EXCEPT, INTERSECT, MINUS
- Certain window functions (LEAD/LAG may trigger FULL)
- Non-deterministic functions (see [incremental-operators.md → Non-Deterministic Functions](../references/incremental-operators.md#non-deterministic-functions) for details)

**Note:** LEFT, RIGHT, and FULL OUTER JOIN all support incremental refresh when using equality predicates and not self-joining.

**Resolution Options:**

1. **Restructure query** to use supported constructs
2. **Accept FULL refresh** if query cannot be changed
3. **Decompose DT** into smaller DTs (route to OPTIMIZE workflow)

**⚠️ MANDATORY STOPPING POINT**: Present analysis and options before any changes.

---

### Step 4D: Troubleshoot Target Lag Not Met

**Goal:** Identify why DT is not meeting freshness requirements

**Diagnostic Queries:**

```sql
-- Check refresh duration trend
SELECT 
  DATE_TRUNC('hour', refresh_start_time) as hour,
  AVG(IFF(refresh_action IN ('INCREMENTAL','FULL') AND refresh_trigger != 'CREATION', DATEDIFF('second', refresh_start_time, refresh_end_time), NULL)) as avg_duration_sec,
  MAX(IFF(refresh_action IN ('INCREMENTAL','FULL') AND refresh_trigger != 'CREATION', DATEDIFF('second', refresh_start_time, refresh_end_time), NULL)) as max_duration_sec,
  AVG(IFF(refresh_trigger = 'CREATION' OR refresh_action = 'REINITIALIZE', DATEDIFF('second', refresh_start_time, refresh_end_time), NULL)) as avg_init_duration_sec
FROM TABLE(INFORMATION_SCHEMA.DYNAMIC_TABLE_REFRESH_HISTORY(name=>'<fully_qualified_name>'))
WHERE refresh_start_time > DATEADD('day', -7, CURRENT_TIMESTAMP())
GROUP BY 1
ORDER BY 1 DESC;

-- Check if upstream is causing delay
SELECT name, maximum_lag_sec, time_within_target_lag_ratio
FROM TABLE(INFORMATION_SCHEMA.DYNAMIC_TABLES())
WHERE name IN (
  SELECT value::STRING 
  FROM TABLE(INFORMATION_SCHEMA.DYNAMIC_TABLE_GRAPH_HISTORY()),
  LATERAL FLATTEN(inputs)
  WHERE name = '<fully_qualified_name>'
);
```

**Analyze refresh query performance:**

**Load** [references/dt-refresh-analysis.md](../references/dt-refresh-analysis.md) — covers which data source to use, when, and what privileges each requires.

> **Short-circuit:** If the user already provided a `query_id`, skip the `DYNAMIC_TABLE_REFRESH_HISTORY` lookup and use the provided `query_id` directly.

Use the data sources in this order:
1. **`GET_QUERY_OPERATOR_STATS`** with the `query_id` from refresh history
2. **`QUERY_HISTORY_BY_WAREHOUSE`** — for query-level I/O metrics
3. **`SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY`** — if outside 7-day retention or warehouse privileges unavailable

**Common Causes:**
1. **Refresh takes longer than target lag** → Optimize query or increase warehouse
2. **Upstream DT is slow** → Fix upstream first
3. **Warehouse contention** → Use dedicated warehouse
4. **Data volume growth** → Consider DT decomposition

**Resolution Options:**

1. **Increase warehouse size**:
   ```sql
   CREATE OR REPLACE DYNAMIC TABLE <dt_name>
     WAREHOUSE = <larger_warehouse>
     -- ... rest of definition
   ```

2. **Relax target lag** if business allows:
   ```sql
   ALTER DYNAMIC TABLE <dt_name> SET TARGET_LAG = '<longer_time>';
   ```

3. **Optimize query** or **decompose DT** (route to OPTIMIZE workflow)

**⚠️ MANDATORY STOPPING POINT**: Present analysis and proposed solution before changes.

---

### Step 4E: Troubleshoot Refresh Errors

**Goal:** Diagnose specific refresh failure

**Diagnostic Queries:**

```sql
-- Get detailed error info
SELECT 
  state_code,
  state_message,
  query_id,
  refresh_start_time
FROM TABLE(INFORMATION_SCHEMA.DYNAMIC_TABLE_REFRESH_HISTORY(name=>'<fully_qualified_name>'))
WHERE state = 'FAILED'
ORDER BY refresh_start_time DESC
LIMIT 1;
```

**Get query error details using the `query_id`:**

**Load** [references/dt-refresh-analysis.md](../references/dt-refresh-analysis.md) — covers which data source to use, when, and what privileges each requires.

Use `SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY` or `QUERY_HISTORY_BY_WAREHOUSE` to retrieve error details for the failed `query_id`.

**Common Error Categories:**

| Error Type | Typical Cause | Resolution |
|------------|---------------|------------|
| Permission denied | Missing grants | Grant required privileges |
| Object not found | Base table dropped/renamed | Update DT definition |
| Out of memory | Query too complex | Increase warehouse or decompose |
| Change tracking | CT disabled on base table | Enable change tracking |

**⚠️ MANDATORY STOPPING POINT**: Present error analysis before executing any fix.

---

### Step 4F: Troubleshoot Change Tracking Issues

**Goal:** Verify and fix change tracking on base objects

**Diagnostic Queries:**

```sql
-- Check change tracking on base tables
SHOW TABLES LIKE '<base_table_name>';
-- Look for change_tracking = TRUE

-- If DT references views, check underlying tables too
SHOW VIEWS LIKE '<view_name>';
```

**Resolution:**

If change tracking is disabled:
```sql
ALTER TABLE <base_table> SET CHANGE_TRACKING = TRUE;
```

**⚠️ MANDATORY STOPPING POINT**: Get approval before enabling change tracking (has performance implications).

---

### Step 5: Execute Approved Fix

**Goal:** Apply the fix after user approval

**Actions:**

1. **Execute** the approved fix (ALTER, RESUME, etc.)
2. **Verify** the fix worked:
   ```sql
   SHOW DYNAMIC TABLES LIKE '<dt_name>' IN SCHEMA <database>.<schema>;
   -- Check scheduling_state and last_suspended_on columns
   ```
3. **Ask user about refresh verification:**
   
   "The dynamic table has been resumed. To verify it's working properly, I can either:
   
   A. **Manually trigger a refresh now** (immediate verification):
      ```sql
      ALTER DYNAMIC TABLE <dt_name> REFRESH;
      ```
   
   B. **Wait for the next scheduled refresh** (based on target lag: X minutes)
   
   Which would you prefer?"

4. **After refresh completes**, check status:
   ```sql
   SELECT state, state_message, refresh_action
   FROM TABLE(INFORMATION_SCHEMA.DYNAMIC_TABLE_REFRESH_HISTORY(name=>'<fully_qualified_name>'))
   ORDER BY refresh_start_time DESC
   LIMIT 1;
   ```

**⚠️ MANDATORY STOPPING POINTS**: 
- Ask user before choosing refresh method for verification.
- Verify fix before proceeding

**If fix fails or issue persists:**
- Maximum **2 retry attempts** per issue type
- After 2 failures: Stop and present full diagnostic summary to user
- Ask user: "The fix didn't resolve the issue. Would you like to:
  1. Try a different approach (route back to Step 3)
  2. Escalate / seek manual intervention
  3. Document current state and stop"
- **Do NOT loop indefinitely** - always give user control after retries exhausted

---

### Step 6: Write Diary Entries

**Goal:** Record problem state and resolution for future reference

**Actions:**

1. **Write DT diary entry** to `~/.snowflake/cortex/memory/dynamic_tables/<connection>/<database>.<schema>.<dt_name>.md`:

   ```markdown
   ## Entry: <CURRENT_TIMESTAMP> - TROUBLESHOOTING

   ### Issue Reported
   - <user description>

   ### Diagnostics
   - scheduling_state: <value>
   - last_completed_refresh_state: <value>
   - refresh_mode: <value>
   - refresh_mode_reason: <value>

   ### Root Cause
   - <identified cause>

   ### Resolution Applied
   - <fix description>

   ### Verification
   - Post-fix status: <SUCCESS/STILL_FAILING>

   ### Recommendations
   - <any follow-up actions>
   ```

2. **Update connection diary** at `~/.snowflake/cortex/memory/dynamic_tables/<connection>/_connection_diary.md`:
   - Update DT status in "Discovered Dynamic Tables" inventory
   - Add session history entry noting the troubleshooting session
   - If root cause affects other DTs, add to cross-DT recommendations

---

## Troubleshooting Decision Tree

```
Start Troubleshooting
    ↓
Run Initial Diagnostics (Step 2)
    ↓
    ├─→ scheduling_state = SUSPENDED (from SHOW)
    │       → Step 4A: Check consecutive failures, fix cause, RESUME
    │
    ├─→ last_completed_refresh_state = UPSTREAM_FAILED (from INFORMATION_SCHEMA)
    │       → Step 4B: Find failing upstream, fix it first
    │
    ├─→ refresh_mode = FULL, expected INCREMENTAL (from SHOW)
    │       → Step 4C: Check refresh_mode_reason, restructure or accept
    │
    ├─→ time_within_target_lag_ratio < 0.9 (from INFORMATION_SCHEMA)
    │       → Step 4D: Check duration/upstream, optimize or relax lag
    │
    ├─→ state = FAILED in history (from DYNAMIC_TABLE_REFRESH_HISTORY)
    │       → Step 4E: Check error details, fix specific issue
    │
    └─→ Change tracking errors
            → Step 4F: Enable change tracking on base tables
```

---

## Stopping Points Summary

1. ✋ After running initial diagnostics - present findings
2. ✋ After identifying root cause - present diagnosis
3. ✋ Before enabling change tracking on any table
4. ✋ Before executing any ALTER DYNAMIC TABLE command
5. ✋ Before executing RESUME on suspended table
6. ✋ After each individual fix - verify before proceeding

**Resume rule:** Only proceed after explicit user approval.
