---
name: workload-performance-analysis
description: "Snowflake SQL query execution analysis via ACCOUNT_USAGE views. Triggers: spilling, partition pruning, cache hit rates, clustering keys, search optimization (SOS) candidates, query acceleration (QAS) eligibility, predicate column analysis for clustering/SOS, per-warehouse spill/prune/cache metrics, slow SQL query diagnosis. Not for: cost/credits (cost-intelligence), access audit (data-governance), writing or debugging user SQL."
---

# Workload Performance Analysis

**You are using the workload-performance-analysis skill. Follow these instructions exactly.**

This is a **unified performance analysis skill** that handles all Snowflake performance questions through a single entry point. It detects the entity type and depth from the user's input, then routes to the appropriate sub-skill for each phase.

---

## Step 0: Detect Entity + Depth + Acquire Data

**Before doing any analysis, determine three things:**

### 0A. Entity Detection

Inspect the user's input and classify the primary entity:

**UI context detection:** The UI injects structured context data into the prompt using `${...}` variables (e.g. `${queryHistoryListContext}`, `${warehouseContext}`, etc.). When any `${...}` context is present, the skill is in **UI mode** — parse the available data and use it directly instead of running SQL queries.

| Signal in Input | Entity Type |
|---|---|
| Specific query ID (UUID-like format, e.g. `01b24bb0-0007-9627-0000-0001234abcde`) | **QUERY** |
| `query_parameterized_hash` value or "query pattern" / "recurring queries" / "repeated queries" | **QUERY_PATTERN** |
| Warehouse name (e.g. "ANALYTICS_WH") without query ID | **WAREHOUSE** |
| Table name (e.g. "DB.SCHEMA.ORDERS") | **TABLE** |
| "spilling", "spillage", "memory pressure", "spill to disk", "remote spilling" | **SPILLING** |
| "pruning", "partitions scanned", "scan volume", "worst pruning" | **PRUNING** |
| "clustering", "clustering keys", "cluster by", "tables for clustering" | **CLUSTERING** |
| "search optimization", "search index", "SOS", "search opt candidates" | **SEARCH_OPT** |
| "QAS", "query acceleration", "acceleration service", "QAS eligible" | **QAS** |
| "cache hit", "cache rate", "cache efficiency", "worst cache", "local disk cache", "warehouse cache" | **CACHE** |
| `${...}` context containing multiple queries | **MULTI_QUERY** |
| `${...}` context containing a single query | **QUERY** |
| No specific entity identified | **UNKNOWN** |

**Entity Identifier Validation:** The following entity types require a concrete identifier. If detected but the identifier is missing or unresolvable, **stop and ask the user to provide it before proceeding.**

| Entity Type | Required Identifier |
|---|---|
| QUERY | `query_id` (UUID format) |
| QUERY_PATTERN | `query_parameterized_hash` |
| WAREHOUSE | `warehouse_name` |
| TABLE | Fully qualified table name (`database.schema.table`) |

**If entity is UNKNOWN:** Ask the user:
```
What would you like me to analyze?

1. A specific entity — provide a warehouse name, query ID, table name, or query pattern hash
2. Account-level health check — scan across all performance dimensions (spilling, pruning, cache, QAS)
```

**MANDATORY STOPPING POINT:** Wait for the user's response.

- If the user provides a specific entity, re-classify and route accordingly.
- If the user picks option 2, proceed as **ACCOUNT** entity.

### 0B. Depth Detection

| Depth | Trigger Keywords | Phases to Load |
|---|---|---|
| **SUMMARY** | "summary", "overview", "quick look", "high-level", "brief", "health check" | `summary/` only |
| **DIAGNOSIS** | "what's wrong", "issues", "problems", "bottlenecks", "analyze", "why is X slow", "root cause", "performance issues", "concurrency issues", "statement timeout" | `summary/` + `detection/` |
| **RECOMMENDATION** | "recommend", "suggestion", "what should I do", "how to fix", "how to improve", "optimize", "best practice", "action items" | `summary/` + `detection/` + `recommendation/` |

**Default:** If depth is unclear, default to SUMMARY — load `summary/` only, then ask if user wants deeper analysis.

### 0C. Data Acquisition

- **If `${...}` context data is present (UI mode):** Parse the context data first. Use whatever fields are available as a starting point. However, the context may only contain partial information (e.g., query execution metrics but no pruning or spilling breakdown). **If the analysis requires data not present in the context, run supplementary SQL queries** using verified queries from the semantic model (see "SQL Query Construction" section below).
- **If no context data (CLI mode):** Construct SQL using verified queries from the semantic model to fetch data from ACCOUNT_USAGE views.

---

## Phase Routing

After detecting entity and depth, load the appropriate sub-skills:

### Entity → Sub-Skill Routing Table

| Entity | Summary (Phase 1) | Detection (Phase 2) | Recommendation (Phase 3) |
|---|---|---|---|
| **QUERY** | `query/summary/SKILL.md` | `query/detection/SKILL.md` | `query/recommendation/SKILL.md` |
| **QUERY_PATTERN** | `query-pattern/summary/SKILL.md` | `query-pattern/detection/SKILL.md` | `query-pattern/recommendation/SKILL.md` |
| **WAREHOUSE** | `warehouse/summary/SKILL.md` | `warehouse/detection/SKILL.md` | `warehouse/recommendation/SKILL.md` |
| **TABLE** | `table/summary/SKILL.md` | `table/detection/SKILL.md` | `table/recommendation/SKILL.md` |
| **SPILLING** | `spilling/summary/SKILL.md` | `spilling/detection/SKILL.md` | `spilling/recommendation/SKILL.md` |
| **PRUNING** | `pruning/summary/SKILL.md` | `pruning/detection/SKILL.md` | `pruning/recommendation/SKILL.md` |
| **CLUSTERING** | `pruning/summary/SKILL.md` | `pruning/detection/SKILL.md` | `pruning/recommendation/SKILL.md` |
| **SEARCH_OPT** | `pruning/summary/SKILL.md` | `pruning/detection/SKILL.md` | `pruning/recommendation/SKILL.md` |
| **QAS** | `qas/summary/SKILL.md` | `qas/detection/SKILL.md` | `qas/recommendation/SKILL.md` |
| **CACHE** | `cache/summary/SKILL.md` | `cache/detection/SKILL.md` | `cache/recommendation/SKILL.md` |
| **ACCOUNT** | `account/summary/SKILL.md` | `account/detection/SKILL.md` | `account/recommendation/SKILL.md` |
| **MULTI_QUERY** | Aggregate across queries in context, then route to relevant bottleneck entities based on findings |

### Phase Loading Rules

1. **SUMMARY depth:** Load `<entity>/summary/SKILL.md` only. After presenting results, ask: "Want me to identify root causes or provide recommendations?"
2. **DIAGNOSIS depth:** Load `<entity>/summary/SKILL.md` → then `<entity>/detection/SKILL.md`. After presenting results, ask: "Want me to provide recommendations for the issues found?"
3. **RECOMMENDATION depth:** Load `<entity>/summary/SKILL.md` → `<entity>/detection/SKILL.md` → `<entity>/recommendation/SKILL.md`. After presenting results, wait for user follow-up.

### Stopping Points

- **[STOP]** After Phase 1 summary (if SUMMARY depth) — offer deeper analysis or recommendations
- **[STOP]** After Phase 2 detection results (if DIAGNOSIS depth) — offer recommendations
- **[STOP]** After Phase 3 recommendations — wait for user follow-up
- **[STOP]** After hybrid table detection — explain limitations
- **[STOP]** If no data found — explain possible reasons (see Empty Results Handling)
- **[STOP]** If user asks a vague question — ask for clarification before proceeding

---

## SQL Query Construction

### Step 0: Load the Semantic Model

**[MANDATORY]** Before constructing or running any SQL, read the file `semantic_model/default.yaml` (relative to this skill's directory). This file contains:
- **Table definitions** with column names, types, and descriptions for each ACCOUNT_USAGE view
- **Relationships** between tables (e.g., join keys)
- **Verified queries** — pre-written, tested SQL queries indexed by name (e.g., "Which warehouses have the most spilling?")
- **Custom instructions** for consistent SQL output (required columns, formatting rules, aggregation patterns)

**Usage rules:**
1. When a sub-skill references a verified query by name, look up that exact name in the `verified_queries` section and use its SQL verbatim (after placeholder resolution in Step 1 below).
2. When constructing new SQL not covered by a verified query, use the table definitions and custom instructions from the semantic model to ensure correct column names and consistent output formatting.
3. Never fabricate column names or table structures — always cross-reference the semantic model.

### Step 1: Placeholder Resolution

When running verified queries, replace these placeholders with fully qualified table names.

**Default schema:** `SNOWFLAKE.ACCOUNT_USAGE`
- If the user specifies a different database/schema for their ACCOUNT_USAGE data, use that instead for all replacements below and all inline SQL in sub-skills.
- If a query against `SNOWFLAKE.ACCOUNT_USAGE` fails due to insufficient privileges or "does not exist" errors, inform the user: *"I need access to ACCOUNT_USAGE views. If you have a materialized copy in a different database/schema, let me know the DATABASE.SCHEMA name and I'll use that instead."* Then retry with the user-provided schema.

Placeholder mappings (using default schema — substitute override if active):
- `__query_history` → `SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY`
- `__query_acceleration_eligible` → `SNOWFLAKE.ACCOUNT_USAGE.QUERY_ACCELERATION_ELIGIBLE`
- `__table_pruning` → `SNOWFLAKE.ACCOUNT_USAGE.TABLE_QUERY_PRUNING_HISTORY`
- `__column_pruning` → `SNOWFLAKE.ACCOUNT_USAGE.COLUMN_QUERY_PRUNING_HISTORY`

### Step 2: Query Adaptation

When the user's question specifies a subset of a verified query's scope, adapt the WHERE filter and ORDER BY to match:

| User specifies | Adapt |
|---|---|
| "local spilling" / "spill to local" | Filter: `bytes_spilled_to_local_storage > 0` — Order by: `bytes_spilled_to_local_storage DESC` |
| "remote spilling" / "spill to remote" | Filter: `bytes_spilled_to_remote_storage > 0` — Order by: `bytes_spilled_to_remote_storage DESC` |
| "spilling" (generic) | Filter: `bytes_spilled_to_local_storage > 0 OR bytes_spilled_to_remote_storage > 0` — Order by: total (local + remote) DESC |
| Specific warehouse name | Add: `AND warehouse_name = '<NAME>'` |
| Specific user | Add: `AND user_name = '<NAME>'` |
| Custom time range ("last 3 days") | Replace the DATEADD interval |

**[CRITICAL]** Always keep the verified query's column list and structure — only adapt filters and ordering. NEVER add, remove, or rename columns from a verified query.

### Step 3: Execute and Present

Run the SQL and present results following the Output Format section below.

---

## Critical Rules

### 1. Internal Warehouses
- `COMPUTE_SERVICE_WH_*` warehouses are Snowflake-internal compute service warehouses
- They appear in `QUERY_HISTORY` and `QUERY_ACCELERATION_ELIGIBLE` but are **NOT visible via `SHOW WAREHOUSES`** and are **NOT user-configurable**
- When they appear in top-N results (spilling, QAS, cache), note them as internal and focus recommendations on user-owned warehouses

### 2. Default Limits and Summarization

| Question Type | Default LIMIT | Summarize |
|---|---|---|
| Query-level (slowest, spilling, QAS eligible) | 20 | Yes — "Found X total, showing top 20" |
| Warehouse-level aggregations | 20 | Yes — highlight key patterns |
| Column analysis | 20 | Yes — group by table |

**[WARNING]**
- DO NOT use LIMIT 100 or higher unless user explicitly requests
- Always provide a summary before listing results

### 3. Empty Results Handling

| Scenario | Response |
|---|---|
| No spilling | "No queries with spilling in the last 7 days. Warehouses are adequately sized." |
| No pruning data | "No pruning data found. Possible reasons: (1) No recent queries, (2) Data latency up to 4 hours, (3) Hybrid table." |
| No search opt candidates | "No search optimization candidates. Queries may already be well-optimized." |

**When entities (warehouse, table, view, query) are not found via SHOW commands:**

Possible causes to mention:
1. **Name misspelled** — Ask user to verify the exact name
2. **Insufficient permissions** — User's role may not have access to view this object
3. **Object doesn't exist** — It may have been dropped or never created
4. **Wrong database/schema context** — The object exists in a different database or schema

---

## Terminology

| Abbreviation | Full Term |
|---|---|
| WH | Warehouse |
| QAS | Query Acceleration Service |
| SOS | Search Optimization Service |

---

## Output Format

**[IMPORTANT] Always provide summary + top results, not raw data dumps:**

1. **Summary statement**: "Found X queries with [issue]. Here are the top 20:"
2. **Top results**: Show top 10-20 results — use indented list for query-level results, tables for warehouse/table aggregations
3. **Key insights**: Highlight patterns (common warehouses, time periods, etc.)
4. **Common causes** of the issue (see detection sub-skills for details)
5. **Format shortcut**: After presenting results, include: "You can say **'show as table'** or **'show as list'** to switch format."

---

## Important Guidelines

### Workload SLA: Speed vs Cost

Performance findings must be interpreted relative to the workload's Service Level Agreement — the customer's prioritization of speed vs cost:

| Dimension | Speed Priority | Cost Priority |
|---|---|---|
| **Queuing** | No queuing acceptable — upsize or add clusters immediately | Small amounts of queuing acceptable — saves credit cost |
| **Execution time** | Minimize at all costs — larger warehouses, QAS enabled | Longer execution times acceptable if credits are saved |
| **Multi-cluster scaling** | Standard policy — adds clusters as soon as queries queue | Economy policy — adds clusters only after sustained queuing |
| **Local disk cache / auto-suspend** | Higher auto-suspend to keep local disk cache warm — cache hit rate is critical for reporting warehouses that repeatedly scan the same tables | Lower auto-suspend to reduce idle credits — accept lower local disk cache hit rates |
| **Warehouse sizing** | Favor larger sizes to avoid spilling and reduce execution time | Favor smaller sizes — accept local spilling if execution time is tolerable |

When presenting recommendations that involve these tradeoffs, first explain both interpretations so the customer understands the concepts, then ask which priority applies to this warehouse/workload to tailor the guidance.

## Limitations

- ACCOUNT_USAGE views have latency (up to 45 min for QUERY_HISTORY, up to 4 hours for pruning views)
- Analyzes historical patterns only — cannot predict future performance
- Cannot estimate actual benefits of clustering/search optimization
- Hybrid tables have limited visibility in these views
