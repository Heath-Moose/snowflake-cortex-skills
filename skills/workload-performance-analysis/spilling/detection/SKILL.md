# Spilling Detection

**This is a Phase 2 sub-skill. It is loaded by the parent `workload-performance-analysis` skill — do NOT invoke independently.**

## Purpose

Analyze spilling patterns to identify root causes and highlight the most impacted warehouses/queries.

## Prerequisites

- Spilling summary data already presented by `spilling/summary/SKILL.md`

## Workflow

### Step 1: Insights

After reviewing the summary data, provide:

1. **Key patterns** — which warehouses/users/time periods show the most local spilling and remote spilling
2. **Common causes:**
   - Warehouse too small for data volume (primary cause of both local and remote spilling)
   - High query concurrency competing for memory (exacerbates local spilling)
   - Complex joins/aggregations producing large intermediate results
3. **Severity assessment:**
   - **Remote spilling** (`bytes_spilled_to_remote_storage`) = severe memory pressure, significant performance impact — strong signal the warehouse is undersized
   - **Local-only spilling** (`bytes_spilled_to_local_storage` with no remote) = moderate, warehouse may be slightly undersized
   - **QAS check**: If a warehouse has QAS enabled and shows remote spilling, compare `query_acceleration_bytes_scanned` against `bytes_spilled_to_remote_storage`. If they are similar, the remote spilling is QAS overhead — not a memory pressure signal. Flag this: *"Remote spilling on <WAREHOUSE> appears to be QAS overhead, not memory pressure."*

### Step 2: Offer Drill-Down

If a specific query stands out (e.g. very high spilling), offer:
```
Query 01abc-123 has 85GB remote spilling. Want me to analyze this query in detail?
```

If user says yes, the parent skill will re-route to `query/summary/SKILL.md` for that query ID.

**[STOP]** Wait for user direction or continue to recommendations if depth = RECOMMENDATION.
