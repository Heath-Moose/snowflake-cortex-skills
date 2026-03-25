# QAS Detection

**This is a Phase 2 sub-skill. It is loaded by the parent `workload-performance-analysis` skill — do NOT invoke independently.**

## Purpose

Analyze QAS eligibility patterns and surface insights about which workloads would benefit most from query acceleration.

## Prerequisites

- QAS summary data already presented by `qas/summary/SKILL.md`

## Workflow

### Step 1: Check QAS Status on Flagged Warehouses

For each warehouse that appeared in the summary, check if QAS is already enabled:

```sql
SHOW WAREHOUSES LIKE '<WAREHOUSE_NAME>';
```

Check the `enable_query_acceleration` and `query_acceleration_max_scale_factor` columns:

- **If `enable_query_acceleration` is false**: QAS is not enabled — this is the primary recommendation opportunity.
- **If `enable_query_acceleration` is true**: QAS is already enabled. Check `query_acceleration_max_scale_factor` — if eligible queries still appear, the scale factor may need to be increased.

Present:
```
### QAS Status

| Warehouse | Size | QAS Enabled | Current Scale Factor |
```

### Step 2: Insights

After reviewing the summary data and QAS status, provide:

1. **Key patterns** — which warehouses have the most QAS opportunity, total potential time savings
2. **Common causes of QAS eligibility:**
   - Queries with selective filters scanning large tables
   - Queries that would benefit from parallel processing of scan-heavy operations
3. **Actionable context:**
   - QAS works best for queries that scan many partitions but produce small result sets
   - The `max_scale_factor` column gives guidance on how much acceleration is possible

### Step 3: Offer Drill-Down

If a specific query has high eligible acceleration time, offer to analyze it in detail.

**[STOP]** Wait for user direction or continue to recommendations if depth = RECOMMENDATION.
