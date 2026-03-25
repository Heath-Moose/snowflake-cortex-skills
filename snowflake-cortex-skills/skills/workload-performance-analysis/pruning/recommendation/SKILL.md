# Pruning Recommendations

**This is a Phase 3 sub-skill. It is loaded by the parent `workload-performance-analysis` skill — do NOT invoke independently.**

## Purpose

Provide actionable recommendations to improve pruning efficiency based on detection findings.

## Workflow

### Step 1: Generate Recommendations

Based on the pruning detection findings, provide targeted recommendations:

| Finding | Recommendations |
|---|---|
| **Tables with low pruning efficiency** | Review frequently filtered columns as clustering key candidates. Present the column usage data from Phase 2. |
| **Columns with high scan volume + selective filters** | Strong search optimization candidates. Provide the `ALTER TABLE ... ADD SEARCH OPTIMIZATION` command. Recommend running cost estimation first. |
| **Queries scanning many partitions on well-clustered tables** | The queries may not be filtering on the clustering key columns. Highlight which columns are clustered vs. which columns the queries actually filter on. **Load** `references/pruning_troubleshooting.md` for common causes. |
| **Multiple columns frequently used together** | These may benefit from a compound clustering key. Present the co-occurrence data. |
| **Filter not pushed down to TableScan** | If operator-level analysis (Step 1C of detection) showed NULL `filter_condition` on a TableScan, explain likely causes: function applied to clustered column, filter on non-clustered column, or multi-column key order mismatch. Reference `references/pruning_troubleshooting.md`. |
| **High average_depth from SYSTEM$CLUSTERING_INFORMATION** | Table has a clustering key but is poorly clustered. Verify auto-clustering is active (detection Step 1B). If suspended, consider recommending resuming. If active, check `clustering_errors` in the output for failures. Common causes: frequent DML overwhelming the clustering service, too soon after enablement, or large recent DML not yet re-clustered. Present the `average_depth` vs `total_partition_count` ratio. |
| **Repetitive point-lookup or selective filter patterns** | Snowflake Optima may automatically optimize these if on Gen2 warehouses. Check Query Insights for `QUERY_INSIGHT_SNOWFLAKE_OPTIMA`. If not on Gen2, upgrading unlocks Optima at zero cost. For guaranteed performance, recommend manual SOS. |

### Step 2: Present Recommendations

```
### Pruning Recommendations

1. **<First recommendation>**
   - Why: <evidence — e.g., "TABLE_X has 15% pruning efficiency, 500M excess rows scanned">
   - How: <specific action>
   - Trade-off: <implication>

2. **<Second recommendation>**
   - Why: <evidence>
   - How: <action>
   - Trade-off: <implication>
```

**[IMPORTANT]:**
- **DO provide** data showing frequently used predicate columns
- **DO NOT make** specific clustering key recommendations — only highlight candidate columns
- **DO explain** that changing clustering keys affects ALL queries on the table
- **DO fetch** the official Snowflake documentation to present trade-offs:
  - Clustering: https://docs.snowflake.com/en/user-guide/tables-clustering-keys (see "Considerations for Choosing Clustering for a Table")
  - Search Optimization: https://docs.snowflake.com/en/user-guide/search-optimization/cost-estimation
  - Snowflake Optima: https://docs.snowflake.com/en/user-guide/snowflake-optima (requires Gen2 warehouses, zero cost, best-effort automatic optimization)
- **DO mention** that cost estimation tools exist (`SYSTEM$ESTIMATE_AUTOMATIC_CLUSTERING_COSTS`, `SYSTEM$ESTIMATE_SEARCH_OPTIMIZATION_COSTS`) but DO NOT run them unless the user asks
- **DO mention** Snowflake Optima as a zero-cost automatic option when pruning issues are detected. Note it requires Gen2 standard warehouses and works on a best-effort basis. For guaranteed performance, manual SOS is still recommended.
- **For SOS candidates**: Provide the `ALTER TABLE ... ADD SEARCH OPTIMIZATION ON EQUALITY(<column>)` command for each candidate column identified in detection

**[STOP]** Wait for user follow-up.
