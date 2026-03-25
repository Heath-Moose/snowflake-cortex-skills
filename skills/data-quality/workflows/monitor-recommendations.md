---
parent_skill: data-quality
---

# Workflow: Monitor Recommendations

AI-driven DMF monitor recommendations based on column types, data patterns, downstream criticality, and access frequency. Ranks tables by priority and generates deployment DDL. The primary path for onboarding a schema to continuous DMF monitoring.

**Closes gaps:** G2, MA-01 (AI Monitor Recommendations), MA-02 (Column-Level Pattern Detection), MA-03 (Auto-Threshold guidance), MA-04 (Coverage Gap Analysis), MA-05 (One-Click Deployment).

## Trigger Phrases
- "Recommend monitors for my schema"
- "What should I monitor?"
- "Which DMFs should I attach?"
- "Set up DMFs for continuous monitoring"
- "Improve my DQ coverage"
- "What monitors am I missing?"
- "DQ gaps"
- "Which tables need monitoring?"
- "Help me attach DMFs"

## When to Load
- User has a schema with no or partial DMF coverage and wants recommendations
- User chose option 1 ("Set up DMFs") from the Step 0 preflight menu
- User explicitly wants guidance on which DMFs to attach

**Scope recommendations to critical assets:** Prioritize DMFs on contract surfaces (gold/shared tables, data products), business keys and identifiers used in joins, high-risk fields (PII, financial), and SLA-sensitive pipelines (FRESHNESS + ROW_COUNT). Avoid recommending heavy DMFs on every transient staging table by default.

---

## Execution Steps

### Step 1: Establish Scope

Extract target scope from user message:
- **Preferred**: `DATABASE.SCHEMA`
- **Acceptable**: database only (will profile all schemas)
- **Acceptable**: `DATABASE.SCHEMA.TABLE` (single table recommendations)

If not provided, ask:
> "Which schema would you like me to analyze for DMF recommendations? Please provide `DATABASE.SCHEMA`."

---

### Step 2: Profile Existing Coverage and Column Types

Read and execute `templates/monitor-recommendations.sql` with `<database>` and `<schema>` replaced.

This query produces a combined profile per table+column:
- Column name, data type, nullability
- Whether a DMF is already attached for that column
- Table access frequency (queries in last 90 days from ACCESS_HISTORY)

Review the results to build a mental model of:
- Which tables have **zero** DMFs (highest priority)
- Which tables have **partial** coverage (missing key columns)
- Column types present: timestamps/dates, VARCHAR IDs, numeric amounts, email-like strings

---

### Step 3: Assess Criticality via Lineage

For the top 10 highest-access tables with zero or partial DMF coverage, assess their downstream impact:

```sql
-- Downstream count per table — higher = more critical to monitor
SELECT
    REFERENCED_OBJECT_NAME AS table_name,
    COUNT(DISTINCT REFERENCING_OBJECT_NAME) AS downstream_count
FROM SNOWFLAKE.ACCOUNT_USAGE.OBJECT_DEPENDENCIES
WHERE REFERENCED_DATABASE = '<database>'
  AND REFERENCED_SCHEMA = '<schema>'
  AND REFERENCED_OBJECT_DOMAIN = 'Table'
GROUP BY 1
ORDER BY downstream_count DESC;
```

If `OBJECT_DEPENDENCIES` is unavailable (new tables or insufficient latency), skip this step and note that criticality is based on access frequency only.

---

### Step 4: Generate Ranked Recommendations

Using the column profile from Step 2 and criticality from Step 3, generate recommendations using this column-type mapping:

| Column Characteristic | Recommended DMF(s) | Rationale |
|---|---|---|
| Timestamp / DATE column | `SNOWFLAKE.CORE.FRESHNESS` | Detect stale data |
| `*_ID` / primary key column | `SNOWFLAKE.CORE.DUPLICATE_COUNT` + `SNOWFLAKE.CORE.UNIQUE_COUNT` | Detect FK/PK violations |
| Nullable VARCHAR (non-ID) | `SNOWFLAKE.CORE.NULL_COUNT` + `SNOWFLAKE.CORE.BLANK_COUNT` | Detect missing/empty values |
| VARCHAR with known valid values (status, category, enum-like) | `SNOWFLAKE.CORE.ACCEPTED_VALUES` with lambda (e.g., `col -> col IN (...)`) | Detect invalid categorical values without a custom DMF |
| Numeric (amount, price, count) | `SNOWFLAKE.CORE.NULL_COUNT` + `SNOWFLAKE.CORE.ACCEPTED_VALUES` (e.g., `price -> price > 0`) | Detect missing values + out-of-range values |
| Table level (all tables) | `SNOWFLAKE.CORE.ROW_COUNT` | Detect unexpected volume changes |
| VARCHAR with email-like values | `SNOWFLAKE.CORE.ACCEPTED_VALUES` (e.g., `email -> email LIKE '%@%.%'`) or custom email format DMF | Detect format issues; use ACCEPTED_VALUES for simple patterns, custom DMF for strict regex |

**Priority tiers:**
- **CRITICAL**: Zero coverage + ≥5 downstream objects OR top 20% access frequency
- **HIGH**: Zero coverage + <5 downstream OR partial coverage on critical columns
- **MEDIUM**: Coverage present but missing timestamp/freshness checks OR missing uniqueness on ID columns
- **LOW**: Well-covered tables with optional additional checks

Present the ranked table:

```
## DMF Recommendations: <DATABASE.SCHEMA>

### Coverage Summary
- Tables in schema: X
- Tables with ≥1 DMF: Y (Z%)
- Columns with DMF coverage: N

### Recommendations by Priority

#### 🔴 CRITICAL (deploy immediately)
1. TABLE_NAME — 0 DMFs, 12 downstream objects, 450 queries/week
   - Add: FRESHNESS on (updated_at)
   - Add: DUPLICATE_COUNT on (customer_id)
   - Add: ROW_COUNT (table level)

#### 🟡 HIGH
2. TABLE_NAME2 — 1 DMF, partially covered
   ...

#### 🟢 MEDIUM
3. TABLE_NAME3 — missing freshness check
   ...
```

---

### Step 5: Generate Deployment DDL

After presenting the ranked recommendations, generate the complete DDL:

```sql
-- Deployment DDL for <DATABASE.SCHEMA>
-- Generated by Monitor Recommendations workflow

-- Set schedule for schema (run every hour, or on changes)
ALTER SCHEMA <database>.<schema>
  SET DATA_METRIC_SCHEDULE = 'TRIGGER_ON_CHANGES';

-- CRITICAL: <TABLE_NAME>
ALTER TABLE <database>.<schema>.<TABLE_NAME>
  ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.FRESHNESS ON (<timestamp_column>),
  ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.DUPLICATE_COUNT ON (<id_column>),
  ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.ROW_COUNT ON ();

-- HIGH: <TABLE_NAME2>
ALTER TABLE <database>.<schema>.<TABLE_NAME2>
  ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.NULL_COUNT ON (<nullable_column>),
  ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.BLANK_COUNT ON (<varchar_column>);
```

**⚠️ MANDATORY STOPPING POINT**: Present the full DDL plan and ask:

> "I've generated the DMF deployment plan above. This will attach **X monitors** to **Y tables** in `<DATABASE.SCHEMA>`.
>
> **Estimated cost:** Approximately X DMF executions per hour (serverless compute, billed per execution).
>
> Shall I execute this? (Yes / No / Modify — e.g., 'skip TABLE_X' or 'only deploy CRITICAL tier')"

**NEVER execute the DDL without explicit user confirmation.**

---

### Step 6: Execute (On Approval)

Execute each `ALTER TABLE` statement in priority order (CRITICAL first).

After execution:
> "DMFs attached. The first measurements will appear within 1–2 minutes (for `TRIGGER_ON_CHANGES`) or on the next scheduled run.
>
> Next steps:
> - Run a **health check** to see the first results: 'Show me the schema health for `<DATABASE.SCHEMA>`'
> - Set **SLA alerts** to be notified when quality drops: 'Set up quality alerts for `<DATABASE.SCHEMA>`'
> - Set **expectation thresholds** to define pass/fail criteria: 'Set DMF expectations for `<DATABASE.SCHEMA>`'"

---

### Step 7 (Optional): Custom DMF Recommendations

If columns with email, phone, UUID, or custom business-rule patterns are detected, offer:

> "I noticed columns that may benefit from **custom format validation DMFs** (e.g., email format, value ranges). Would you like me to create those too?"

If yes → Load `workflows/custom-dmf-patterns.md`.

---

## Output Format
- Coverage summary (tables total, monitored %, columns covered)
- Ranked recommendation table by priority tier
- Column-type-to-DMF mapping rationale
- Complete deployment DDL (ready to execute)
- Post-deployment next steps

## Stopping Points
- ✋ **Step 1**: Scope not provided — ask for DATABASE.SCHEMA
- ✋ **Step 5**: Before executing DDL — show full plan and await explicit approval

## Error Handling
| Issue | Resolution |
|-------|-----------|
| OBJECT_DEPENDENCIES unavailable | Skip lineage criticality, base priority on access frequency only |
| ACCESS_HISTORY unavailable | Skip access frequency; base priority on column types and table row counts |
| DMF already attached | Skip that column/metric combination in recommendations |
| Schema has no tables | "Schema is empty or doesn't exist. Verify the database and schema names." |
| ALTER TABLE fails (permissions) | Report which privilege is missing: `CREATE DATA METRIC FUNCTION` or `ATTACH DATA METRIC FUNCTION PRIVILEGE` |

## Notes
- This workflow is **DMF-first by design** — it always recommends DMF setup, never ad-hoc assessment
- For custom pattern validation DMFs, see `workflows/custom-dmf-patterns.md`
- For setting pass/fail thresholds on the attached DMFs, see `workflows/expectations-management.md`
