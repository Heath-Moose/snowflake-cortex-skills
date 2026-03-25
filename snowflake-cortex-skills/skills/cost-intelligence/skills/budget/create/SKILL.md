# Create Custom Budget

Step-by-step workflow for creating a new Snowflake custom budget.

> **See**: Parent `SKILL.md` for account vs custom decision, tag lookup, attribution methods overview, reference files, interaction rules, verification queries, and summary table format.

---

## Workflow

> **IMPORTANT: Walk through both attribution methods sequentially (Steps 2 and 3). Always prompt the user for each — never skip. The user may opt out of either step, but must be asked.**
>
> **After Steps 2–3**: If the user opted out of both, warn that the budget will not track any costs and offer to go back.
>
> **EXECUTION ORDER**: All SQL in Step 5 has strict dependencies. The budget must be CREATED before any method calls (SET_SPENDING_LIMIT, ADD_RESOURCE_TAG, ADD_RESOURCE, SET_EMAIL_NOTIFICATIONS, etc.) can be made. Execute statements one at a time — never in parallel.

### Step 1: Budget Identity

Collect (confirm pre-provided values rather than re-asking):
- **Budget name** — Object name
- **Database.Schema** — Location for the budget instance
- **Spending limit** — Monthly credit limit (alerting only; does not block usage)

---

### Step 2: Tagged Resources (Recommended)

Tag-based tracking adds all objects matching a tag/value pair. 100% of each matching object's cost is attributed.

Key points:
- Objects CAN be in multiple budgets via tags
- Backfills from the start of the current month
- New objects tagged later are automatically included

Collect: tag/value pairs. Tag must be fully qualified — look up short names per parent SKILL.md.

---

### Step 3: Direct Inclusion

Direct inclusion adds specific objects to the budget. Use only when tag-based tracking isn't feasible.

Key points:
- An object can only be in ONE budget via direct add — adding it here silently removes it from any other budget
- No backfill — only tracks from the date it's added
- Supported types: WAREHOUSE, DATABASE, TABLE, TASK, PIPE, COMPUTE_POOL, MATERIALIZED_VIEW, ALERT, REPLICATION_GROUP

Collect: object type + fully qualified name for each resource.

---

### Step 4: Notifications (Optional)

- **Email**: Comma-separated addresses (must be verified in Snowsight)
- **Threshold**: Percentage of spending limit that triggers alert (default: 110%)
- **Webhook/Queue**: Integration name for Slack, Teams, SNS, etc.

Can be added later via the modify workflow.

---

### Step 5: Review & Execute

Assemble the complete SQL script and present for review. Only include sections for methods the user configured. Get explicit approval before executing.

> **CRITICAL — STRICT SEQUENTIAL EXECUTION REQUIRED**
>
> The SQL statements below have hard dependencies and **MUST be executed one at a time, in exact order**. Do NOT execute multiple statements in parallel. Each statement depends on the previous one succeeding:
>
> 1. **Privileges** — must be granted before anything else
> 2. **USE SCHEMA** — sets context for CREATE
> 3. **CREATE BUDGET** — the budget instance must exist before ANY method call
> 4. **Method calls** (SET_SPENDING_LIMIT, ADD_RESOURCE_TAG, ADD_RESOURCE, SET_EMAIL_NOTIFICATIONS, etc.) — these are methods on the budget object and will fail with "does not exist" errors if the budget has not been created yet
> 5. **REFRESH_USAGE** — must be last
>
> **Execute each statement individually, wait for it to succeed, then execute the next.** If any statement fails, stop and report the error.

**Template**:

```sql
-- ============================================
-- Budget: {budget_name}
-- Location: {database}.{schema}
-- Spending Limit: {spending_limit} credits/month
-- ============================================

-- Step A: Privileges (execute these first)
GRANT CREATE SNOWFLAKE.CORE.BUDGET ON SCHEMA {database}.{schema} TO ROLE {current_role};
-- (one GRANT APPLYBUDGET per direct resource)
GRANT APPLYBUDGET ON {object_type} {object_name} TO ROLE {current_role};
-- (one GRANT APPLYBUDGET per resource tag)
GRANT APPLYBUDGET ON TAG {tag_fqn} TO ROLE {current_role};

-- Step B: Create budget (must complete before ANY method calls below)
USE SCHEMA {database}.{schema};
CREATE SNOWFLAKE.CORE.BUDGET {budget_name}();

-- Step C: Set spending limit (requires budget from Step B to exist)
CALL {database}.{schema}.{budget_name}!SET_SPENDING_LIMIT({spending_limit});

-- Step D: Tagged Resources (preferred — if applicable — requires budget from Step B)
CALL {database}.{schema}.{budget_name}!ADD_RESOURCE_TAG(
    SYSTEM$REFERENCE('TAG', '{tag_fqn}', 'SESSION', 'applybudget'),
    '{tag_value}'
);

-- Step E: Direct Inclusion (if applicable — requires budget from Step B)
CALL {database}.{schema}.{budget_name}!ADD_RESOURCE(
    SYSTEM$REFERENCE('{object_type}', '{object_name}', 'SESSION', 'applybudget')
);

-- Step F: Notifications (if applicable — requires budget from Step B)
CALL {database}.{schema}.{budget_name}!SET_EMAIL_NOTIFICATIONS('{emails}');
CALL {database}.{schema}.{budget_name}!SET_NOTIFICATION_THRESHOLD({threshold});

-- Step G: Refresh usage (must be last)
CALL {database}.{schema}.{budget_name}!REFRESH_USAGE();
```

See `references/budget/troubleshooting.md` for common errors.

---

### Step 6: Verify

Run the shared verification queries from parent `SKILL.md` and present the summary table.
