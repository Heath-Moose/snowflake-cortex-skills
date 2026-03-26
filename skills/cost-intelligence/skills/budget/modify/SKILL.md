# Modify Existing Budget

Interactive workflow for modifying an existing Snowflake budget: add/remove resources, change limits, configure notifications, or drop the budget.

> **See**: Parent `SKILL.md` for account vs custom decision, tag lookup, attribution methods overview, reference files, interaction rules, verification queries, and summary table format.

---

## Workflow

### Step 1: Identify Target Budget

If the user hasn't specified which budget to modify:

```sql
SHOW SNOWFLAKE.CORE.BUDGET INSTANCES IN ACCOUNT;
```

Present the list and ask the user to pick one. Record the fully qualified name: `{database}.{schema}.{budget_name}`.

If the user mentions the account budget, remind them of the limitations in parent `SKILL.md` — it does NOT support resource or tag management.

---

### Step 2: Show Current State

Run the shared verification queries from parent `SKILL.md` and present the summary table.

---

### Step 3: Action Menu

Present available actions grouped by category. If the user already stated what they want to change, skip the menu and go directly to the relevant action.

```
What would you like to change?

Spending Limit:
  1. Change spending limit

Tagged Resources (Method 1 — Recommended):
  2. Add a resource tag
  3. Remove a resource tag

Direct Inclusion (Method 2):
  4. Add a directly included resource
  5. Remove a directly included resource

Notifications & Actions:
  6. Configure notifications (email, webhook, threshold)
  7. Add custom action (trigger stored procedure)

Danger Zone:
  8. Drop this budget
```

---

## Actions: Spending Limit

### Change Spending Limit

Collect the new limit, then execute:

```sql
CALL {budget_fqn}!SET_SPENDING_LIMIT({new_limit});
```

---

## Actions: Direct Inclusion (Method 2)

These add or remove specific objects. 100% of the object's cost is attributed to this budget. Use when tag-based tracking isn't feasible.

### Add Direct Resource

Collect: object type + fully qualified name.
- Supported types: WAREHOUSE, DATABASE, TABLE, TASK, PIPE, COMPUTE_POOL, MATERIALIZED_VIEW, ALERT, REPLICATION_GROUP

Execute (for each collected resource):

```sql
-- Grant APPLYBUDGET first
GRANT APPLYBUDGET ON {object_type} {object_name} TO ROLE {current_role};

-- Add to budget
CALL {budget_fqn}!ADD_RESOURCE(
    SYSTEM$REFERENCE('{object_type}', '{object_name}', 'SESSION', 'applybudget')
);
```

> **Warning**: An object can only be in ONE budget via direct add. Adding it here silently removes it from any other budget.

### Remove Direct Resource

Show current resources via `GET_BUDGET_SCOPE()`, ask user to pick which to remove:

```sql
CALL {budget_fqn}!REMOVE_RESOURCE(
    SYSTEM$REFERENCE('{object_type}', '{object_name}', 'SESSION', 'applybudget')
);
```

---

## Actions: Tagged Resources (Method 1 — Recommended)

These add or remove tag-based groups. All objects matching a tag/value are tracked — 100% of each matching object's cost is attributed.

### Add Resource Tag

Collect: fully qualified tag name + value.

Execute (for each tag/value pair):

```sql
GRANT APPLYBUDGET ON TAG {tag_fqn} TO ROLE {current_role};

CALL {budget_fqn}!ADD_RESOURCE_TAG(
    SYSTEM$REFERENCE('TAG', '{tag_fqn}', 'SESSION', 'applybudget'),
    '{tag_value}'
);
```

### Remove Resource Tag

Show current tags via `GET_BUDGET_SCOPE()`, ask user to pick:

```sql
CALL {budget_fqn}!REMOVE_RESOURCE_TAG(
    SYSTEM$REFERENCE('TAG', '{tag_fqn}', 'SESSION', 'applybudget'),
    '{tag_value}'
);
```

---

## Actions: Notifications & Actions

### Configure Notifications

Collect what the user wants to set up:

**Email**:
```sql
CALL {budget_fqn}!SET_EMAIL_NOTIFICATIONS('{comma_separated_emails}');
```

**Threshold** (default 110%):
```sql
CALL {budget_fqn}!SET_NOTIFICATION_THRESHOLD({percentage});
```

**Webhook/Queue integration**:
```sql
CALL {budget_fqn}!ADD_NOTIFICATION_INTEGRATION('{integration_name}');
```

> **See**: `references/budget/notifications.md` for muting, payloads, multiple integrations.

### Add Custom Action

Collect:
- **Stored procedure**: Fully qualified name (must be owner's rights, no OUTPUT args)
- **Trigger type**: `PROJECTED` or `ACTUAL`
- **Threshold**: Percentage of spending limit

```sql
-- Grant usage to Snowflake app (ALL THREE required)
GRANT USAGE ON DATABASE {db} TO APPLICATION SNOWFLAKE;
GRANT USAGE ON SCHEMA {db}.{schema} TO APPLICATION SNOWFLAKE;
GRANT USAGE ON PROCEDURE {db}.{schema}.{proc_name}() TO APPLICATION SNOWFLAKE;

-- Add the action
CALL {budget_fqn}!ADD_CUSTOM_ACTION(
    SYSTEM$REFERENCE('PROCEDURE', '{db}.{schema}.{proc_name}()', 'SESSION', 'USAGE'),
    ARRAY_CONSTRUCT(),
    '{trigger_type}',
    {threshold}
);
```

> **See**: `references/budget/actions.md` for cycle-start actions, SP requirements.

---

## Actions: Danger Zone

### Drop Budget

> **Warning**: Dropping a budget removes all historical data and cannot be undone. Get explicit confirmation before executing.

```sql
DROP BUDGET {budget_fqn};
```

---

## Step 4: Refresh & Loop

After any modification, offer to refresh and ask if there's more to change:

```sql
CALL {budget_fqn}!REFRESH_USAGE();
```

```
Change applied. Would you like to:
1. Make another change to this budget
2. View the updated configuration
3. Done
```

If the user picks option 1, return to Step 3 (Action Menu).
If option 2, return to Step 2 (Show Current State).
