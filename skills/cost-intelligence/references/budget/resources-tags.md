# Resources & Tags Reference

Custom budgets track objects via tag-based selection or direct resource addition.

> **Prefer `ADD_RESOURCE_TAG`** — it supports backfill, allows objects to belong to multiple budgets, and automatically picks up newly tagged objects. Use `ADD_RESOURCE` only when tag-based tracking isn't feasible.

---

## Two Ways to Add Objects

| Method | Use Case | Backfill? | Multi-Budget? |
|--------|----------|-----------|---------------|
| `ADD_RESOURCE_TAG` ✅ Preferred | Dynamic groups via tags | Yes | Yes (multiple budgets) |
| `ADD_RESOURCE` | Specific objects (no tagging) | No | No (1 budget only) |

---

## Tag-Based Resource Management (Preferred)

Tags allow dynamic membership: all objects with a specific tag/value are automatically tracked.

### Add Resource Tag

```sql
-- Grant APPLYBUDGET on the tag
GRANT APPLYBUDGET ON TAG mydb.tags.cost_center TO ROLE budget_owner;

-- Add tag to budget (tracks all objects with cost_center='engineering')
CALL my_budget!ADD_RESOURCE_TAG(
    SYSTEM$REFERENCE('TAG', 'mydb.tags.cost_center', 'SESSION', 'applybudget'),
    'engineering'
);
```

### Remove Resource Tag

```sql
CALL my_budget!REMOVE_RESOURCE_TAG(
    SYSTEM$REFERENCE('TAG', 'mydb.tags.cost_center', 'SESSION', 'applybudget'),
    'engineering'
);
```

### List Resource Tags

```sql
CALL my_budget!GET_RESOURCE_TAGS();
```

---

## Direct Resource Management

Use only when tag-based tracking isn't feasible. Objects added directly can only belong to **one** budget and do not backfill historical data.

### Add Resource

```sql
-- Grant APPLYBUDGET privilege first
GRANT APPLYBUDGET ON WAREHOUSE analytics_wh TO ROLE budget_owner;

-- Add to budget
CALL my_budget!ADD_RESOURCE(
    SYSTEM$REFERENCE('WAREHOUSE', 'analytics_wh', 'SESSION', 'applybudget')
);
```

### Supported Object Types

```sql
-- Warehouse
CALL my_budget!ADD_RESOURCE(
    SYSTEM$REFERENCE('WAREHOUSE', 'my_wh', 'SESSION', 'applybudget'));

-- Table
CALL my_budget!ADD_RESOURCE(
    SYSTEM$REFERENCE('TABLE', 'mydb.myschema.my_table', 'SESSION', 'applybudget'));

-- Database
CALL my_budget!ADD_RESOURCE(
    SYSTEM$REFERENCE('DATABASE', 'my_db', 'SESSION', 'applybudget'));

-- Task
CALL my_budget!ADD_RESOURCE(
    SYSTEM$REFERENCE('TASK', 'mydb.myschema.my_task', 'SESSION', 'applybudget'));

-- Pipe
CALL my_budget!ADD_RESOURCE(
    SYSTEM$REFERENCE('PIPE', 'mydb.myschema.my_pipe', 'SESSION', 'applybudget'));

-- Compute Pool
CALL my_budget!ADD_RESOURCE(
    SYSTEM$REFERENCE('COMPUTE_POOL', 'my_pool', 'SESSION', 'applybudget'));

-- Materialized View
CALL my_budget!ADD_RESOURCE(
    SYSTEM$REFERENCE('MATERIALIZED_VIEW', 'mydb.myschema.my_mv', 'SESSION', 'applybudget'));

-- Alert
CALL my_budget!ADD_RESOURCE(
    SYSTEM$REFERENCE('ALERT', 'mydb.myschema.my_alert', 'SESSION', 'applybudget'));

-- Replication Group
CALL my_budget!ADD_RESOURCE(
    SYSTEM$REFERENCE('REPLICATION_GROUP', 'my_rg', 'SESSION', 'applybudget'));

-- Native App (use DATABASE type)
CALL my_budget!ADD_RESOURCE(
    SYSTEM$REFERENCE('DATABASE', 'my_app', 'SESSION', 'applybudget'));
```

### Remove Resource

```sql
CALL my_budget!REMOVE_RESOURCE(
    SYSTEM$REFERENCE('WAREHOUSE', 'analytics_wh', 'SESSION', 'applybudget')
);
```

### List Linked Resources

> **Prefer `GET_BUDGET_SCOPE()`** — it returns everything (direct resources + resource tags) in one call. Use `GET_LINKED_RESOURCES()` only if you specifically need the direct-resource-only view.

```sql
CALL my_budget!GET_LINKED_RESOURCES();
```

---

## Tag Inheritance

Tags inherit from parent objects:
- Database tag → applies to schemas, tables within
- Schema tag → applies to tables within

**Override behavior**: If a child object has the same tag key with a different value, the child's value is used.

**Account-level tags**: NOT used for budgets (only object-level tags).

---

## Advanced: Tag Intersection Mode

By default, tags use **UNION** logic: object matches if it has ANY of the configured tags.

**Intersection mode**: Object must have ALL distinct tag keys (values within same key still UNION).

### Set Resource Tags with Mode

```sql
-- Atomically set up to 20 tags with intersection mode
CALL my_budget!SET_RESOURCE_TAGS(
    [
        {'tag': SYSTEM$REFERENCE('TAG', 'db.tags.cost_center', 'SESSION', 'applybudget'), 'value': 'engineering'},
        {'tag': SYSTEM$REFERENCE('TAG', 'db.tags.environment', 'SESSION', 'applybudget'), 'value': 'production'}
    ],
    'INTERSECTION'  -- or 'UNION' (default)
);
```

**INTERSECTION example**: Must have BOTH `cost_center=engineering` AND `environment=production`.

### Get Full Budget Scope (Recommended)

Use this as the single method for all scope-related questions — it returns direct resources, resource tags, and tag intersection mode in one structured view.

```sql
CALL my_budget!GET_BUDGET_SCOPE();
```

---

## Important Behavior Notes

### Direct Add: Single Budget Only

An object can only be in **ONE** budget via direct `ADD_RESOURCE`:
- Adding an object to Budget B that's already in Budget A removes it from Budget A
- No warning is given
- Use tags to have objects in multiple budgets

### Tag Add: Multiple Budgets OK

Objects added via tags can be in multiple budgets:
- Budget A tracks `cost_center='engineering'`
- Budget B tracks `environment='production'`
- Object with both tags is in both budgets

### Backfill Differences

| Method | First Month Data |
|--------|------------------|
| Direct ADD_RESOURCE | Only from add date forward |
| Tag ADD_RESOURCE_TAG | Backfilled from month start |

---

## SYSTEM$REFERENCE Parameters

```sql
SYSTEM$REFERENCE(
    'OBJECT_TYPE',      -- WAREHOUSE, TABLE, DATABASE, TAG, etc.
    'object_name',      -- Fully qualified name
    'SESSION',          -- Scope: SESSION or CALL
    'applybudget'       -- Privilege
)
```

| Parameter | Options |
|-----------|---------|
| Scope | `SESSION` (persists for session) or `CALL` (single call) |
| Privilege | Always `applybudget` for budgets |

---

> **Common Errors**: See `references/budget/troubleshooting.md` for error messages and solutions related to resources and tags.

---
