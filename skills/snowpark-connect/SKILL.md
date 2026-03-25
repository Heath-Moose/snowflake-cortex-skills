---
name: snowpark-connect
description: |
  Snowpark Connect (SCOS) skills for migrating and validating PySpark workloads on Snowflake.
  Use when: migrating PySpark to Snowpark Connect, validating SCOS migrations,
  analyzing Spark compatibility, or working with Snowpark Connect for Spark.
  Triggers: snowpark connect, scos, pyspark migration, spark connect,
  validate migration, pyspark compatibility.
---

# Snowpark Connect

Skills for working with Snowpark Connect for Spark (SCOS) on Snowflake.

## When to Use

- User wants to migrate PySpark or Databricks code to Snowflake
- User asks about SCOS or Snowpark Connect compatibility
- User wants to validate a completed SCOS migration
- User mentions "spark connect", "scos", or "snowpark connect"

## Intent Detection

Determine which sub-skill to load based on user intent:

```
Start
  ↓
Analyze User Request
  ↓
  ├─→ Migration intent → Load migrate-pyspark-to-snowpark-connect/SKILL.md
  │     (convert, migrate, update imports, rewrite for SCOS)
  │
  └─→ Validation intent → Load validate-pyspark-to-snowpark-connect/SKILL.md
        (validate, verify, check migration, test compatibility)
```

### Route: Migrate PySpark to Snowpark Connect

**If user wants to migrate or convert PySpark code:**
- Keywords: migrate, convert, rewrite, update imports, move to SCOS
- **Load** `migrate-pyspark-to-snowpark-connect/SKILL.md`
- Follow the migration workflow

### Route: Validate a Migration

**If user wants to validate or verify a completed migration:**
- Keywords: validate, verify, check, test, review migration
- **Load** `validate-pyspark-to-snowpark-connect/SKILL.md`
- Follow the validation workflow

## Stopping Points

None — this skill routes to sub-skills. Stopping points are defined within each sub-skill.

## Output

Output is determined by the loaded sub-skill:
- **Migration**: Migrated `_scos` files with compatibility fixes and migration headers
- **Validation**: Validation report with pass/fail status for each check
