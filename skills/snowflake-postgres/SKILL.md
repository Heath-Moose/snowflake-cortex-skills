---
name: snowflake-postgres
description: "**[REQUIRED]** Use for **ALL** requests involving Snowflake Postgres: create instance, list instances, suspend, resume, reset credentials, describe instance, import connection, health check, diagnostics, pg_lake, iceberg tables, data lake, storage integration. Triggers: 'postgres', 'pg', 'create instance', 'show instances', 'suspend', 'resume', 'reset credentials', 'rotate password', 'reset access', 'import connection', 'network policy', 'my IP', 'health check', 'diagnose', 'insights', 'pg_doctor', 'slow queries', 'cache hit', 'bloat', 'vacuum', 'dead rows', 'locks', 'blocking queries', 'blocked', 'disk usage', 'what's running', 'active queries', 'connection count', 'pg_lake', 'iceberg', 'data lake', 'storage integration', 'parquet', 'COPY to S3', 'export to S3', 'lake'."
---

# Snowflake Postgres

## When to Use

When a user wants to manage Snowflake Postgres instances via Snowflake SQL.

## Setup

1. **Check for connection**: Verify a saved connection using the `connect/SKILL.md` workflow.
2. **Load references** as needed based on intent.

## Connection Storage (PostgreSQL Standard Files)

Connections use PostgreSQL's native configuration files instead of custom formats. This provides:
- Compatibility with all PostgreSQL tools (`psql`, pgAdmin, DBeaver, etc.)
- OS-enforced security (PostgreSQL rejects `.pgpass` if permissions are wrong)
- Separation of connection metadata from secrets

Never ask for credentials in chat.

### Service File: `~/.pg_service.conf`

PostgreSQL service file - stores named connection profiles (no passwords). Allows connecting with `psql service=<name>` instead of specifying all parameters:

```ini
[my_instance]
host=abc123.snowflakecomputing.com
port=5432
dbname=postgres
user=snowflake_admin
sslmode=verify-ca
sslrootcert=/Users/me/.snowflake/postgres/certs/my_instance.pem
```

When `sslrootcert` is present, `sslmode=verify-ca` verifies the server's identity using the CA certificate (MITM protection). The cert is fetched automatically on `--create` and `--reset`, or manually with `--fetch-cert`. Existing connections with `sslmode=require` continue to work.

Users can connect manually with: `psql service=my_instance` (if psql is installed)

### Password File: `~/.pgpass`

PostgreSQL password file - stores credentials separately from connection profiles. PostgreSQL clients automatically look up passwords from this file when connecting. Must have `chmod 600` permissions.

**⚠️ NEVER display `.pgpass` contents or format with actual passwords.** Always use `pg_connect.py` to manage passwords - it handles the file securely without exposing credentials in chat.

**Running queries:** Use `psql "service=<instance_name>" -c "<SQL>"` — authentication is handled automatically via the service file and pgpass. Never read or echo credential files.

**⚠️ Bash timeout:** All Postgres commands (psql, pg_connect.py, pg_lake_setup.py, pg_lake_storage.py) require network round-trips and SSL negotiation. **Never set `timeout_ms` below 60000 (60 seconds).** For bulk operations (COPY, CREATE TABLE AS, large queries), use 120000+ (2 minutes). The default `timeout_ms` is sufficient — do not lower it.

**⚠️ Check instance state before psql:** Instances with `auto_suspend_secs` will enter SUSPENDED state after inactivity. A psql connection to a suspended instance will hang (PG instances do NOT auto-resume on connection). **Before running any psql or pg_lake_setup.py command**, ensure the instance is READY:

```bash
uv run --project <SKILL_DIR> python <SKILL_DIR>/scripts/pg_connect.py \
  --ensure-ready --instance-name <INSTANCE_NAME> \
  [--snowflake-connection <SF_CONN>]
```

This checks the instance state, auto-resumes if SUSPENDED, and waits up to 6 minutes for READY. The pg_lake_setup.py script also retries connections automatically (3 attempts with backoff), but `--ensure-ready` avoids wasting time on retries when the instance needs a full resume cycle.

## Progress Tracking

For multi-step operations, use `system_todo_write` to show progress:

```
┌──────────────────┬──────────────────────────────────────────────────────┐
│ Scenario         │ Create Todos                                         │
├──────────────────┼──────────────────────────────────────────────────────┤
│ Create + setup   │ Create instance → Save connection → Network policy   │
├──────────────────┼──────────────────────────────────────────────────────┤
│ Batch operations │ One todo per instance/object                         │
└──────────────────┴──────────────────────────────────────────────────────┘
```

**Rules:**
- Mark `in_progress` BEFORE starting each step
- Mark `completed` IMMEDIATELY after finishing
- Add new todos if issues are discovered mid-workflow

## Intent Detection

| Intent | Trigger Phrases | Route |
|--------|-----------------|-------|
| **MANAGE** | "create instance", "show instances", "list instances", "suspend", "resume", "describe", "rotate password", "reset credentials", "reset access" | Load `manage/SKILL.md` |
| **CONNECT** | "my IP", "network policy", "can't connect", "add IP", "import connection" | Load `connect/SKILL.md` |
| **DIAGNOSE** | "health check", "diagnose", "diagnostics", "insights", "pg_doctor", "cache hit", "bloat", "vacuum", "dead rows", "autovacuum", "locks", "blocking queries", "blocked", "waiting", "long running", "slow queries", "query performance", "outliers", "unused indexes", "table sizes", "disk usage", "storage", "connections", "connection count", "what's running", "active queries" | Load `diagnose/SKILL.md` |
| **PG_LAKE** | "pg_lake", "iceberg", "data lake", "storage integration", "parquet", "COPY to S3", "export to S3", "move data", "lake" | Load `pg-lake/SKILL.md` |

### Unrecognized or Extended Operations

If the user's request involves Snowflake Postgres but doesn't match the intents above (e.g., fork, replica, maintenance window, upgrade, POSTGRES_SETTINGS):

1. **First** check `references/documentation.md` for the relevant doc URL
2. **Fetch** the official docs to get current syntax
3. **Apply** the same safety rules (approval for billable/destructive operations, no secrets in chat)

Examples of operations requiring doc lookup:
- Fork instance / point-in-time recovery
- Create read replica
- Set maintenance window
- Modify POSTGRES_SETTINGS
- Major version upgrades

## Routing

⚠️ **MANDATORY: Execute Sub-Skill Immediately**

After detecting intent, you MUST:
1. Load the sub-skill file
2. Execute its workflow **in this same response**
3. Do NOT stop after loading - continue to completion

| Intent | Action |
|--------|--------|
| **MANAGE** | Load `manage/SKILL.md` → Execute SQL immediately |
| **CONNECT** | Load `connect/SKILL.md` → Execute workflow immediately |
| **DIAGNOSE** | Load `diagnose/SKILL.md` → Execute diagnostics immediately |
| **PG_LAKE** | Load `pg-lake/SKILL.md` → Follow its workflow (has its own stopping points — present plan first for SETUP) |

❌ **WRONG:** Load skill, then stop or explain without doing anything
✅ **RIGHT:** Load skill, then follow its workflow (which may include presenting a plan and waiting for user confirmation before executing)

## Global Safety Rules

- Never ask for passwords in chat or echo secrets.
- **Never use `cat`, `echo`, heredoc (`<<`), or any shell command to create files containing `access_roles` or passwords** - these appear in chat history.
- Always require explicit approval for billable actions and network policy changes.
- For DESCRIBE responses, never show `access_roles`.
- **Prefer Cortex Search docs over web search for Snowflake-specific questions.** Check skill references and Snowflake documentation via Cortex Search first. Only fall back to web search if Cortex Search doesn't have what you need.
- For CREATE responses, never show raw SQL results - `access_roles` contains passwords.
- If any output might include secrets (passwords, access tokens), never display them in chat. Scripts save secrets to secure files (`~/.pgpass` with 0600 permissions) without echoing them.
- **For CREATE INSTANCE: MUST use `pg_connect.py --create`** - never use SQL tool directly. The script saves the connection automatically.
- **For RESET ACCESS: MUST use `pg_connect.py --reset`** - never use SQL tool directly. The script saves the password automatically.
- **Do NOT ask if user wants to save after CREATE/RESET** - the scripts save automatically.
- **Do NOT run RESET after CREATE** - CREATE already saves the password. RESET is only for rotating passwords later.
- **Never execute destructive operations (DROP TABLE, DROP COLUMN, DELETE, TRUNCATE, DROP INTEGRATION) without the user explicitly requesting it.** If the user asks to "clean up" or "remove" something, confirm exactly what will be deleted before executing. DROP TABLE on Iceberg tables permanently deletes S3 data files.

## Tools

### Tool: ask_user_question

**Description:** Ask the user to choose from a fixed list of options.

**When to use:** Present configuration menus (instance size, storage, HA, version, network policy).

### Script: network_policy_check.py

**Description:** Check whether an IP is allowed by a Snowflake network policy.

**Usage:**
```bash
uv run --project <SKILL_DIR> python <SKILL_DIR>/scripts/network_policy_check.py \
  --policy-name <POLICY_NAME> \
  [--ip <IP>]
```

### Script: pg_connect.py

**Description:** Manage Snowflake Postgres connections. Handles CREATE, RESET, and connection file management (`~/.pg_service.conf` and `~/.pgpass`) without exposing credentials.

**Usage (create instance - executes SQL + saves connection):**
```bash
uv run --project <SKILL_DIR> python <SKILL_DIR>/scripts/pg_connect.py \
  --create \
  --instance-name <NAME> \
  --compute-pool <STANDARD_M|STANDARD_L|...> \
  --storage <GB> \
  [--auto-suspend-secs <SECONDS>] \
  [--enable-ha] \
  [--postgres-version <VERSION>] \
  [--network-policy <POLICY_NAME>] \
  [--snowflake-connection <NAME>]
```

**Usage (reset credentials - executes SQL + updates password):**
```bash
uv run --project <SKILL_DIR> python <SKILL_DIR>/scripts/pg_connect.py \
  --reset \
  --instance-name <NAME> \
  [--role <snowflake_admin|application>] \
  [--host <HOST>] \
  [--snowflake-connection <NAME>]
```
Use `--host` to create the service entry if it doesn't exist (e.g., from DESCRIBE output).

**Usage (fetch CA certificate for server identity verification):**
```bash
uv run --project <SKILL_DIR> python <SKILL_DIR>/scripts/pg_connect.py \
  --fetch-cert \
  --instance-name <NAME> \
  [--snowflake-connection <NAME>]
```
Fetches the CA certificate via `DESCRIBE POSTGRES INSTANCE` and upgrades the service entry to `sslmode=verify-ca`. Run this for existing connections that use `sslmode=require`.

**Usage (list saved connections):**
```bash
uv run --project <SKILL_DIR> python <SKILL_DIR>/scripts/pg_connect.py --list
```

**Usage (ensure instance is ready before PG operations):**
```bash
uv run --project <SKILL_DIR> python <SKILL_DIR>/scripts/pg_connect.py \
  --ensure-ready \
  --instance-name <NAME> \
  [--snowflake-connection <NAME>] \
  [--no-auto-resume]
```
Checks instance state via Snowflake, auto-resumes if SUSPENDED, waits for READY. Use `--no-auto-resume` to only check without resuming.

Uses Snowflake connection from `~/.snowflake/connections.toml` or environment variables. Use `--snowflake-connection` to specify a named connection.

### Script: pg_doctor.py

**Description:** Run Postgres health diagnostics. All queries run in readonly mode with statement timeout.

**Usage (full health check):**
```bash
uv run --project <SKILL_DIR> python <SKILL_DIR>/scripts/pg_doctor.py \
  --connection-name <NAME>
```

**Usage (single check):**
```bash
uv run --project <SKILL_DIR> python <SKILL_DIR>/scripts/pg_doctor.py \
  --connection-name <NAME> \
  --check <CHECK_NAME>
```

**Flags:** `--json`, `--detailed`, `--category <CATEGORY>`, `--all`, `--list-checks`, `--timeout <MS>`

### Script: pg_lake_setup.py

**Description:** pg_lake extension setup and verification on Postgres. Checks extensions, enables pg_lake, configures S3, verifies access, manages Iceberg tables.

**Usage:**
```bash
uv run --project <SKILL_DIR> python <SKILL_DIR>/scripts/pg_lake_setup.py \
  --check-extensions --connection-name <PG_CONN> --json
uv run --project <SKILL_DIR> python <SKILL_DIR>/scripts/pg_lake_setup.py \
  --enable-extensions --connection-name <PG_CONN>
uv run --project <SKILL_DIR> python <SKILL_DIR>/scripts/pg_lake_setup.py \
  --verify-s3 --connection-name <PG_CONN> --json
uv run --project <SKILL_DIR> python <SKILL_DIR>/scripts/pg_lake_setup.py \
  --list-iceberg --connection-name <PG_CONN> --json
```

### Script: pg_lake_storage.py

**Description:** Snowflake storage integration management for pg_lake. Creates, describes, attaches, and drops POSTGRES_EXTERNAL_STORAGE integrations. Sensitive IAM values written to secure temp files.

**Usage:**
```bash
uv run --project <SKILL_DIR> python <SKILL_DIR>/scripts/pg_lake_storage.py \
  create --name <NAME> --role-arn <ARN> --locations s3://bucket/ \
  --snowflake-connection <SF_CONN> --json
uv run --project <SKILL_DIR> python <SKILL_DIR>/scripts/pg_lake_storage.py \
  describe --name <NAME> --snowflake-connection <SF_CONN>
uv run --project <SKILL_DIR> python <SKILL_DIR>/scripts/pg_lake_storage.py \
  check-aws --role-arn <ARN> \
  --expected-principal <IAM_USER_ARN> --expected-external-id "<EXT_ID>" \
  [--aws-profile <PROFILE>] --json
uv run --project <SKILL_DIR> python <SKILL_DIR>/scripts/pg_lake_storage.py \
  update-aws --role-arn <ARN> --sensitive-file <DESCRIBE_OUTPUT_FILE> \
  [--aws-profile <PROFILE>] --json
uv run --project <SKILL_DIR> python <SKILL_DIR>/scripts/pg_lake_storage.py \
  attach --instance <INST> --integration <NAME> --snowflake-connection <SF_CONN>
uv run --project <SKILL_DIR> python <SKILL_DIR>/scripts/pg_lake_storage.py \
  verify --instance <INST> --snowflake-connection <SF_CONN> --json
```

## Output

Routes to the correct workflow and returns the results from that sub-skill.

## Stopping Points Summary

| Operation | Approval Required |
|-----------|-------------------|
| CREATE instance | ⚠️ Yes (billable) |
| SUSPEND instance | ⚠️ Yes (drops connections) |
| Network policy changes | ⚠️ Yes |
| CREATE storage integration | ⚠️ Yes (cloud resources, ACCOUNTADMIN) |
| Update AWS trust policy | ⚠️ Yes (manual AWS step) |
| RESUME instance | No |
| LIST/DESCRIBE | No |
| Health check / diagnostics | No (readonly) |

**Resume rule:** On approval ("yes", "proceed", "approved"), continue without re-asking.

## Troubleshooting

**Error: `invalid property 'STORAGE_SIZE'`**
→ Use `STORAGE_SIZE_GB` (not `STORAGE_SIZE`)

**Error: `Missing option(s): [AUTHENTICATION_AUTHORITY]`**
→ Add `AUTHENTICATION_AUTHORITY = POSTGRES`

**Error: Network policy not working**
→ Verify rule uses `MODE = POSTGRES_INGRESS`

**Error: Connection refused**
→ IP not in network policy. Offer to check IP and add to policy.

## References

- `references/instance-options.md` - Valid compute families, storage limits
- `references/instance-states.md` - Instance state descriptions
- `references/documentation.md` - Official Snowflake docs URLs (fallback for commands not covered here)
- `references/thresholds.md` - Health check thresholds and recommended actions
