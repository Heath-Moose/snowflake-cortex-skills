# Native Apps Troubleshooting

## Declarative Sharing Issues

| Issue | Cause | Fix |
|-------|-------|-----|
| `schema shares udfs or stored procedures along with views or tables` | Mixed shared-by-copy and shared-by-reference in same schema | Move agents/UDFs/procedures to one schema, tables/views/semantic views to another |
| Objects work in provider, fail in consumer | Wrong schema layout | Separate shared-by-copy vs shared-by-reference objects into different schemas |
| Version not updating for consumers | Using wrong release commands | Use `PUT` + `ALTER RELEASE`, not `snow app version create` or `BUILD` |
| Consumer can't see shared objects | Objects missing from manifest | Verify all objects are listed in manifest with correct schema paths |
| `CREATE APPLICATION PACKAGE` fails with privilege error | Missing privilege on current role | Run `GRANT CREATE APPLICATION PACKAGE ON ACCOUNT TO ROLE <ROLE>` — check this BEFORE starting the workflow |
| Mixed BUILD and RELEASE commands cause confusion | Using `ALTER ... BUILD` for LIVE version | For declarative sharing with LIVE version, use `RELEASE LIVE VERSION` only. `BUILD` is for versioned CI/CD workflows |

## Notebook Issues

| Issue | Cause | Fix |
|-------|-------|-----|
| **SQL cells show syntax errors / interpreted as Python (MOST COMMON)** | **Missing `"metadata": {"language": "sql"}` on code cells** | **Every code cell MUST have `"metadata": {"language": "sql"}` or `"metadata": {"language": "python"}`. Without this, cells default to Python and SQL will not execute. Verify EVERY cell after generating a notebook.** |
| Notebook can't access provider's source data | Notebooks can only access data within the same app package | This is expected — notebooks are scoped to the application. Use `SCHEMA.TABLE` references (no database prefix) |
| Notebook shows "connecting" but never loads | Missing EAI or warehouse configuration | Consumer needs an active warehouse; verify notebook runtime settings |

## Cortex Agent Issues

| Issue | Cause | Fix |
|-------|-------|-----|
| **Agent queries fail / no results** | **`execution_environment` not set (MOST COMMON)** | **REQUIRED: Add `execution_environment` with `type: "warehouse"` and `warehouse: ""` (empty string) to tool_resources for ALL Analyst and custom (UDF/procedure) tools** |
| Analyst tool fails with missing warehouse | `execution_environment` not set or warehouse has a value | Add `execution_environment` with `warehouse: ""` (must be empty string, NOT an actual warehouse name) |
| `generic tool has empty execution environment` | Custom tool (UDF/procedure) missing `execution_environment` | Unlike Analyst tools (which can fall back to default warehouse), generic tools FAIL HARD without `execution_environment` — always include it |
| `Unknown user-defined function` on consumer | Using relative identifier (`SCHEMA.OBJECT`) in agent tool_resources | Use FQN with provider source DB name: `SOURCE_DB.SCHEMA.OBJECT` — Snowflake auto-rewrites the DB portion to the app name |
| Tool call error 370001 | Nested objects in input_schema | Flatten to primitive types only (string, number, boolean) |
| Tool call error 370001 with semantic view | Semantic view has verified_queries with FQN references | Remove verified_queries or use table aliases only (no FQN) |
| Consumer 404 on agent REST API | Wrong database in URL | Use APPLICATION NAME as database, not source database |
| Search tool not working | Limited declarative sharing support | Cortex Search has limited support in declarative shares |
| Agent can't be granted to share | Agent has tools in different database | All agent tool_resources must be in same database as agent |

## Manifest Issues

| Issue | Cause | Fix |
|-------|-------|-----|
| Function not found in consumer | Missing parameter types | Include types in manifest: `my_func(VARCHAR, NUMBER):` |
| Semantic view not accessible | Missing from manifest | Include ALL dependencies the agent references |

## Diagnostic Commands

```sql
-- Check package grants
SHOW GRANTS TO APPLICATION PACKAGE <PKG>;

-- Check what's in the share
DESCRIBE APPLICATION PACKAGE <PKG>;

-- Verify manifest uploaded
LIST @snow://package/<PKG>/versions/LIVE/;

-- Check versions
SHOW VERSIONS IN APPLICATION PACKAGE <PKG>;
```

## Consumer-Side Troubleshooting

### REST API Testing

**Recommended: Always try the Snowflake UI first** before resorting to REST API. The UI provides better error messages and is easier to debug.

If you need REST API testing, common issues include:

| Issue | Cause | Fix |
|-------|-------|-----|
| 401 Unauthorized | Invalid or expired PAT | Generate new PAT, verify token type header |
| 404 Not Found | Wrong endpoint or object path | Verify application name, schema, and object name |
| 390142 Invalid payload (REST API) | Wrong message format | REST API Agent: `"content": "string"`. REST API Analyst: `"content": [{"type": "text", "text": "..."}]` |
| `Request is malformed` (SQL `DATA_AGENT_RUN`) | Wrong content format in SQL function | `DATA_AGENT_RUN` requires array format: `"content": [{"type": "text", "text": "..."}]` — plain string `"content": "string"` FAILS (unlike REST API which accepts strings) |
| Response has warnings about verified_queries | FQN references in verified_queries | Expected behavior - Analyst removes problematic queries but continues |

### Cortex Agent REST API

```bash
curl -s -X POST "https://<CONSUMER_HOST>/api/v2/cortex/agent:run" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer <PAT>" \
  -H "X-Snowflake-Authorization-Token-Type: PROGRAMMATIC_ACCESS_TOKEN" \
  -d '{
    "model": "<APP_NAME>.<SCHEMA>.<AGENT_NAME>",
    "messages": [{"role": "user", "content": "What can you help me with?"}]
  }'
```

### Cortex Analyst REST API

```bash
curl -s -X POST "https://<CONSUMER_HOST>/api/v2/cortex/analyst/message" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer <PAT>" \
  -H "X-Snowflake-Authorization-Token-Type: PROGRAMMATIC_ACCESS_TOKEN" \
  -d '{
    "semantic_view": "<APP_NAME>.<SCHEMA>.<SEMANTIC_VIEW>",
    "messages": [{"role": "user", "content": [{"type": "text", "text": "What data is available?"}]}]
  }'
```

### Getting Consumer Account Info & Authentication

**Check `~/.snowflake/connections.toml` or `~/.snowflake/config.toml` first** — these often already contain the host, account, and token needed for REST API calls.

```toml
# Example ~/.snowflake/connections.toml
[my_connection]
account = "orgname-accountname"
host = "orgname-accountname.snowflakecomputing.com"
token = "..."
authenticator = "snowflake_jwt"  # or other auth method
```

If connection config is not available, fall back to SQL:

```sql
SELECT CURRENT_ACCOUNT_URL();  -- Host for REST API
SELECT CURRENT_ACCOUNT();      -- Account identifier
```

### PAT (Programmatic Access Token) Setup

If no token is found in connection config:

1. In Snowsight: User menu → My Profile → Programmatic Access Token
2. Create token with appropriate permissions
3. Use `X-Snowflake-Authorization-Token-Type: PROGRAMMATIC_ACCESS_TOKEN` header
