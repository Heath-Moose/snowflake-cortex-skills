# Snowpark Deployment

Deploy Python code to Snowflake as stored procedures, either via a `snow snowpark` CLI project or direct SQL.

## Table of Contents

1. [When to Load](#when-to-load)
2. [Prerequisites](#prerequisites)
3. [Workflow](#workflow)
   - [Step 1: Get Code to Validate](#step-1-get-code-to-validate)
   - [Step 2: Validate Code](#step-2-validate-code)
   - [Step 3: Choose Deployment Method](#step-3-choose-deployment-method)
4. [Type Mapping Reference](#type-mapping-reference)
5. [Method 1: Generate snow snowpark CLI Project](#method-1-generate-snow-snowpark-cli-project)
   - [Project Structure](#project-structure)
   - [File Templates](#file-templates)
   - [Project Workflow (Steps P1–P6)](#project-workflow)
   - [Project Examples](#project-examples)
6. [Method 2: Direct SQL Deployment](#method-2-direct-sql-deployment)
   - [SQL Template](#sql-template)
   - [Direct SQL Workflow (Steps D1–D6)](#direct-sql-workflow)
   - [Direct SQL Examples](#direct-sql-examples)
7. [Troubleshooting](#troubleshooting)
8. [Stopping Points Summary](#stopping-points-summary)

---

## When to Load

Route here when user wants to:
- Deploy Python code to Snowflake
- Generate a `snow snowpark` CLI compatible project
- Use `snow snowpark build` and `snow snowpark deploy`
- Execute CREATE PROCEDURE SQL directly

## Prerequisites

- Snowflake connection established (from session prerequisites)
- Customer has Python code or logic to deploy
- Target database/schema exists with CREATE PROCEDURE privilege

---

## Workflow

> **Convention:** Steps marked **BLOCKING** require an explicit user response before proceeding. NEVER skip these.

### Step 1: Get Code to Validate

**BLOCKING** — Get code before proceeding.

**Ask user:**

```
Please provide your Python code to deploy.
(Paste code directly or provide file path)
```

### Step 2: Validate Code — SP Handler Contract

**BLOCKING** — Must validate before choosing deployment method.

Snowflake stored procedures run inside a managed sandbox. Code that works locally will **fail at runtime** if it violates the SP handler contract. You MUST fix ALL of the following before deployment:

**SP Handler Contract — MANDATORY Rules:**

1. **`session: Session` as first parameter** — The runtime provides the session. The handler MUST accept it as the first parameter. NEVER instantiate a session (`Session.builder.create()`, `Session.builder.getOrCreate()`) inside the handler.
2. **No `os.environ` or environment variables** — Environment variables from the local machine are NOT available inside the SP runtime. Hardcode values or pass them as SP parameters.
3. **No `USE DATABASE` or `USE SCHEMA` in any form** — `session.sql("USE DATABASE ...")`, `session.sql("USE SCHEMA ...")`, `session.use_database()`, and `session.use_schema()` all throw `Unsupported statement type 'USE'` at runtime. Use fully-qualified table names instead (e.g., `DB.SCHEMA.TABLE`).
4. **No `session.close()`** — The runtime manages the session lifecycle. Calling `session.close()` will cause errors.
5. **Return value must match declared type** — If the return type is declared (e.g., `-> str`), the handler must return a matching value. Handlers declared as `-> None` do not need a return statement.

**Validation Checklist:**
- [ ] Handler function has `session: Session` as first parameter
- [ ] No `Session.builder.create()` or `Session.builder.getOrCreate()` inside handler
- [ ] No `os.environ` or `os.getenv` calls — values are hardcoded or passed as parameters
- [ ] No `session.sql("USE DATABASE ...")`, `session.sql("USE SCHEMA ...")`, `session.use_database()`, or `session.use_schema()` — only fully-qualified names
- [ ] No `session.close()` in the handler
- [ ] Handler returns a value matching the declared return type (or has `-> None` if no return is needed)
- [ ] No unsupported operations (local file I/O, network calls outside Snowflake)
- [ ] Only uses packages from Snowflake Anaconda channel

**Common pattern — converting local script to SP handler:**

Before (local script — will FAIL as SP):
```python
import os
from snowflake.snowpark import Session

DATABASE = os.environ["MY_DB"]
SCHEMA = os.environ["MY_SCHEMA"]

def main():
    session = Session.builder.config("connection_name", "default").create()
    session.use_database(DATABASE)
    session.use_schema(SCHEMA)
    df = session.table("MY_TABLE")
    df.write.mode("append").save_as_table("OUTPUT_TABLE")
    session.close()

if __name__ == "__main__":
    main()
```

After (valid SP handler):
```python
from snowflake.snowpark import Session

def main(session: Session) -> str:
    df = session.table("MY_DB.MY_SCHEMA.MY_TABLE")
    df.write.mode("append").save_as_table("MY_DB.MY_SCHEMA.OUTPUT_TABLE")
    return "Success"
```

**If validation passes:** Continue to Step 3

**If validation fails:** Apply the SP Handler Contract rules (#1-#5) above to fix violations. Refer to the [Troubleshooting](#troubleshooting) section in this document for common errors.

### Step 3: Choose Deployment Method

**BLOCKING** — Get user selection before proceeding.

**Ask user:**

```
How would you like to deploy?

1. **Generate snow snowpark CLI project** (Recommended)
   - Creates project folder with snowflake.yml
   - Deploy with: snow snowpark build && snow snowpark deploy
   - Best for: version control, CI/CD, reusable code

2. **Execute SQL directly**
   - Runs CREATE PROCEDURE SQL immediately
   - Best for: quick deployments, simple code
```

**Route based on choice:**

| Choice | Section |
|--------|---------|
| Generate snow snowpark CLI project | [Method 1](#method-1-generate-snow-snowpark-cli-project) |
| Execute SQL directly | [Method 2](#method-2-direct-sql-deployment) |

---

## Type Mapping Reference

| Python Type | snowflake.yml | SQL Type |
|-------------|---------------|----------|
| `str` | `string` | `VARCHAR` |
| `int` | `int` | `INTEGER` |
| `float` | `float` | `FLOAT` |
| `bool` | `boolean` | `BOOLEAN` |
| `list` | `array` | `ARRAY` |
| `dict` | `variant` | `VARIANT` |

---

## Method 1: Generate snow snowpark CLI Project

Generate a complete project that can be built and deployed using `snow snowpark build` and `snow snowpark deploy`.

> **CRITICAL: `--connection` flag is REQUIRED for all `snow` CLI commands. Commands will FAIL without it.**

### Project Structure

```
<name>/
├── <name>/
│   ├── __init__.py
│   └── procedure.py
├── requirements.txt
└── snowflake.yml
```

### File Templates

#### `snowflake.yml`

```yaml
definition_version: 1
snowpark:
  project_name: "<project_name>"
  stage_name: "<schema>.<stage>"
  src: "<name>/"
  procedures:
    - name: "<name>"
      database: "<database>"
      schema: "<schema>"
      handler: "procedure.main"
      runtime: "3.10"
      signature:
        - name: "<param_name>"
          type: "<snowflake_type>"
      returns: string
```

**Signature Patterns:**

```yaml
# No parameters
signature: ""

# Single parameter
signature:
  - name: "table_name"
    type: "string"

# Multiple parameters
signature:
  - name: "table_name"
    type: "string"
  - name: "threshold"
    type: "int"
```

#### `requirements.txt`

```
snowflake-snowpark-python
```

Add additional packages as needed (must be in Snowflake Anaconda channel).

#### `<name>/__init__.py`

Empty file — makes directory a Python module.

#### `<name>/procedure.py`

```python
from snowflake.snowpark import Session
import snowflake.snowpark.functions as F

def main(session: Session, <params>) -> str:
    """<Description>"""
    # Implementation
    return "Success"

if __name__ == '__main__':
    with Session.builder.getOrCreate() as session:
        import sys
        if len(sys.argv) > 1:
            print(main(session, *sys.argv[1:]))  # type: ignore
        else:
            print(main(session))  # type: ignore
```

---

### Project Workflow

#### Step P1: Gather Requirements — BLOCKING

**Ask user:**
```
Please provide the following:

1. **Name**: What should it be called? (e.g., process_data_sp)
2. **Database/Schema**: Where to deploy? (e.g., MY_DB.MY_SCHEMA)
3. **Stage**: Stage for deployment? (e.g., MY_SCHEMA.deployment)
4. **Output location**: Where to create the project? (default: ./<name>/)
```

Wait for user to provide all information before proceeding.

#### Step P2: Create All Files

1. Create project directory: `<name>/`
2. Create module subdirectory: `<name>/<name>/`
3. Write `snowflake.yml`
4. Write `requirements.txt`
5. Write `__init__.py` (empty)
6. Write `procedure.py`

#### Step P3: Present Summary

```
Created project: <name>/

<name>/
├── snowflake.yml
├── requirements.txt
└── <name>/
    ├── __init__.py
    └── procedure.py
```

#### Step P4: Ask About Deployment — BLOCKING

**Ask user:**
```
Would you like me to build and deploy now? (Yes/No)
```

**If No:** Done. User can run manually later.

**If Yes:** Continue to Step P5.

#### Step P5: Get Connection and Warehouse — BLOCKING

**Ask user:**
```
Please provide:
1. **Connection** (REQUIRED): Which snow CLI connection? (e.g., dev, prod)
   - Default: Use connection from session prerequisites if established
2. **Warehouse**: Which warehouse for deployment? (e.g., COMPUTE_WH)
```

#### Step P6: Execute Build and Deploy

**`--connection` is REQUIRED — commands fail without it.**

```bash
cd <project_path>
snow snowpark build --connection <CONNECTION> --warehouse <WAREHOUSE>
snow snowpark deploy --connection <CONNECTION> --warehouse <WAREHOUSE>
```

**If build/deploy fails:** Refer to the [Troubleshooting](#troubleshooting) section. Detailed troubleshooting reference is not yet supported.

**Verify deployment:**
```sql
SHOW PROCEDURES LIKE '<name>' IN SCHEMA <database>.<schema>;
```

---

### Project Examples

#### Example: No Parameters

**Request:** "Count rows in ORDERS table"

**Project:** `count_orders_sp/`

`snowflake.yml`:
```yaml
definition_version: 1
snowpark:
  project_name: "my_project"
  stage_name: "public.deployment"
  src: "count_orders_sp/"
  procedures:
    - name: "count_orders_sp"
      database: "my_db"
      schema: "public"
      handler: "procedure.main"
      runtime: "3.10"
      signature: ""
      returns: string
```

`count_orders_sp/procedure.py`:
```python
from snowflake.snowpark import Session

def main(session: Session) -> str:
    count = session.table("ORDERS").count()
    return f"ORDERS table has {count} rows"

if __name__ == '__main__':
    with Session.builder.getOrCreate() as session:
        print(main(session))
```

**Deploy:**
```bash
snow snowpark build --connection dev --warehouse COMPUTE_WH
snow snowpark deploy --connection dev --warehouse COMPUTE_WH
```

```sql
CALL count_orders_sp();
-- Returns: "ORDERS table has 1523 rows"
```

---

#### Example: With Parameters

**Request:** "Filter and save data"

**Project:** `filter_data_sp/`

`snowflake.yml`:
```yaml
definition_version: 1
snowpark:
  project_name: "my_project"
  stage_name: "public.deployment"
  src: "filter_data_sp/"
  procedures:
    - name: "filter_data_sp"
      database: "my_db"
      schema: "public"
      handler: "procedure.main"
      runtime: "3.10"
      signature:
        - name: "source_table"
          type: "string"
        - name: "min_amount"
          type: "int"
      returns: string
```

`filter_data_sp/procedure.py`:
```python
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col

def main(session: Session, source_table: str, min_amount: int) -> str:
    df = session.table(source_table)
    filtered = df.filter(col("AMOUNT") > min_amount)
    
    count = filtered.count()
    filtered.write.mode("overwrite").save_as_table(f"{source_table}_FILTERED")
    
    return f"Saved {count} rows to {source_table}_FILTERED"

if __name__ == '__main__':
    with Session.builder.getOrCreate() as session:
        print(main(session, "ORDERS", 100))
```

**Deploy:**
```bash
snow snowpark build --connection dev --warehouse COMPUTE_WH
snow snowpark deploy --connection dev --warehouse COMPUTE_WH
```

```sql
CALL filter_data_sp('ORDERS', 100);
-- Returns: "Saved 456 rows to ORDERS_FILTERED"
```

---

## Method 2: Direct SQL Deployment

Execute CREATE PROCEDURE SQL directly to deploy Python code to Snowflake.

**When to use:**
- User wants immediate deployment without project setup
- Has simple, single-file code
- Prefers direct execution over generating files

### SQL Template

```sql
CREATE OR REPLACE PROCEDURE <DATABASE>.<SCHEMA>.<NAME>(
    <param_name> <SNOWFLAKE_TYPE>
)
RETURNS <RETURN_TYPE>
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = '<function_name>'
AS
$$
<PYTHON_CODE>
$$;
```

---

### Direct SQL Workflow

#### Step D1: Gather Requirements — BLOCKING

**Ask user:**
```
Please provide the following:

1. **Name**: What should it be called? (e.g., process_data_sp)
2. **Database/Schema**: Where to deploy? (e.g., MY_DB.MY_SCHEMA)
```

#### Step D2: Generate SQL

Generate the CREATE PROCEDURE SQL using the template above, incorporating the validated code from Step 2.

#### Step D3: Review and Ask About Deployment — BLOCKING

**Present generated SQL:**
```
I've generated the following deployment SQL:

[SHOW GENERATED SQL]

Does this look correct? Would you like me to deploy it?
- Yes: Deploy now
- No: [specify what to change or stop here]
```

**If No:** Done. User can run SQL manually.

**If Yes:** Continue to Step D4.

#### Step D4: Get Warehouse — BLOCKING

**Ask user:**
```
Which warehouse should be used for deployment? (e.g., COMPUTE_WH)
```

#### Step D5: Execute Deployment

**Set warehouse and execute:**
```sql
USE WAREHOUSE <WAREHOUSE>;
```

Then execute the CREATE PROCEDURE SQL.

**If execution fails:** Refer to the [Troubleshooting](#troubleshooting) section. Detailed troubleshooting reference is not yet supported.

**Verify deployment:**
```sql
SHOW PROCEDURES LIKE '<NAME>' IN SCHEMA <DATABASE>.<SCHEMA>;
```

#### Step D6: Test (Optional) — BLOCKING

**Ask user:**
```
Deployed successfully! Would you like to test it now?
- Yes: [provide test parameters]
- No: I'll test it myself
```

**Test:**
```sql
CALL <DATABASE>.<SCHEMA>.<NAME>(<test_params>);
```

---

### Direct SQL Examples

#### Example: No Parameters

**Request:** "Deploy a procedure to count orders"

```sql
CREATE OR REPLACE PROCEDURE MY_DB.PUBLIC.COUNT_ORDERS()
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
AS
$$
from snowflake.snowpark import Session

def main(session: Session) -> str:
    count = session.table("ORDERS").count()
    return f"ORDERS table has {count} rows"
$$;
```

```sql
CALL MY_DB.PUBLIC.COUNT_ORDERS();
-- Returns: "ORDERS table has 1523 rows"
```

---

#### Example: With Parameters

**Request:** "Deploy a procedure to filter and save data"

```sql
CREATE OR REPLACE PROCEDURE MY_DB.PUBLIC.FILTER_DATA(
    SOURCE_TABLE VARCHAR,
    MIN_AMOUNT INTEGER
)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
AS
$$
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col

def main(session: Session, source_table: str, min_amount: int) -> str:
    df = session.table(source_table)
    filtered = df.filter(col("AMOUNT") > min_amount)
    
    count = filtered.count()
    filtered.write.mode("overwrite").save_as_table(f"{source_table}_FILTERED")
    
    return f"Saved {count} rows to {source_table}_FILTERED"
$$;
```

```sql
CALL MY_DB.PUBLIC.FILTER_DATA('ORDERS', 100);
-- Returns: "Saved 456 rows to ORDERS_FILTERED"
```

---

#### Example: With Additional Packages

**Request:** "Deploy a procedure using pandas"

```sql
CREATE OR REPLACE PROCEDURE MY_DB.ANALYTICS.ANALYZE_DATA(
    TABLE_NAME VARCHAR
)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('snowflake-snowpark-python', 'pandas')
HANDLER = 'main'
AS
$$
from snowflake.snowpark import Session
import pandas as pd

def main(session: Session, table_name: str) -> str:
    df = session.table(table_name).to_pandas()
    
    stats = {
        "rows": len(df),
        "columns": len(df.columns),
        "memory_mb": round(df.memory_usage(deep=True).sum() / 1024 / 1024, 2)
    }
    
    return f"Table {table_name}: {stats}"
$$;
```

```sql
CALL MY_DB.ANALYTICS.ANALYZE_DATA('CUSTOMERS');
-- Returns: "Table CUSTOMERS: {'rows': 1000, 'columns': 10, 'memory_mb': 0.5}"
```

---

## Troubleshooting

> **TODO:** When `ops-troubleshoot.md` is added, update all "not yet supported" references in this file to link to it.

| Error | Fix |
|-------|-----|
| **`Unsupported statement type 'USE'`** | Remove all `session.sql("USE DATABASE ...")` and `session.sql("USE SCHEMA ...")` calls. Use fully-qualified table names (`DB.SCHEMA.TABLE`) instead. |
| **`NameError: name 'os' is not defined`** or env var errors | Remove `os.environ`/`os.getenv` calls. Hardcode values or pass as SP parameters. |
| **Session-related errors at runtime** | Remove `Session.builder.create()` from handler. Accept `session: Session` as first parameter. |
| **Stage not found** | `CREATE STAGE IF NOT EXISTS <database>.<schema>.<stage>;` |
| **Package not found** | Verify spelling, check [Snowflake Anaconda Channel](https://repo.anaconda.com/pkgs/snowflake/), remove unsupported packages |
| **snow CLI not installed** | `brew install snowflake-cli` |
| **Python runtime version not supported** | Try `RUNTIME_VERSION = '3.11'` or `'3.10'` or `'3.9'` |
| **Handler function not found** | Handler name must match function name exactly. For inline SQL, use just the function name (no module prefix) |

**Permission errors:**
```sql
GRANT USAGE ON DATABASE <db> TO ROLE <role>;
GRANT USAGE ON SCHEMA <db>.<schema> TO ROLE <role>;
GRANT CREATE PROCEDURE ON SCHEMA <db>.<schema> TO ROLE <role>;
```

**For more errors:** Detailed troubleshooting reference (`ops-troubleshoot.md`) is not yet supported.

---

## Stopping Points Summary

All steps marked **BLOCKING** require an explicit user response before proceeding.

**Common steps (both methods):**
- Step 1: Wait for user to provide code
- Step 2: Validate code before proceeding
- Step 3: Wait for user to choose deployment method

**Project method (Method 1):**
- Step P1: Wait for requirements (name, database, schema, stage)
- Step P4: Wait for user to confirm build/deploy
- Step P5: Wait for connection and warehouse
- Step P6: Execute with `--connection` flag (REQUIRED). If fails, refer to the [Troubleshooting](#troubleshooting) section.

**Direct SQL method (Method 2):**
- Step D1: Wait for requirements (name, database, schema)
- Step D3: Wait for user to confirm deployment
- Step D4: Wait for warehouse
- Step D5: If fails, refer to the [Troubleshooting](#troubleshooting) section.
- Step D6: Wait before running test
