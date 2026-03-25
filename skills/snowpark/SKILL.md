---
name: snowpark-python
description: "**[REQUIRED]** Use for **ALL** requests related to deploying Snowpark Python scripts. Always load this skill first before handling any Snowpark Python deployment requests. Snowpark Python is Snowflake's Python API for data ingestion, transformation, user defined functions (UDF, UDAF, UDTF) and Python stored procedures (SP) deployment. Triggers: Snowpark, Python, UDF, Stored Procedure, snow snowpark CLI, Deploy." 
---

# Snowpark Python

## Project Structure

```
project/
├── src/
│   ├── __init__.py
│   ├── pipeline.py          # Main pipeline code
│   └── udf_def.py            # UDF definitions
├── tests/
│   ├── __init__.py
│   ├── conftest.py           # Shared fixtures
│   ├── test_pipeline.py      # Pipeline tests
│   └── data/
│       ├── input_sample.csv  # Test input data
│       └── expected_output.csv
├── configs.sql               # DDLs and permission grants
└── pyproject.toml
```

---

## Prerequisites

### Install `uv`
**Check if `uv` is installed** by running `uv --version`. If it's not installed, prompt the user to install it using one of these methods:
   - `curl -LsSf https://astral.sh/uv/install.sh | sh` (recommended)
   - `brew install uv` (macOS)
   - `pip install uv`

### Create a Python Project
Use `uv init` to create a Python project if the folder is empty.

### Create a Python Interpreter
Use `uv venv --python 3.13` to create Python environment.

### Install Python Packages
Use `uv add` to add project dependency packages.
Use `uv pip install` to install Python packages for development only.

---

## Primary Operations

These are the common operations users perform regularly. Route here confidently for any general Snowpark request.

### Plan the data pipeline
Plan the code in steps and explain to the user the plan. The plan may have these components depending the user's question but not limited to:
1. Load data from data sources.
2. Transform data. If this step is complex, break it down so it's easier for the user to understand.
3. Save the data to the destination table or stage location.

### Write Code
Use Snowpark Python Client to write code according to the above plan.

### Run and debug
Run the code using `uv` to try and fix the problems. Iterate until the problem is fixed.
If you find there are permission or other configuration problems, fix them and put the SQL statement in configs.sql.

### Deploy Code

Deploy stored procedures directly or as a snow snowpark CLI project. For more details, read `references/snowpark-deployment.md`.

### Data Flow Detection

If the user describes a data workflow, route to Primary tier:

**Common patterns:**
- **Write code:** "I need to build data pipeline using Snowpark Python Client" → Write Snowpark Code
- **Run and debug:** "Run the code", "Fix the error", "Debug my pipeline" → Run and debug code
- **Deploy code:** "Deploy my Python as stored procedure", "Deploy my project using snow snowpark CLI" → Deployment workflow

### Primary Routing Table

| User Language | Operation | Reference |
|---------------|-----------|-----------|
| Write code, build pipeline, load data, transform data, Snowpark basics, ETL, DataFrame | Write Snowpark Code | Not yet supported |
| Deploy, create procedure, register sproc, productionize, stored procedure from Python, snow snowpark, generate project, snowflake.yml, build and deploy | Deployment | `references/snowpark-deployment.md` |

**⚠️ MANDATORY** — If the tasks involves deploying Snowpark Python code, read `references/snowpark-deployment.md` first.
---

## Secondary Operations

Route here when the user language contains explicit problems or operational indicators. These operations may become complex.

**Only perform these when user explicitly asks.**

### Create Test Data

Write a Python program to generate the test data and run it only if the user asks.
Put the data into the folder `tests/data`.

### Create Test Code

Use `pytest` to write the test code. Put the test code in folder `tests`.

### Secondary Routing Table

| Explicit Indicators | Operation | Reference |
|---------------------|-----------|-----------|
| Create test data, generate sample data, mock data, test fixtures | Create Test Data | Not yet supported |
| Write tests, create test code, unit tests, test my code | Create Test Code | Not yet supported |
| Monitor, check status, query history, performance, execution time, resource usage | Monitoring | Not yet supported |
| Error, failing, debug, not working, fix, troubleshoot, why is it failing | Troubleshooting | Not yet supported |

---

## Compound Requests

If the user describes multiple operations:

1. Create a todo list capturing all requested operations
2. Ask the user to confirm the order:
   > "I've identified these tasks: [list]. What order would you like me to tackle them?"
3. Execute in confirmed order, completing each before moving to the next
4. Note: Some operations have natural dependencies (e.g., ingest before transform before deploy)

**Typical Customer Journey:**
```
Write Code → Deploy → Monitor → Troubleshoot (if needed)
```

---

## Reference Index

### Core Operations (Primary)

| Reference | Purpose | Status |
|-----------|---------|--------|
| `references/snowpark-deployment.md` | Deploy Python code as Snowflake stored procedures | Available |
| Write Code | Overview of Snowpark Python code: session, load, transform, save | Not yet supported |

### Detailed References

| Reference | Purpose | Status |
|-----------|---------|--------|
| Ingestion | Detailed ingestion: file formats, stages, external databases | Not yet supported |
| Transformation | Detailed transformations: joins, window functions, aggregations | Not yet supported |

### Operational (Secondary)

| Reference | Purpose | Status |
|-----------|---------|--------|
| Write Tests | Write tests with pytest (only when user asks) | Not yet supported |
| Monitoring | Check job status, query history, performance metrics | Not yet supported |
| Troubleshooting | Debug errors, diagnose failures, fix issues | Not yet supported |

---

## Stopping Points Summary

All references follow this philosophy: **NO changes without explicit user approval.**

- **READ-ONLY queries**: Can run freely (diagnostics, monitoring)
- **ANY mutation**: Requires stopping point and user approval

See individual references for specific stopping points.
