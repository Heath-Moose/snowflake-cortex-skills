---
name: migrate-pyspark-to-snowpark-connect
description: |
  Migrate PySpark and Databricks workloads to Snowflake SCOS (Snowpark Connect for Spark).
  Use when: converting Spark code to run on Snowflake, analyzing PySpark compatibility,
  updating imports to Spark Connect equivalents, or migrating from Databricks.
  Triggers: migrate pyspark, convert spark, scos migration,
  spark connect, pyspark compatibility, snowpark connect.
parent_skill: snowpark-connect
allowed-tools: Read, Write, Bash
---


# Migrate PySpark to SCOS

Migrate a PySpark workload to be compatible with Snowflake SCOS (Snowpark Connect for Spark).

## When to Load

[snowpark-connect] Intent Detection: After user indicates migration intent (convert, migrate, update imports, rewrite for SCOS).

## Arguments

- `$ARGUMENTS` - Path to the PySpark file or directory to migrate

## Prerequisites

### uv Package Manager

Check if uv is installed:
```bash
uv --version
```

If not installed:
```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

### Snowflake Connection

A valid Snowflake connection must be configured. The default connection name is `default`, but you can specify a different name using `--connection <name>`.

### RAG Knowledge Base

The analyzer requires a Cortex Search Service with known SCOS compatibility issues. Step 0 will check and initialize this automatically if needed.

## Tools

### Tool: analyze_pyspark.py

**Description**: Analyzes PySpark scripts for SCOS compatibility issues using RAG-based pattern matching and LLM validation.

**Usage:**
```bash
uv run --project <SKILL_DIRECTORY> \
  python <SKILL_DIRECTORY>/scripts/analyze_pyspark.py \
  --path <FILE_OR_DIR> \
  --output-format json > analysis.json
```

**Arguments:**
- `--path`: Path to PySpark file or directory (required)
- `--output-format`: Output format - `text` or `json` (default: text)
- `--risk-threshold`: Minimum risk to report 0-1 (default: 0.1)
- `--connection`: Snowflake connection name (default: default)
- `--rag-backend`: RAG backend - `cortex` (Snowflake Cortex Search, default) or `remote` (HTTP endpoint)

**When to use:** First step of any migration

## Workflow

You are an expert migration agent specializing in converting PySpark workloads to run on Snowflake SCOS (Snowpark Connect for Spark). 
Your goal is to produce a functional, SCOS-compatible version of the provided code while preserving the original business logic.
You **MUST** perform the steps below **STEP by STEP**.

### Step 0: Setup RAG Resources (One-Time)

**Goal:** Ensure the RAG knowledge base exists for compatibility analysis.

**Check if Cortex Search RAG is already initialized:**
```bash
uv run --project <SKILL_DIRECTORY> \
  python -c "
from snowflake.snowpark import Session
session = Session.builder.config('connection_name', 'default').create()
result = session.sql('''
SELECT COUNT(*) as cnt FROM SCOS_MIGRATION.INFORMATION_SCHEMA.CORTEX_SEARCH_SERVICES 
WHERE SERVICE_NAME = 'SCOS_COMPAT_ISSUES_SERVICE'
''').collect()
print('EXISTS' if result[0]['CNT'] > 0 else 'NOT_FOUND')
"
```

**If output is `EXISTS`**, use `--rag-backend cortex` in Step 1. Skip the rest of Step 0.

**If output is `NOT_FOUND`**:

⚠️ **MANDATORY STOPPING POINT** - Do NOT proceed without user input.

Ask the user:
```
The RAG knowledge base is not set up yet. I need to initialize it once.

Please provide your Snowflake warehouse name for creating the Cortex Search Service:
```

Wait for the user to provide the warehouse name, then run:
```bash
uv run --project <SKILL_DIRECTORY> \
  python <SKILL_DIRECTORY>/scripts/rag/scos_rag.py --warehouse <USER_PROVIDED_WAREHOUSE>
```

If setup **succeeds**, use `--rag-backend cortex` in Step 1.

If setup **fails** (e.g., permission error, warehouse not found, Cortex Search not available in the region), fall back to `--rag-backend remote` in Step 1. The remote backend uses a hosted HTTP endpoint and requires no Snowflake RAG setup.

**If the check itself fails** (e.g., connection error, missing database), also fall back to `--rag-backend remote` in Step 1.

**Note:** Cortex Search setup only needs to run once per Snowflake account. Subsequent migrations will reuse the existing RAG resources.

### Step 1: Analyze the Workload

Run the compatibility analysis tool to detect issues and output them to a JSON file. Use the RAG backend determined in Step 0:

```bash
uv run --project <SKILL_DIRECTORY> \
  python <SKILL_DIRECTORY>/scripts/analyze_pyspark.py \
  --path $ARGUMENTS --output-format json --rag-backend <cortex|remote> > analysis.json
```

**Wait for the analysis to complete.**

Then, read the `analysis.json` file. It contains a list of potential compatibility issues with the following structure:

```json
[
  {
    "file": "src/etl/transformations.py",
    "lines": "142-142",
    "code": "combined = df1.unionByName(df2, allowMissingColumns=True)",
    "final_risk": 0.4,
    "root_cause": "unionByName with allowMissingColumns may fail if there are type mismatches between corresponding columns in the two DataFrames",
    "explanation": "This code may fail if the DataFrames have columns with matching names but incompatible types. If schemas are compatible or only missing columns exist, it should work correctly.",
    "fix": "Ensure column types match between DataFrames before union, or explicitly cast columns to compatible types",
    "confidence": "MEDIUM"
  }
]
```

**Fields**:

- `file`: Path to the source file
- `lines`: Line range of the problematic code
- `code`: The code snippet flagged for review
- `final_risk`: Float (0.0-1.0) indicating failure probability
- `root_cause`: Why this code may fail in SCOS
- `explanation`: Detailed explanation of the risk
- `fix`: Suggested fix (may be `null` if no direct fix)
- `confidence`: Prediction confidence (HIGH/MEDIUM/LOW)


### Step 2: Create Migration Copy and File Manifest

Do not modify the original files. Create a copy for migration:

```bash
# For a single file:
cp $ARGUMENTS ${ARGUMENTS%.py}_scos.py

# For a directory:
cp -r $ARGUMENTS ${ARGUMENTS}_scos
```

If it is a directory, do not add or remove any files from the copy. Both directories MUST have exactly the same structure.

#### 2.1 Build the file manifest

Immediately after copying, enumerate **every** `.py` file in the migrated copy. This manifest is the single source of truth for all subsequent steps — every file in it MUST be processed.

```bash
# For a single file:
echo "${ARGUMENTS%.py}_scos.py"

# For a directory:
find ${ARGUMENTS}_scos -name "*.py" -type f | sort
```

Record this list. You will use it in Steps 3, 4, 5, and 6 to ensure no file is missed.

#### 2.2 Map analysis issues to files

Cross-reference the `analysis.json` issues against the file manifest. For each file in the manifest, determine:
- **Has issues**: the file appears in `analysis.json` → will be processed in Step 3
- **No issues reported**: the file does NOT appear in `analysis.json` → still needs Steps 4, 5, and 6

```
File Manifest:
  ✎ src/etl/transformations.py  — 3 issues from analysis
  ✎ src/etl/loader.py           — 1 issue from analysis
  · src/utils/helpers.py         — no issues (still needs import/header updates)
  · src/config.py                — no issues (still needs import/header updates)
  · src/__init__.py              — no issues (still needs import/header updates)
```

**Every file marked `·` (no issues) still MUST be processed in Steps 4 and 5.** The analysis tool only flags compatibility risks — it does not check for imports or session creation that need updating.

### Step 3: Apply Fixes from the Analysis output

**For EACH issue in `analysis.json`**, perform the following:

1. **Locate the issue**: Find the code at `file` and `lines` in the **copied** directory.
2. **Assess the risk**: Check the `final_risk` value.
3. **Apply the appropriate action** based on the rules below.
4. **Document the action**: Next to the code chunk that you've just processed **ALWAYS** add a code comment explaining the potential issue root cause and explain the decision you have made. Add a comment regardless of whether you have decided to apply a fix or not — **except** for no-op operations and no-op configs, which should be left as-is without any comment (see General Rules 4 and 5). Use one of these prefixes so the validation skill can parse them:
   - `# SCOS: <explanation>` — fix applied or issue reviewed (no action needed)
   - `# SCOS: TODO - <explanation>` — requires manual review; could not be auto-fixed
   - `# SCOS: Performance tip - <explanation>` — optimization recommendation

**Rules for Fixing based on Risk Score:**
1. **Must Fix (`final_risk` >= 0.7)**: These are critical compatibility issues. You **MUST** apply a fix or rewrite the logic. If no direct fix is available, you must rewrite the code to avoid the unsupported feature. If a rewrite is not feasible, add `# SCOS: TODO - <explanation>` so the validation skill flags it.
2. **Should Fix (0.3 <= `final_risk` < 0.7)**: These are likely issues. You **SHOULD** apply a fix if one is suggested. If unsure, add `# SCOS: TODO - <explanation>` to flag it for manual review.
3. **Fix if possible (`final_risk` < 0.3)**: These are minor risks or potential false positives. You **MUST still review them** and apply a fix if possible. If the code is safe, just add a comment `# SCOS: <explanation>`.

**General Rules:**
1.  **Use the Tool's Fix**: If the issue object provides a `fix` value, use it. It is tailored to the specific error.
2.  **Handle RDDs**: RDD operations (`final_risk` near 1.0) are not supported. You MUST rewrite them using DataFrame transformations or SQL expressions. **Read** `references/rdd-conversion.md` for detailed conversion rules and examples.
3.  **Unsupported Formats**: Change file formats if required (e.g., ORC/Avro -> Parquet).
4.  **No-Op Operations**: Operations like `hint()`, `repartition()`, or `coalesce()` are silently ignored in SCOS — they have no effect but do not cause errors. Leave this code as-is without adding any comment. No code change or annotation is needed.
5.  **No-Op Configs**: Spark configs that are not supported by SCOS (category: "No-Op Config") are silently ignored — they have no effect but do not cause errors. Leave this code as-is without adding any comment. No code change or annotation is needed. Common no-op configs include `spark.sql.shuffle.partitions`, `spark.executor.memory`, `spark.driver.memory`, `spark.sql.adaptive.enabled`, etc.
6.  **Missing Fixes**: If `fix` is null, use the `root_cause` to determine the best workaround. If unsure, add a TODO comment: `# SCOS: TODO - <explanation>`.
7.  **File Reads**: For file read operations (`.read.csv`, `.read.json`, `.read.parquet`, `.load`), check the path being read:
    -   **Already using Snowflake stage** (`@STAGE_NAME/...` or `@~/...`): No comment needed, this is optimal.
    -   **External cloud storage** (paths starting with `s3://`, `s3a://`, `gs://`, `abfs://`, `wasb://`, `adl://`): Add performance comment recommending Snowflake stage upload.
    -   **Local paths or variables**: If the path is a variable, trace it to determine if it's external cloud storage. Add performance comment recommending Snowflake stage upload for both.
    
    ```python
    # SCOS: Performance tip - Consider uploading this file to a Snowflake stage
    # for faster processing. Use: session.file.put("local_path", "@STAGE_NAME/path")
    df = spark.read.csv("s3://bucket/path/file.csv", header=True)
    ```
8.  **Snowflake Connector Pushdown (Recommended)**: If code uses the Spark Snowflake Connector (`.format("snowflake")` with `.options(...)` and `.load()`), recommend replacing it with `SnowflakeSession.sql()`. The connector is **supported and functional** in SCOS, but `SnowflakeSession` provides a better experience: simpler code, no connector config boilerplate, and direct use of the Snowpark Connect session. Since this is a recommendation (not a required fix), add a comment with the complete suggested replacement code while keeping the original code intact.

    **BEFORE:**
    ```python
    rest_data_info = spark.read \
       .format("snowflake") \
       .options(**sfOptions) \
       .option("sfDatabase", "BRAND_PLK") \
       .option("sfSchema", "STORES") \
       .option("sfWarehouse", "ANALYSIS_PLK") \
       .option("query", f"""
           select store_id as rest_no, full_address as rest_address
           from STORES where status = 'OPEN'
       """) \
       .load()
    ```

    **Comment with suggested replacement:**
    ```python
    # SCOS: Recommended improvement - The Snowflake Connector (.format("snowflake")) works
    # in SCOS but SnowflakeSession.sql() provides a better experience. Suggested replacement:
    #
    #   from snowflake.snowpark_connect.snowflake_session import SnowflakeSession
    #   snowflake_session = SnowflakeSession(spark)
    #   snowflake_session.sql("USE DATABASE BRAND_PLK").collect()
    #   snowflake_session.sql("USE SCHEMA STORES").collect()
    #   snowflake_session.sql("USE WAREHOUSE ANALYSIS_PLK").collect()
    #   rest_data_info = snowflake_session.sql("""
    #       select store_id as rest_no, full_address as rest_address
    #       from STORES where status = 'OPEN'
    #   """)
    rest_data_info = spark.read \
       .format("snowflake") \
       .options(**sfOptions) \
       ...
       .load()
    ```

    **Key mapping rules for the suggestion:**
    - Extract the SQL from `.option("query", ...)` and pass it to `snowflake_session.sql()`
    - If `.option("dbtable", "TABLE_NAME")` is used instead of `query`, suggest `snowflake_session.sql("SELECT * FROM TABLE_NAME")`
    - Map `sfDatabase`, `sfSchema`, `sfWarehouse` options to `USE DATABASE/SCHEMA/WAREHOUSE` statements
    - The `from snowflake.snowpark_connect.snowflake_session import SnowflakeSession` import should appear once per file

10.  **UDF Serialization (ALL UDF patterns: `udf()`, `@udf`, `@pandas_udf`, `applyInPandas`, `mapInPandas`, factory-style `udf()` calls)**: When the workload uses UDFs that call helper functions, reference module-level variables, or import external modules, these will fail on Snowflake's server-side worker because cloudpickle serializes function references that point to the workload module (which doesn't exist on the server). **Read** `references/udf-dependencies.md` (Part 2) for the tiered fix approach:
    - **Tier 1 (Preferred)**: Use `snowpark.connect.udf.packages` for Anaconda packages and `snowpark.connect.udf.python.imports` for custom modules uploaded to a stage. Import inside the UDF body.
    - **Tier 2**: For UDFs with simple logic (including factory-style `udf()` calls that return `udf(fn, type)`), keep all logic self-contained (inline) inside the closure body. Move all imports (`import datetime`, `import ast`, etc.), constants, and helper functions inside the UDF function body so cloudpickle captures them by value. Do NOT replace working UDFs with built-in SQL functions — apply the minimal fix to make the closure self-contained.
    - **Tier 3**: For complex UDFs that call many tightly-coupled helper functions in the same file, use the factory function pattern (to capture data in closures) and `__module__ = "__main__"` patching (to force serialization by value) on the UDF and **all** helper functions in its call chain.

    ```python
    # Example: Tier 3 — factory + __module__ patching
    def make_process_udf(config_dict):
        """Factory captures config in closure."""
        def process_udf(pdf):
            result = helper_a(pdf, config_dict)
            return helper_b(result)
        return process_udf

    process_udf = make_process_udf(my_config)
    for _fn in [process_udf, helper_a, helper_b]:
        _fn.__module__ = "__main__"

    result = df.groupby("key").applyInPandas(process_udf, schema=output_schema)
    ```

10. **Server-Side Package Availability**: When UDFs import third-party packages, verify they are available in Snowflake's Anaconda channel or use PyPI via artifact repository. **Read** `references/udf-dependencies.md` (Part 1) for details. If a package is missing from Anaconda:
    - Use PyPI via artifact repository (recommended): `spark.conf.set("snowpark.connect.artifact_repository", "snowflake.snowpark.pypi_shared_repository")`
    - Or replace with a stdlib/numpy-only implementation.
    - Or upload a pure-Python package via `snowpark.connect.udf.python.imports`.

    To check Anaconda availability:
    ```sql
    SELECT * FROM INFORMATION_SCHEMA.PACKAGES
    WHERE LANGUAGE = 'python' AND PACKAGE_NAME ILIKE '%<package>%';
    ```

    To use PyPI:
    ```python
    spark.conf.set("snowpark.connect.artifact_repository", "snowflake.snowpark.pypi_shared_repository")
    spark.conf.set("snowpark.connect.udf.packages", "[package1, package2]")
    ```

#### Issue Processing Checklist

After processing all issues from `analysis.json`, verify completeness:

- [ ] Every issue in `analysis.json` has been reviewed
- [ ] All high-risk issues (`final_risk` >= 0.7) have fixes applied
- [ ] All medium-risk issues (`final_risk` >= 0.3) have fixes or TODO comments
- [ ] All low-risk issues (`final_risk` < 0.3) have fixes or TODO comments

#### Files with No Issues

For files in the manifest (Step 2.2) that had **no issues** reported by the analysis tool: no changes are needed in this step. These files will still be processed in Steps 4 and 5 for import updates and migration headers. Confirm you have accounted for them:

```
Step 3 Summary:
  Files with fixes applied: N
  Files with no issues:     M
  Total in manifest:        N + M  ← must match Step 2.1 count
```

**Do NOT proceed to Step 4 until ALL issues have been addressed and the file count is confirmed.**

### Step 4: Update Imports and Session Creation

SCOS requires using the Snowpark Connect client. You must update imports and session initialization.

**⚠️ CRITICAL: You MUST apply Steps 4 and 5 to EVERY `.py` file in the file manifest from Step 2.1.** Process them one at a time and track completion:

```
Step 4 Progress:
  [x] src/etl/transformations.py  — imports updated, session updated
  [x] src/etl/loader.py           — imports updated, no session creation
  [ ] src/utils/helpers.py         — pending
  [ ] src/config.py                — pending
  [ ] src/__init__.py              — pending (no PySpark imports, header only)
```

**Do NOT proceed to Step 5 until every file is checked off.**

#### 4.1 Update Session Initialization
**Identify the main entry point of the application.**

Initialize the Snowpark Connect session **ONLY ONCE** in the main entry point (e.g., `main.py` or the primary script).

**In the main entry point ONLY, replace session creation with:**
```python
from snowflake import snowpark_connect

spark = snowpark_connect.init_spark_session()
```

**In all other files:**
- Remove redundant session initialization.
- Ensure the file uses the active session (e.g., via `snowpark_connect.get_session()` after updating imports - make sure there is `from snowflake import snowpark_connect` import, or by passing the `spark` object).

#### 4.2 Remove Unsupported Imports
**For EACH Python file**, remove imports that are NOT supported in SCOS.

**Imports to REMOVE:**

| Unsupported Import | Action |
| :--- | :--- |
| `databricks.connect` | Remove - use `snowpark_connect` in entry point |
| `databricks.sdk.runtime` | Remove |
| `delta.tables` | Remove - Delta format not supported |

**Example Transformation:**
```python
# BEFORE
from pyspark.sql import SparkSession
from databricks.connect import DatabricksSession
from databricks.sdk.runtime import dbutils

# AFTER
from pyspark.sql import SparkSession
# databricks imports removed - not supported in SCOS
```

**Note:** Standard PySpark imports (`pyspark.sql.functions`, `pyspark.sql.types`, etc.) are generally supported and do NOT need to be changed.

### Step 5: Add Migration Header

**For EACH Python file in the file manifest from Step 2.1**, add a docstring in the following format at the very top:

```python
"""
SCOS Migration Output
=====================
Source File: [Insert original file path, e.g., $ARGUMENTS/filename.py]
Migrated on: [Insert Current Date, e.g., 2023-10-27]

Changes Overview:
- [Lines 10-12] Replaced legacy SparkSession initialization with snowpark_connect.
- [Lines 45-50] Updated import statements to use Spark Connect equivalents.
- [Lines 88-92] [Description of another fix applied]

Known Limitations:
- [List every # SCOS: TODO item in this file, with line numbers and descriptions]
- [If none, write "None — all issues resolved"]
"""
```

For files with **no changes** (no analysis issues, no import updates), use:
```python
"""
SCOS Migration Output
=====================
Source File: $ARGUMENTS/filename.py
Migrated on: [Current Date]

Changes Overview:
- No compatibility issues detected. No changes required.

Known Limitations:
- None — all issues resolved
"""
```

**IMPORTANT:** Every change listed in the 'Changes Overview' must be prefixed with the specific line numbers affected (e.g., [Lines 12-15]).

Track completion against the manifest:
```
Step 5 Progress:
  [x] src/etl/transformations.py  — header added (3 changes, 1 TODO)
  [x] src/etl/loader.py           — header added (1 change)
  [ ] src/utils/helpers.py         — pending
  [ ] src/config.py                — pending
  [ ] src/__init__.py              — pending
```

**Checklist before proceeding to Step 6 — every item MUST be true:**
- [ ] Every `.py` file in the manifest from Step 2.1 has been processed
- [ ] Each file has had unsupported imports removed (databricks, delta, etc.)
- [ ] Each file that creates a SparkSession has been updated to use snowpark_connect
- [ ] Each file has a migration header docstring added at the top
- [ ] File count matches: `manifest count == processed count`

### Step 6: Verify Migration

**For EACH file in the manifest from Step 2.1**, perform the following checks:

1.  **Syntax Check**: Run a syntax check on ALL files in the manifest to ensure no parse errors were introduced.
    ```bash
    # For a single file:
    python3 -m py_compile ${ARGUMENTS%.py}_scos.py
    
    # For a directory (check ALL .py files):
    find ${ARGUMENTS}_scos -name "*.py" -exec python3 -m py_compile {} \;
    ```

2.  **Per-File Review**: For **EACH** file in the manifest, verify:
    -   All imports are correct (no mixed `pyspark.sql` and `pyspark.sql.connect` for the same classes).
    -   The `snowpark_connect` initialization is present (in files that create sessions).
    -   The migration header docstring is present at the top of the file.
    -   No critical `TODO` items remain that block execution.

3.  **Completeness Gate**: Compare the manifest against the final state. This check is **mandatory** and MUST pass before proceeding.
    ```bash
    # Count files in original vs migrated
    echo "Original: $(find $ARGUMENTS -name '*.py' | wc -l) files"
    echo "Migrated: $(find ${ARGUMENTS}_scos -name '*.py' | wc -l) files"
    
    # Verify every migrated file has a migration header
    for f in $(find ${ARGUMENTS}_scos -name "*.py" -type f | sort); do
      if head -5 "$f" | grep -q "SCOS Migration Output"; then
        echo "✓ $f"
      else
        echo "✗ $f — MISSING MIGRATION HEADER"
      fi
    done
    ```
    
    If **any file** is missing its migration header, go back and add it before proceeding. The migration is not complete until every `.py` file passes this check.

### Step 7: Offer Validation

After migration and verification are complete, ask the user:

```
Migration complete. Would you like to validate the migrated workload
by running it end-to-end with synthetic data?

This will smoke-test the _scos code against a live SCOS session to
verify it runs without errors.
```

If the user accepts, load `validate-pyspark-to-snowpark-connect/SKILL.md` with the migrated output path as `$ARGUMENTS` and follow the validation workflow.

## Success Criteria

- **Every `.py` file** in the manifest from Step 2.1 has been processed through Steps 3–6
- All syntax checks pass (`py_compile` exits 0 for every file)
- All high-risk issues (`final_risk` >= 0.7) have fixes applied
- **Every** file has a migration header docstring (verified by Step 6 completeness gate)
- File count matches between original and migrated directories
- Step 6 completeness gate passes with no `✗` entries

## Troubleshooting

See `references/troubleshooting.md` for common issues and solutions.

## Output

Present the migrated code clearly. If multiple files were migrated, list them.
Do not remove the `analysis.json` file.
