---
name: evaluate-cortex-agent
description: Run formal evaluations on Cortex Agents using Snowflake's native Agent Evaluations. Use this to benchmark agent performance, measure accuracy metrics (answer_correctness, logical_consistency), and compare before/after improvements.
---

# Evaluate Cortex Agent

Evaluate Cortex Agents using Snowflake's native Agent Evaluations feature.

**Available Metrics:**
| Metric | API Name | Requires Ground Truth | Description |
|--------|----------|----------------------|-------------|
| Answer Correctness | `answer_correctness` | Yes | Semantic match of final answer |
| Logical Consistency | `logical_consistency` | No | Consistency across instructions, planning, and tool calls within a single execution (reference-free) |

## Prerequisites

- Active Snowflake connection
- Agent must already exist
- A role with appropriate permissions (see Troubleshooting if you hit permission errors)

Whenever running scripts, make sure to use `uv`.

**IMPORTANT: Do NOT use `cortex ctx task` or `cortex ctx step` commands during this workflow. The skill's own step-by-step structure with mandatory stopping points provides sufficient tracking.**

**Required Access Control Grants (reference):**
```sql
GRANT DATABASE ROLE SNOWFLAKE.CORTEX_USER TO ROLE <role>;
GRANT EXECUTE TASK ON ACCOUNT TO ROLE <role>;
GRANT CREATE FILE FORMAT ON SCHEMA <agent_schema> TO ROLE <role>;
GRANT CREATE DATASET ON SCHEMA <agent_schema> TO ROLE <role>;
GRANT CREATE TASK ON SCHEMA <agent_schema> TO ROLE <role>;
GRANT MONITOR ON AGENT <database>.<schema>.<agent> TO ROLE <role>;
GRANT USAGE ON WAREHOUSE <warehouse> TO ROLE <role>;
```

**Note:** Do not verify permissions upfront. Proceed with the workflow and debug if a step fails. See the **Troubleshooting** section for common permission errors and fixes.

## Tools

### Script: evaluate_cortex_agent.py

**Description**: Creates stages, uploads YAML evaluation configs via PUT, and executes/checks evaluation runs through `EXECUTE_AI_EVALUATION`. Required because PUT is a client-side command that cannot run in Snowsight worksheets.

**Usage:**
```bash
uv run python ../scripts/evaluate_cortex_agent.py \
    <subcommand> [args]
```

**Subcommands:**
| Subcommand | Description | Key Args |
|------------|-------------|----------|
| `upload` | Create stage + upload YAML config | `--yaml-file`, `--stage` |
| `start` | Start an evaluation run | `--run-name`, `--stage`, `--config-filename` |
| `status` | Check evaluation run status | `--run-name`, `--stage`, `--config-filename`, `--wait` |

**Common args** (all subcommands): `--connection`, `--database`, `--schema`

**Status-specific args:** `--wait` (poll until done), `--poll-interval` (default 30s), `--timeout` (default 600s)

**Example:**
```bash
uv run python ../scripts/evaluate_cortex_agent.py \
    upload \
    --yaml-file /tmp/my_eval_config.yaml \
    --stage MYDB.MYSCHEMA.EVAL_CONFIG_STAGE \
    --database MYDB --schema MYSCHEMA --connection my_conn
```

## Workflow

**IMPORTANT: Go through each step ONE AT A TIME. Wait for user confirmation before proceeding.**

Present this plan first:
```
I'll help you evaluate your Cortex Agent. Here's the workflow:

1. **Identify Agent** - Confirm which agent to evaluate
2. **Choose Metrics** - Select evaluation metrics (answer_correctness, logical_consistency)
3. **Dataset Setup** - Use existing dataset
4. **Run Evaluation** - Build YAML config, upload to stage, execute evaluation
5. **View Results** - Review scores in Snowsight and query results programmatically (optional)

```


---

### Step 1: Identify Agent and Gather Info

**Ask user without using the AskUserQuestion tool**
```
Which agent would you like to evaluate?
- Database: [e.g., MY_DATABASE]
- Schema: [e.g., AGENTS]
- Agent Name: [e.g., MY_SALES_AGENT]
- Connection: [default: snowhouse]
```

If the user only provides the agent name, help them find it:
```sql
SHOW AGENTS LIKE '%<AGENT_NAME>%' IN ACCOUNT;
```

**Construct Fully Qualified Agent Name:** `<DATABASE>.<SCHEMA>.<AGENT_NAME>`

**Extract agent configuration:**
```sql
DESC AGENT <DATABASE>.<SCHEMA>.<AGENT_NAME>;
```

The `agent_spec` column (index 6) contains a JSON object with the full agent configuration. **If `agent_spec` is empty or null**, the agent has no tools or model configured

**STOP**: Confirm agent details before proceeding to Step 2.
Present the agent name and tools found to the user.

---

### Step 2: Choose Evaluation Metrics

**Ask user:**
```
Which metrics do you want to evaluate?

1. [ ] answer_correctness - Does the agent give correct answers?
       Requires: expected answer for each question

2. [ ] logical_consistency - Is the response internally consistent? (reference-free)
       Requires: nothing (no ground truth needed)

Select metrics (e.g., "1,2" or "all" or "just 2"):
```

**Based on selection, determine dataset requirements:**

| If user selects... | Dataset needs... |
|-------------------|------------------|
| Only `logical_consistency` | Just a query column (no ground truth needed) |
| `answer_correctness` | `ground_truth_output` in ground truth column |


---

### Step 3: Dataset Setup

**Ask user:**
```
Do you have a registered dataset or an existing table?
- If registered dataset: provide the dataset name (e.g., DB.SCHEMA.MY_EVAL_DATASET)
- If existing table: provide the fully qualified table name (e.g., DB.SCHEMA.MY_TABLE)
```

**If user provides a registered dataset name:**

Verify it exists:
```sql
SHOW DATASETS IN SCHEMA <DATABASE>.<SCHEMA>;
```

**Note:** Use `SHOW DATASETS` to list available datasets, not `DESC DATASET`.

Record the dataset name — it will be referenced in the YAML config's `evaluation.source_metadata.dataset_name`. Skip to Step 4.

**If user provides a table name:**

Ask user for column names:
```
What are the column names in your table?
- Column containing the input queries (VARCHAR): [e.g., user_question]
- Column containing the ground truth (OBJECT): [e.g., expected_output]
```

Column names can be anything — the YAML config's `column_mapping` handles the mapping. The requirements are:
- The query column must be `VARCHAR`
- The ground truth column must be `OBJECT` type, containing JSON with the keys below

Record the table name and column names — they will be used in the YAML config's `dataset.column_mapping`:
```yaml
column_mapping:
  query_text: "<USER_QUERY_COLUMN>"
  ground_truth: "<USER_GROUND_TRUTH_COLUMN>"
```

**Ground truth JSON keys:**
| Key | Description | Used by |
|-----|-------------|---------|
| `ground_truth_output` | Expected final answer (semantic match) | `answer_correctness` |

**If user's table doesn't have an OBJECT ground truth column**, help them create one:
```sql
CREATE OR REPLACE TABLE <DATABASE>.<SCHEMA>.<AGENT_NAME>_EVAL_DATASET (
    input_query VARCHAR,
    ground_truth OBJECT
);

INSERT INTO <DATABASE>.<SCHEMA>.<AGENT_NAME>_EVAL_DATASET (input_query, ground_truth)
VALUES (
    '<QUESTION_1>',
    OBJECT_CONSTRUCT('ground_truth_output', '<EXPECTED_ANSWER>')
);
```



**⚠️ MANDATORY STOPPING POINT**: Confirm dataset details before proceeding to Step 4.

---

### Step 4: Build YAML Config, Upload to Stage, and Run Evaluation

#### Step 4.1: Generate YAML Config

Based on the user's choices in Steps 2 and 3, generate a YAML config file.

**If user has an existing registered dataset (no dataset creation needed):**

```yaml
evaluation:
  agent_params:
    agent_name: "<DATABASE>.<SCHEMA>.<AGENT_NAME>"
    agent_type: "CORTEX AGENT"
  run_params:
    label: "evaluation"
    description: "<DESCRIPTION>"
  source_metadata:
    type: "dataset"
    dataset_name: "<EXISTING_DATASET_NAME>"

metrics:
  - "answer_correctness"
  - "logical_consistency"
```

**If user has a table that needs to be registered as a dataset:**

```yaml
dataset:
  dataset_type: "cortex agent"
  table_name: "<DATABASE>.<SCHEMA>.<TABLE_NAME>"
  dataset_name: "<AGENT_NAME>_eval_ds_<YYYYMMDD>"
  column_mapping:
    query_text: "INPUT_QUERY"
    ground_truth: "GROUND_TRUTH"

evaluation:
  agent_params:
    agent_name: "<DATABASE>.<SCHEMA>.<AGENT_NAME>"
    agent_type: "CORTEX AGENT"
  run_params:
    label: "evaluation"
    description: "<DESCRIPTION>"
  source_metadata:
    type: "dataset"
    dataset_name: "<AGENT_NAME>_eval_ds_<YYYYMMDD>"

metrics:
  - "answer_correctness"
  - "logical_consistency"
```

**Only include metrics the user selected in Step 2.** Adjust `column_mapping` keys to match the actual column names in the user's table.

**Save the YAML config to a workspace directory:**

Create the workspace directory `<DATABASE>_<SCHEMA>_<AGENT_NAME>/` (FQN with underscores) if it doesn't exist. This convention matches `init_agent_workspace.py` and keeps files organized when evaluating multiple agents. Use the **Write** tool to save the YAML config to `<DATABASE>_<SCHEMA>_<AGENT_NAME>/<AGENT_NAME>_eval_config.yaml`.

#### Step 4.2: Upload YAML to Stage

```bash
uv run python ../scripts/evaluate_cortex_agent.py \
    upload \
    --yaml-file <DATABASE>_<SCHEMA>_<AGENT_NAME>/<AGENT_NAME>_eval_config.yaml \
    --stage <DATABASE>.<SCHEMA>.EVAL_CONFIG_STAGE \
    --database <DATABASE> --schema <SCHEMA> --connection <CONNECTION>
```

The script creates the file format, stage, uploads via PUT, and verifies the upload automatically.

#### Step 4.3: Start Evaluation

```bash
uv run python ../scripts/evaluate_cortex_agent.py \
    start \
    --run-name <AGENT_NAME>_eval_<YYYYMMDD_HHMMSS> \
    --stage <DATABASE>.<SCHEMA>.EVAL_CONFIG_STAGE \
    --config-filename <AGENT_NAME>_eval_config.yaml \
    --database <DATABASE> --schema <SCHEMA> --connection <CONNECTION>
```

#### Step 4.4: Check Evaluation Status

Use `--wait` to auto-poll until the evaluation completes:
```bash
uv run python ../scripts/evaluate_cortex_agent.py \
    status --wait \
    --run-name <AGENT_NAME>_eval_<YYYYMMDD_HHMMSS> \
    --stage <DATABASE>.<SCHEMA>.EVAL_CONFIG_STAGE \
    --config-filename <AGENT_NAME>_eval_config.yaml \
    --database <DATABASE> --schema <SCHEMA> --connection <CONNECTION>
```

The script polls every 30 seconds (configurable via `--poll-interval`) up to 10 minutes (`--timeout`).

**Status values:**
| Status | Meaning |
|--------|---------|
| `INVOCATION_IN_PROGRESS` | Agent is being invoked on evaluation inputs |
| `COMPUTATION_IN_PROGRESS` | Metrics are being computed |
| `COMPLETED` | Evaluation finished successfully |
| `FAILED` | Evaluation failed — check `STATUS_DETAILS` |

If `FAILED`, check `STATUS_DETAILS` and consult **Troubleshooting** below.

---

### Step 5: View Results

**Generate Snowsight link:**
```sql
SELECT LOWER(CURRENT_ORGANIZATION_NAME()), LOWER(CURRENT_ACCOUNT_NAME());
```

URL format:
```
https://app.snowflake.com/<org>/<account>/#/agents/database/<DATABASE>/schema/<SCHEMA>/agent/<AGENT_NAME>/evaluations/<RUN_NAME>/records
```

**Note**: Use underscore in account name for Snowsight URLs (e.g., `sfdevrel_enterprise` not `sfdevrel-enterprise`).

Present the link to the user.

**Present results as:**
1. Summary table with overall average score per metric

**Query results programmatically (optional):**
```sql
-- Get evaluation results
SELECT *
FROM TABLE(SNOWFLAKE.LOCAL.GET_AI_EVALUATION_DATA(
    '<DATABASE>', '<SCHEMA>', '<AGENT_NAME>', 'CORTEX AGENT', '<RUN_NAME>'
))
ORDER BY TIMESTAMP DESC;

-- Get evaluation criteria for low scores
SELECT
    RECORD_ID, METRIC_NAME, EVAL_AGG_SCORE,
    e.VALUE:criteria::VARCHAR AS CRITERIA,
    e.VALUE:explanation::VARCHAR AS EXPLANATION
FROM TABLE(SNOWFLAKE.LOCAL.GET_AI_EVALUATION_DATA(
    '<DATABASE>', '<SCHEMA>', '<AGENT_NAME>', 'CORTEX AGENT', '<RUN_NAME>'
)),
LATERAL FLATTEN(input => EVAL_CALLS) e
WHERE EVAL_AGG_SCORE < 0.5
ORDER BY EVAL_AGG_SCORE ASC;

-- Drill into a specific record's execution trace
SELECT *
FROM TABLE(SNOWFLAKE.LOCAL.GET_AI_RECORD_TRACE(
    '<DATABASE>', '<SCHEMA>', '<AGENT_NAME>', 'CORTEX AGENT', '<RECORD_ID>'
))
ORDER BY START_TIMESTAMP;

-- Check for errors and warnings
SELECT *
FROM TABLE(SNOWFLAKE.LOCAL.GET_AI_OBSERVABILITY_LOGS(
    '<DATABASE>', '<SCHEMA>', '<AGENT_NAME>', 'CORTEX AGENT'
))
WHERE record:"severity_text" IN ('ERROR', 'WARN')
AND record_attributes:"snow.ai.observability.run.name" = '<RUN_NAME>';
```

**⚠️ MANDATORY STOPPING POINT**: Review results with user. Discuss findings and next steps.

---

## Troubleshooting

### Permission Errors

If any step fails with a permission error, diagnose with:
```sql
SELECT CURRENT_ROLE(), CURRENT_USER();
SHOW GRANTS TO ROLE <your_role>;
SHOW GRANTS ON SCHEMA <database>.<schema>;
SHOW GRANTS ON AGENT <database>.<schema>.<agent_name>;
```

| Error | Fix |
|-------|-----|
| `Insufficient privileges to operate on dataset` | `GRANT CREATE DATASET ON SCHEMA <schema> TO ROLE <role>;` |
| `Cannot create task` | `GRANT CREATE TASK ON SCHEMA <schema> TO ROLE <role>;` |
| `Insufficient privileges on agent` | `GRANT MONITOR ON AGENT <db>.<schema>.<agent> TO ROLE <role>;` |
| `Cannot execute task` | `GRANT EXECUTE TASK ON ACCOUNT TO ROLE <role>;` |
| `Insufficient privileges to operate on stage` | `GRANT READ ON STAGE <db>.<schema>.<stage> TO ROLE <role>;` |
| `Insufficient privileges to operate on file format` | `GRANT CREATE FILE FORMAT ON SCHEMA <schema> TO ROLE <role>;` |

### YAML Config Not Parsed

1. Ensure the file format uses `FIELD_DELIMITER = NONE` (not comma)
2. Verify upload: `SELECT $1 FROM @<stage>/<file>.yaml;`
3. Check YAML indentation — spaces, not tabs
4. Ensure `dataset_type` is `"cortex agent"` (lowercase, with space)

### Script Execution Fails

1. Ensure the local YAML file exists at the path passed to `--yaml-file`
2. The script uses PUT via the Snowflake Python connector — cannot run in Snowsight
3. Check that your role has `CREATE STAGE` and `CREATE FILE FORMAT` permissions
4. Verify `uv` is installed: `uv --version`

### Evaluation STATUS Shows FAILED

1. Check `STATUS_DETAILS` column for specific errors
2. Query logs: `SELECT * FROM TABLE(SNOWFLAKE.LOCAL.GET_AI_OBSERVABILITY_LOGS(...)) WHERE record:"severity_text" IN ('ERROR', 'WARN');`
3. Common causes: invalid metric names, missing ground truth, agent timeout

### Ground Truth Not Parsed

1. Column type must be `OBJECT` or `VARIANT` — use `OBJECT_CONSTRUCT()` when inserting
2. JSON must use `ground_truth_output` for expected answers
3. Ensure YAML `column_mapping.ground_truth` points to the correct column name

### Agent Refuses to Use Tools (0% scores)

Questions don't match the agent's persona. Check `DESCRIBE AGENT` for guardrails and create questions matching what the agent is designed to do.

### "No current database" Error

Run `USE DATABASE <DATABASE>; USE SCHEMA <SCHEMA>;` then re-run the failing command.

### Dataset Inspection

`SHOW DATASETS IN SCHEMA <DATABASE>.<SCHEMA>;` works to list datasets. **Note:** `DESC DATASET` is not currently supported — do not use it for dataset inspection.

---

This skill integrates with `optimize-cortex-agent` for baseline benchmarking (Phase 3) and validation after changes (Phase 6).
