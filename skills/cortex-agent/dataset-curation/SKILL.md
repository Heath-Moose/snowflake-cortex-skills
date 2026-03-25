---
name: dataset-curation
description: Create and manage evaluation datasets for Cortex Agents. Use this to build datasets from scratch, from production data, or to add questions to existing datasets. Outputs datasets in the format required by Snowflake Agent Evaluations.
---

# Dataset Curation for Cortex Agent Evaluation

## Purpose

Create and manage evaluation datasets for Cortex Agents. This workflow helps you build high-quality datasets that can be used with Snowflake's native Agent Evaluations (`evaluate-cortex-agent` skill).

## When to Use

- **New agent**: Create evaluation dataset from scratch
- **Production agent**: Build dataset from real production queries
- **Expanding coverage**: Add questions to existing dataset
- **Format conversion**: Convert existing Q&A data to evaluation format

## Prerequisites

**Snowflake Access:**
- Connection with write access to create tables
- For production data: access to agent event tables

**Understanding:**
- Agent's tools and capabilities
- Expected behaviors for different question types

## Dataset Format

Snowflake Agent Evaluations require a specific format:

**Source table columns:**
| Column | Type | Description |
|--------|------|-------------|
| `INPUT_QUERY` | VARCHAR | The question to ask the agent |
| `GROUND_TRUTH` | OBJECT | Expected results (structure below) |

**GROUND_TRUTH structure:**
```json
{
  "ground_truth_output": "Expected answer text"
}
```

**What each field enables:**
| Field | Enables Metric |
|-------|----------------|
| `ground_truth_output` | `answer_correctness` |
| (none required) | `logical_consistency` |

## Workflows

### Option A: Create Dataset from Scratch

**Goal:** Design and build evaluation dataset for a new or untested agent.

#### Step 1: Understand Agent Capabilities

**Gather agent information:**

```sql
-- Get agent tools
SELECT tool_name, tool_type, tool_spec
FROM <DATABASE>.INFORMATION_SCHEMA.CORTEX_AGENT_TOOLS
WHERE agent_name = '<AGENT_NAME>';
```

Or extract from agent config:
```bash
uv run --project <SKILL_DIR> python <SKILL_DIR>/scripts/get_agent_config.py \
    --agent-name AGENT_NAME --database DATABASE --schema SCHEMA \
    --connection CONNECTION_NAME --output agent_config.json
```

**Document capabilities:**
- What tools are available?
- What questions can each tool answer?
- What are the boundaries between tools?

#### Step 1.5: Review Agent Instructions

Before creating questions, understand what the agent is designed to do:

```sql
DESCRIBE AGENT <DATABASE>.<SCHEMA>.<AGENT_NAME>;
```

Check `instructions` in the agent config for:
- **Guardrails**: Does the agent refuse certain question types?
- **Persona**: Is it customer-facing? Analytics-focused?
- **Sample questions**: What questions is it designed to answer?

**⚠️ Common pitfall**: Creating analytics questions for a customer-service agent that's programmed to deflect data queries.

**Present findings to user:**
```
Agent Instructions Summary:
- Persona: [Customer service / Analytics / etc.]
- Guardrails: [List any restrictions]
- Example questions from instructions: [List sample questions if provided]

I'll design questions that align with this persona and respect these guardrails.
```

#### Step 2: Design Question Categories

**Recommended distribution:**

| Category | % | Purpose | Example |
|----------|---|---------|---------|
| Core use cases | 40% | Primary agent purpose | "What was Q3 revenue?" |
| Tool routing | 25% | Verify correct tool selection | "Show ML platform usage" (not general usage) |
| Edge cases | 15% | Boundary conditions | "Revenue for Feb 30th" (invalid date) |
| Ambiguous queries | 10% | Interpretation tests | "Show me recent activity" (vague) |
| Data validation | 10% | Quality checks | "Total for incomplete period" |

**For each tool, include:**
- 1-2 clear routing questions (obviously maps to this tool)
- 1 negative routing question (similar but should NOT use this tool)
- 1 ambiguous question (could use multiple tools)

#### Step 3: Draft Questions with Expected Answers

**Target: 10-20 queries** depending on agent complexity:
- Simple agent (1-2 tools): 10-12 queries
- Medium agent (3-4 tools): 12-16 queries
- Complex agent (5+ tools): 16-20 queries

**Work with user to create questions:**

Present proposed questions one category at a time:
```
Here are proposed evaluation questions for [CATEGORY]:

| # | Question | Expected Tool | Notes |
|---|----------|---------------|-------|
| 1 | [question] | [tool] | [note] |
| 2 | [question] | [tool] | [note] |

Any to add, modify, or remove for this category?
```

**STOP**: Get user approval on each category before moving to next.

For each question, gather:
1. The exact question text
2. Expected answer (specific, verifiable)
3. Which tool should handle it
4. Any edge case notes

```
Let's start with core use cases for [TOOL_NAME]:

Question 1: "What was the total revenue for Q3 2025?"
Expected answer: ?
Expected tool: ?
```

**Generate ground truth** for each approved question based on:
- The agent's tools and their purposes
- The semantic model / search corpus
- The agent's instructions and persona

Present ground truth for review:
```
| # | Question | Expected Tool(s) | Ground Truth Output |
|---|----------|------------------|---------------------|
| 1 | [question] | [tool] | [concise expected answer] |

Review the ground truth above. Any corrections needed?
```

**STOP**: Get user approval on ground truth before creating table.

**Expected answer guidelines:**

✅ **Good** (specific, verifiable):
- "Total revenue for Q3 2025 was $2.5M"
- "15,432 active users in December"
- "No data available for the specified period"

❌ **Bad** (vague, unverifiable):
- "Revenue information"
- "Some users were active"
- "The agent should return data"

#### Step 4: Create Dataset Table

**Create source table:**

```sql
CREATE OR REPLACE TABLE <DATABASE>.<SCHEMA>.EVAL_DATASET_<AGENT_NAME> (
    question_id INT AUTOINCREMENT,
    INPUT_QUERY VARCHAR NOT NULL,
    GROUND_TRUTH OBJECT NOT NULL,
    category VARCHAR,
    author VARCHAR DEFAULT CURRENT_USER(),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    notes VARCHAR
);
```

**Insert questions:**

```sql
INSERT INTO EVAL_DATASET_<AGENT_NAME> (INPUT_QUERY, GROUND_TRUTH, category, notes)
VALUES (
    'What was the total revenue for Q3 2025?',
    OBJECT_CONSTRUCT('ground_truth_output', 'Total revenue for Q3 2025 was $2.5M'),
    'core_use_case',
    'Basic revenue query'
);

INSERT INTO EVAL_DATASET_<AGENT_NAME> (INPUT_QUERY, GROUND_TRUTH, category, notes)
VALUES (
    'Show ML platform usage for last month',
    OBJECT_CONSTRUCT('ground_truth_output', 'ML Platform had 1,234 executions last month'),
    'tool_routing',
    'Should route to ML tool, not general usage'
);
```

**Critical format requirements:**
- Column name: `GROUND_TRUTH` (required by SYSTEM$CREATE_EVALUATION_DATASET)
- Column type: `OBJECT` or `VARIANT` (required - do not use VARCHAR)
- Use `OBJECT_CONSTRUCT()` for inserting ground truth data

#### Step 5: Register Dataset

**Create evaluation dataset:**

```sql
CALL SYSTEM$CREATE_EVALUATION_DATASET(
    'Cortex Agent',
    '<DATABASE>.<SCHEMA>.EVAL_DATASET_<AGENT_NAME>',
    '<AGENT_NAME>_eval_v1',
    OBJECT_CONSTRUCT('query_text', 'INPUT_QUERY', 'ground_truth', 'GROUND_TRUTH')
);
```

**Deliverables:**
- Source table with 15-20 questions
- Registered evaluation dataset
- Coverage across all agent tools

---

### Option B: Create Dataset from Production Data

**Goal:** Build evaluation dataset from real production queries.

#### Step 1: Access Production Events

**Option 1: Use Agent Events Explorer (recommended)**

```bash
uv run --project <SKILL_DIR> streamlit run <SKILL_DIR>/scripts/agent_events_explorer.py -- \
    --connection CONNECTION_NAME \
    --database DATABASE \
    --schema SCHEMA \
    --agent AGENT_NAME
```

**Option 2: Query observability logs directly**

```sql
-- Find recent agent interactions using AI Observability
SELECT DISTINCT
    RECORD_ATTRIBUTES:"ai.observability.record_root.input"::STRING AS USER_QUESTION,
    RECORD_ATTRIBUTES:"ai.observability.record_root.output"::STRING AS AGENT_RESPONSE,
    RECORD_ATTRIBUTES:"ai.observability.record_id"::STRING AS REQUEST_ID
FROM TABLE(SNOWFLAKE.LOCAL.GET_AI_OBSERVABILITY_EVENTS(
    '<DATABASE>',
    '<SCHEMA>',
    '<AGENT_NAME>',
    'CORTEX AGENT'))
WHERE RECORD_ATTRIBUTES:"ai.observability.span_type" = 'record_root'
AND USER_QUESTION IS NOT NULL
ORDER BY RECORD_ATTRIBUTES:"ai.observability.record_id" DESC
LIMIT 100;
```

**Present findings:**
```
Found [N] unique questions in agent logs. Here are common patterns:
1. [Category]: "example question 1", "example question 2"
2. [Category]: "example question 3"

Would you like to include some of these in your evaluation dataset?
```

#### Step 2: Filter and Select Questions

**Criteria for good evaluation questions:**
- Representative of real usage
- Clear expected answer exists
- Tests specific capability
- Not duplicate of existing questions

**Filter examples:**
```sql
-- Find questions about specific topics
WHERE question ILIKE '%revenue%'

-- Find questions that used specific tools
WHERE RECORD:response LIKE '%tool_name%'

-- Find questions with errors or issues
WHERE RECORD:response:error IS NOT NULL
```

#### Step 3: Annotate with Expected Answers

**For each selected question:**
1. Review the agent's actual response
2. Determine if it was correct
3. Write the correct answer in the expected_answer column. If you are unsure of the correct answer, ask the user.
4. Note which tool should have been used

**Using Agent Events Explorer:**
- Browse events with filters
- View question, answer, and trace
- Add expected answer annotation
- Provide feedback (correct/incorrect)
- Auto-saves to JSON file

**Manual annotation:**
```sql
CREATE OR REPLACE TABLE EVAL_ANNOTATIONS AS
SELECT 
    REQUEST_ID,
    question,
    answer AS actual_answer,
    NULL AS expected_answer,  -- Fill in manually
    NULL AS is_correct        -- Fill in manually
FROM production_events;
```

#### Step 4: Convert to Evaluation Format

**From annotated data:**

```sql
CREATE OR REPLACE TABLE EVAL_DATASET_<AGENT_NAME> AS
SELECT 
    ROW_NUMBER() OVER (ORDER BY timestamp) AS question_id,
    question AS INPUT_QUERY,
    OBJECT_CONSTRUCT(
        'ground_truth_output', expected_answer
    ) AS GROUND_TRUTH,
    CASE WHEN is_correct THEN 'passing' ELSE 'failing' END AS category,
    'production_data' AS source
FROM annotated_production_data
WHERE expected_answer IS NOT NULL;
```

#### Step 5: Register Dataset

```sql
CALL SYSTEM$CREATE_EVALUATION_DATASET(
    'Cortex Agent',
    '<DATABASE>.<SCHEMA>.EVAL_DATASET_<AGENT_NAME>', -- agent FQN
    '<AGENT_NAME>_eval_v1', -- version
    OBJECT_CONSTRUCT('query_text', 'INPUT_QUERY', 'ground_truth', 'GROUND_TRUTH') -- column mapping
);
```

**Deliverables:**
- Dataset built from real production queries
- Mix of passing and failing cases
- Registered for evaluation

---

### Option C: Add Questions to Existing Dataset

**Goal:** Expand coverage of existing evaluation dataset.

#### Step 1: Review Current Coverage

```sql
-- Count by category
SELECT category, COUNT(*) as count
FROM EVAL_DATASET_<AGENT_NAME>
GROUP BY category;

-- List all questions
SELECT question_id, INPUT_QUERY, category
FROM EVAL_DATASET_<AGENT_NAME>
ORDER BY question_id;
```

**Identify gaps:**
```
Current Coverage:
- revenue_tool: 5 questions
- usage_tool: 3 questions
- ml_platform_tool: 0 questions  ← GAP
- Edge cases: 1 question         ← GAP
- Tool routing tests: 2 questions ← Need more

Recommendations:
1. Add 2 questions for ml_platform_tool
2. Add 3 edge case questions
3. Add 2 tool routing tests
```

#### Step 2: Add New Questions

```sql
INSERT INTO EVAL_DATASET_<AGENT_NAME> (INPUT_QUERY, GROUND_TRUTH, category, notes)
VALUES 
-- ML platform questions
(
    'How many ML models were trained last quarter?',
    OBJECT_CONSTRUCT('ground_truth_output', '47 models were trained in Q4 2025'),
    'core_use_case',
    'New - filling ML tool coverage gap'
),
-- Edge case
(
    'What was revenue on February 30th, 2025?',
    OBJECT_CONSTRUCT('ground_truth_output', 'February 30th is not a valid date. Please provide a valid date.'),
    'edge_case',
    'Invalid date edge case'
),
-- Ambiguous
(
    'Show platform usage statistics',
    OBJECT_CONSTRUCT('ground_truth_output', 'I need clarification: Are you asking about ML Platform usage or general Snowflake platform usage?'),
    'ambiguous',
    'Ambiguous - should ask for clarification'
);
```

#### Step 3: Re-register Dataset

**Important:** After adding questions, re-register the dataset by passing in a new version to CREATE_EVALUATION_DATASET.

**Deliverables:**
- Expanded dataset with better coverage
- New dataset version registered

---

## Best Practices

### Question Design

**Do:**
- ✅ Use realistic language (how users actually ask)
- ✅ Include variations ("Q3 revenue", "third quarter revenue", "revenue for Jul-Sep")
- ✅ Test boundaries (first day, last day, invalid inputs)
- ✅ Include negative cases (questions agent should NOT answer)

**Don't:**
- ❌ Use overly formal language users wouldn't use
- ❌ Make all questions easy/obvious
- ❌ Skip edge cases
- ❌ Ignore tool routing scenarios

### Expected Answers

**Do:**
- ✅ Be specific with numbers ("$2.5M" not "around 2 million")
- ✅ Match expected format ("15,432 users" if agent formats with commas)
- ✅ Include context ("for Q3 2025" not just the number)
- ✅ Document what constitutes "close enough"

**Don't:**
- ❌ Use vague descriptions
- ❌ Expect exact string matches for long responses
- ❌ Forget about date/time formatting variations

### Coverage

**Minimum targets:**
- 15-20 questions total
- At least 1-2 questions per tool
- 25% tool routing tests
- Mix of passing and expected-failing cases

### Maintenance

- Version your datasets (`_v1`, `_v2`, etc.)
- Document changes between versions
- Keep source table for easy updates
- Re-register after modifications

---

## Integration with Other Skills

**From `adhoc-testing-for-cortex-agent`:**
- Test questions interactively first
- Add validated questions here for formal evaluation

**To `evaluate-cortex-agent`:**
- Use created dataset to run evaluations
- Measure agent performance with metrics

**In `optimize-cortex-agent`:**
- Create dataset in Phase 2
- Use for baseline and validation evaluations
