-- ============================================
-- CREATING OBJECTS FOR DECLARATIVE SHARING
-- ============================================
-- Reference for creating views, semantic views, agents, notebooks, UDFs, and procedures.
-- Create all objects FIRST, then write the manifest, then create & release the package.

-- ============================================
-- VIEWS — MUST BE SECURE
-- ============================================
-- WRONG: CREATE VIEW ...
-- CORRECT:
CREATE OR REPLACE SECURE VIEW <DB>.<SCHEMA>.<VIEW_NAME> AS
SELECT * FROM <SOURCE_TABLE>;

-- Do NOT use REFERENCE_USAGE grants — the manifest handles access automatically.

-- ============================================
-- SEMANTIC VIEWS
-- ============================================
-- Do NOT hallucinate semantic view DDL syntax. It is unique and changes across releases.
-- ALWAYS run this first to get current syntax:
--   cortex search docs "CREATE SEMANTIC VIEW SQL syntax"
--
-- The DDL has TABLES, FACTS, DIMENSIONS, METRICS sections — get the exact format from docs.
--
-- GOTCHA — verified_queries must NOT use fully qualified names:
--
-- BAD - causes INTERNAL_ERROR 370001:
--   verified_queries:
--     - sql: SELECT * FROM MY_DB.MY_SCHEMA.COMPANIES
--
-- GOOD - table alias only:
--   verified_queries:
--     - sql: SELECT * FROM COMPANIES
--
-- Note: Semantic views with verified_queries are not yet supported in declarative sharing.
-- Avoid using AI Optimization when creating semantic views for sharing.

-- ============================================
-- CORTEX SEARCH SERVICE
-- ============================================
-- Do NOT guess the syntax. Run:
--   cortex search docs "CREATE CORTEX SEARCH SERVICE"
--
-- Key parameters: ON <search_column>, ATTRIBUTES, WAREHOUSE, TARGET_LAG, AS (SELECT ...)
-- Note: Cortex Search has limited support in declarative shares.

-- ============================================
-- UDFs
-- ============================================
-- For complex UDF patterns, run:
--   cortex search docs "CREATE FUNCTION Snowflake SQL UDF"
--
-- MANIFEST GOTCHA: Functions MUST include their signature in manifest.yml:
--   WRONG: - my_function:
--   CORRECT: - my_function(VARCHAR):
--   CORRECT: - my_function(NUMBER, VARCHAR):
--
-- SCHEMA SEPARATION: Functions/procedures MUST be in a SEPARATE schema from
--   data objects (tables, views, semantic_views). Use e.g. LOGIC_SCHEMA for functions,
--   DATA_SCHEMA for tables/views.
--
-- Basic template:
CREATE OR REPLACE FUNCTION <DB>.<SCHEMA>.<FUNC_NAME>(<PARAM> <TYPE>)
RETURNS VARCHAR
LANGUAGE SQL
AS $$
  CASE
    WHEN PARAM = 'value1' THEN 'Result 1'
    WHEN PARAM = 'value2' THEN 'Result 2'
    ELSE 'Default result'
  END
$$;

-- ============================================
-- STORED PROCEDURES
-- ============================================
-- For complex procedure patterns, run:
--   cortex search docs "CREATE PROCEDURE Snowflake SQL"
--
-- Same MANIFEST and SCHEMA SEPARATION rules as UDFs above:
--   - Include signature in manifest: - my_procedure():  or  - my_procedure(VARCHAR, NUMBER):
--   - Must be in a logic-only schema (no tables/views/semantic_views)
--
-- Basic template:
CREATE OR REPLACE PROCEDURE <DB>.<SCHEMA>.<PROC_NAME>(
    <PARAM1> <TYPE1>,
    <PARAM2> <TYPE2>
)
RETURNS VARCHAR
LANGUAGE SQL
AS $$
BEGIN
    RETURN 'Result: ' || PARAM1 || ', ' || PARAM2;
END
$$;

-- ============================================
-- CORTEX AGENTS
-- ============================================
-- Syntax is CREATE AGENT, NOT CREATE CORTEX AGENT.

-- CRITICAL CONSTRAINTS
-- 1. ALL TOOLS MUST BE IN THE SAME DATABASE as the agent (different schemas OK)
-- 2. warehouse MUST be "" (empty string) for Analyst/UDF/Procedure tools — they WILL FAIL without this
-- 3. Cortex Search does NOT need execution_environment (uses max_results instead)
-- 4. NEVER reference objects in a DIFFERENT database — keep all dependencies in the same DB
--
-- IDENTIFIER FORMAT IN tool_resources:
--   - UDFs/Procedures: ALWAYS use RELATIVE names: SCHEMA.OBJECT (NEVER FQN with database!)
--   - Semantic views/Search services: Use FQN with provider source DB: SOURCE_DB.SCHEMA.OBJECT
--     (Snowflake auto-rewrites the DB portion to the app name when installed)
--
-- CORRECT:
--   identifier: "AGENT_SCHEMA.MY_FUNCTION"                  CORRECT: Relative for UDFs/procedures
--   identifier: "AGENT_SCHEMA.MY_PROCEDURE"                 CORRECT: Relative for UDFs/procedures
--   semantic_view: "MY_SOURCE_DB.DATA_SCHEMA.MY_SV"         CORRECT: FQN for semantic views
--   search_service: "MY_SOURCE_DB.DATA_SCHEMA.MY_SEARCH"    CORRECT: FQN for search services
--
-- WRONG:
--   identifier: "MY_SOURCE_DB.AGENT_SCHEMA.MY_FUNCTION"     WRONG: FQN for UDFs — breaks!

-- TOOL TYPE REFERENCE
-- | Tool Type           | tool_spec.type              | tool_resources Structure                    |
-- |---------------------|-----------------------------|--------------------------------------------|
-- | Cortex Analyst (SV) | cortex_analyst_text_to_sql  | semantic_view + execution_environment      |
-- | Cortex Search       | cortex_search               | search_service + max_results (NO exec env) |
-- | UDF (Function)      | generic                     | identifier + type:"function" + exec env    |
-- | Stored Procedure    | generic                     | identifier + type:"procedure" + exec env   |

-- COMPREHENSIVE AGENT EXAMPLE (ALL TOOL TYPES)
-- Agent and all tools in the same database (different schemas OK).

CREATE OR REPLACE AGENT <DB>.AGENT_SCHEMA.<AGENT_NAME>
  COMMENT = 'Agent with all tool types: Analyst, Search, UDF, Procedure'
  PROFILE = '{"display_name": "Agent Name", "color": "#4a90d9"}'
  FROM SPECIFICATION
  $$
orchestration:
  budget:
    seconds: 60
    tokens: 16000
instructions:
  system: |
    You are a helpful assistant with access to multiple tools.
    Use the appropriate tool based on the user's question.
  sample_questions:
    - question: "What is the total inventory value by category?"
    - question: "Search for return policy information"
    - question: "What are the store hours?"
    - question: "Calculate 20% discount on $150"
tools:
  - tool_spec:
      type: "cortex_analyst_text_to_sql"
      name: "product_analytics"
      description: "Query product catalog data for inventory, pricing, and category analysis"
  - tool_spec:
      type: "cortex_search"
      name: "doc_search"
      description: "Search documentation for policies, support info, and help articles"
  - tool_spec:
      type: "generic"
      name: "store_info"
      description: "Get store information about hours, location, or return policy"
      input_schema:
        type: "object"
        properties:
          topic:
            type: "string"
            description: "Topic to lookup: hours, location, or returns"
        required:
          - topic
  - tool_spec:
      type: "generic"
      name: "discount_calculator"
      description: "Calculate discount amount and final price"
      input_schema:
        type: "object"
        properties:
          original_price:
            type: "number"
            description: "Original price in dollars"
          discount_percent:
            type: "number"
            description: "Discount percentage (e.g., 20 for 20%)"
        required:
          - original_price
          - discount_percent
tool_resources:
  product_analytics:
    semantic_view: "<SOURCE_DB>.DATA_SCHEMA.<SEMANTIC_VIEW>"
    execution_environment:
      type: "warehouse"
      warehouse: ""
  doc_search:
    search_service: "<SOURCE_DB>.DATA_SCHEMA.<SEARCH_SERVICE>"
    max_results: 5
  store_info:
    identifier: "AGENT_SCHEMA.<UDF_NAME>"
    type: "function"
    execution_environment:
      type: "warehouse"
      warehouse: ""
  discount_calculator:
    identifier: "AGENT_SCHEMA.<PROCEDURE_NAME>"
    type: "procedure"
    execution_environment:
      type: "warehouse"
      warehouse: ""
  $$;

-- INDIVIDUAL TOOL TYPE EXAMPLES

-- ----- CORTEX ANALYST (Semantic View) -----
-- tool_spec.type: "cortex_analyst_text_to_sql"
-- tool_resources: semantic_view + execution_environment
-- warehouse MUST be "" (empty string)
-- Use FQN with provider source DB: SOURCE_DB.SCHEMA.OBJECT (auto-rewritten on install)
--
-- tools:
--   - tool_spec:
--       type: "cortex_analyst_text_to_sql"
--       name: "analyzer"
--       description: "Query and analyze data"
-- tool_resources:
--   analyzer:
--     semantic_view: "<SOURCE_DB>.<SCHEMA>.<SEMANTIC_VIEW>"
--     execution_environment:
--       type: "warehouse"
--       warehouse: ""

-- ----- CORTEX SEARCH -----
-- tool_spec.type: "cortex_search"
-- tool_resources: search_service + max_results
-- NO execution_environment needed for search tools!
-- Use FQN with provider source DB: SOURCE_DB.SCHEMA.OBJECT (auto-rewritten on install)
--
-- tools:
--   - tool_spec:
--       type: "cortex_search"
--       name: "searcher"
--       description: "Search documents"
-- tool_resources:
--   searcher:
--     search_service: "<SOURCE_DB>.<SCHEMA>.<SEARCH_SERVICE>"
--     max_results: 5

-- ----- UDF (Function) -----
-- tool_spec.type: "generic" + input_schema
-- tool_resources: identifier + type:"function" + execution_environment
-- warehouse MUST be "" (empty string)
-- ALWAYS use RELATIVE identifier (SCHEMA.OBJECT) — NEVER FQN with database!
--
-- tools:
--   - tool_spec:
--       type: "generic"
--       name: "my_function"
--       description: "Does something useful"
--       input_schema:
--         type: "object"
--         properties:
--           param1:
--             type: "string"
--             description: "First parameter"
--         required:
--           - param1
-- tool_resources:
--   my_function:
--     identifier: "<SCHEMA>.<FUNCTION_NAME>"
--     type: "function"
--     execution_environment:
--       type: "warehouse"
--       warehouse: ""

-- ----- STORED PROCEDURE -----
-- tool_spec.type: "generic" + input_schema
-- tool_resources: identifier + type:"procedure" + execution_environment
-- warehouse MUST be "" (empty string)
-- ALWAYS use RELATIVE identifier (SCHEMA.OBJECT) — NEVER FQN with database!
--
-- tools:
--   - tool_spec:
--       type: "generic"
--       name: "my_procedure"
--       description: "Processes something"
--       input_schema:
--         type: "object"
--         properties:
--           param1:
--             type: "string"
--             description: "First parameter"
--           param2:
--             type: "number"
--             description: "Second parameter"
--         required:
--           - param1
--           - param2
-- tool_resources:
--   my_procedure:
--     identifier: "<SCHEMA>.<PROCEDURE_NAME>"
--     type: "procedure"
--     execution_environment:
--       type: "warehouse"
--       warehouse: ""

-- AGENT MANAGEMENT
SHOW AGENTS IN SCHEMA <DB>.<SCHEMA>;
SHOW AGENTS IN DATABASE <DB>;
SHOW AGENTS IN ACCOUNT;
SHOW AGENTS IN APPLICATION PACKAGE <PACKAGE_NAME>;

DESCRIBE AGENT <DB>.<SCHEMA>.<AGENT_NAME>;

DROP AGENT IF EXISTS <DB>.<SCHEMA>.<AGENT_NAME>;

-- ============================================
-- NOTEBOOKS
-- ============================================
-- Notebooks are an interface for consumers to explore shared data interactively.
-- They complement agents — agents provide natural language, notebooks provide direct SQL.
--
-- Snowsight notebooks run inside Snowflake — NO connection setup needed.
-- The session is already active. Use SCHEMA.TABLE (no database prefix — the app name IS the database).
--
-- CONSTRAINT: Notebooks can ONLY access data within the same application package.
-- They cannot query external databases or the provider's source data directly.
-- Surface this constraint to users so they understand the data access scope.

-- PARAMOUNT: NOTEBOOK CELL FORMAT
-- THIS IS THE #1 MOST COMMON FAILURE POINT FOR NOTEBOOKS.
-- If you get this wrong, SQL cells will be interpreted as Python and WILL NOT EXECUTE.
-- The consumer will see syntax errors and the notebook will be unusable.
--
-- EVERY code cell MUST have "metadata": { "language": "..." } set correctly.
-- There is NO exception. Missing this metadata = broken notebook.
--
-- CORRECT SQL cell:
--   {
--     "cell_type": "code",
--     "metadata": { "language": "sql" },
--     "source": ["SELECT * FROM MY_TABLE"],
--     "outputs": []
--   }
--
-- CORRECT Python cell:
--   {
--     "cell_type": "code",
--     "metadata": { "language": "python" },
--     "source": ["import pandas as pd"],
--     "outputs": []
--   }
--
-- WRONG — missing language metadata (defaults to Python, SQL will break):
--   {
--     "cell_type": "code",
--     "metadata": {},
--     "source": ["SELECT * FROM MY_TABLE"],
--     "outputs": []
--   }
--
-- WRONG — no metadata key at all:
--   {
--     "cell_type": "code",
--     "source": ["SELECT * FROM MY_TABLE"],
--     "outputs": []
--   }
--
-- MANDATORY POST-GENERATION CHECK:
-- After creating ANY .ipynb file, VERIFY that:
-- 1. EVERY code cell has "metadata": { "language": "sql" } or "metadata": { "language": "python" }
-- 2. SQL cells (SELECT, SHOW, DESCRIBE, etc.) have "language": "sql"
-- 3. Python cells (import, session, df, etc.) have "language": "python"
-- 4. Markdown cells use "cell_type": "markdown" (no language metadata needed)
-- 5. Do NOT omit the "outputs": [] key from code cells

-- CREATE DATA-SPECIFIC NOTEBOOKS
-- The template (NOTEBOOK.ipynb) has PLACEHOLDERS like SCHEMA_NAME.TABLE_NAME.
-- When creating a notebook for an app package:
-- 1. Use ACTUAL table/view names from the provider's data
-- 2. Use ACTUAL column names in queries
-- 3. Write MEANINGFUL queries for the specific dataset
-- 4. Do NOT just copy the template with placeholders!
--
-- Notebook content should include:
-- 1. SHOW TABLES / SHOW VIEWS to discover available data
-- 2. Basic SELECT * FROM SCHEMA.TABLE LIMIT N previews
-- 3. DESCRIBE TABLE for schema inspection
-- 4. COUNT(*) and summary statistics
-- 5. GROUP BY aggregations for categorical analysis
-- 6. Domain-specific queries relevant to the data
-- 7. Python cell with pandas (session via get_active_session())
-- 8. Empty cells for consumer's custom queries
--
-- See: NOTEBOOK.ipynb for the template structure

