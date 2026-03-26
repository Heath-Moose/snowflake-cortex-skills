---
name: data-policy
parent_skill: data-governance
description: "**[REQUIRED]** for creating, modifying, or auditing Snowflake masking policies, row access policies, or projection policies. Also required for protecting sensitive columns (SSN, email, phone, TIMESTAMP), role-based column access control, checking existing policies before adding new ones, and following data policy best practices. This skill provides critical best practices and audit checklists that identify security anti-patterns (like CURRENT_ROLE vs IS_ROLE_IN_SESSION). Triggers: masking policy, row access policy, projection policy, aggregation policy, join policy, audit policies, policy best practices, create policy, data policy, protect sensitive data, protect column, column masking, TIMESTAMP masking, SSN masking, email masking, phone masking, existing policies, check existing policies, role-based access control, physicians access, compliance officers access."
---

# Snowflake Data Policy Skill

## When to Use/Load
Use this skill when a user asks to design or improve data policies, audit existing data policies, troubleshoot data policy issues, or needs help choosing the right policy approach.

**Also use when:**
- Protecting a specific column (SSN, email, phone, TIMESTAMP, or any data type) with masking
- Checking existing policies before adding a new one ("same access rules", "existing masking")
- Controlling which roles (physicians, compliance officers, analysts) can see column values
- Any request containing "protect sensitive data", "mask column", "column masking", "role-based access"

## Abstraction Hierarchy

This skill organizes content in layers, from low-level syntax to high-level methodology:

- **L1 — Core Concepts** (`data-policy/L1_core_concepts.md`)
  - Policy syntax and structure (masking, row access, projection)
  - Data type matching rules
  - Context functions (`IS_ROLE_IN_SESSION`, `CURRENT_ROLE`, etc.)
  - Tag-based masking mechanics
  - Memoizable function syntax
  - Privileges and runtime behavior

- **L2 — Proven Patterns** (`data-policy/L2_proven_patterns.md`)
  - **Pattern 1:** Attribute-Based Access Control (ABAC) — column masking and row access policies that use tags for attributes
  - **Pattern 2:** Split Pattern — extract unmask logic into a memoizable function, then call it from all policies (key pattern for extending to new data types)

- **L3 — Best Practices** (`data-policy/L3_best_practices.md`)
  - **Check similar tables first** (before creating any new policy)
  - Use generic, reusable policies (avoid table-specific sprawl)
  - Centralize policies in a governance database
  - Use memoizable functions for lookups
  - Use IS_ROLE_IN_SESSION() for role checks
  - Anti-patterns to avoid
  - Visual pattern recognition for spotting bad policies

- **L4 — Guided Workflows**
  - **`data-policy/L4_workflow_create.md`** — Creating new policies
    - Discovery questions to understand requirements
    - Policy type selection (masking, row access, projection)
    - Check existing policies — **is it split?** (uses shared function vs. embedded logic)
    - Apply split pattern when extending policies
    - Verification steps
  - **`data-policy/L4_workflow_audit.md`** — Auditing existing policies
    - Policy discovery queries
    - Evaluation checklist with severity levels
    - Health report generation
    - Safe migration workflow with rollback

- **Reference — Compliance Regulations** (`../reference/data-policy/compliance_reference.md`)
  - PCI-DSS (payment card data)
  - HIPAA (healthcare/PHI)
  - GDPR (EU personal data)
  - CCPA/CPRA (California consumer data)
  - SOX (financial reporting)
  - FERPA (student records)
  - Quick lookup table by data type and region

## Setup

Load **only** the file(s) matching the detected intent — do not load all layers upfront:

| Detected Intent | Load |
|---|---|
| CONCEPTS — syntax, how to write, data types, policy definition | `data-policy/L1_core_concepts.md` |
| PATTERNS — example, ABAC, template, show me how | `data-policy/L2_proven_patterns.md` |
| BEST_PRACTICES — best practice, anti-pattern, memoizable | `data-policy/L3_best_practices.md` |
| CREATE — create policy, new policy, mask column, extend policy | `data-policy/L4_workflow_create.md` |
| AUDIT — audit, review, inventory, health check, consolidate | `data-policy/L4_workflow_audit.md` |
| COMPLIANCE — HIPAA, GDPR, PCI, CCPA, SOX, FERPA, privacy law | `../reference/data-policy/compliance_reference.md` |

If intent spans multiple layers (e.g., "create a best-practice masking policy"), load the L3 + L4-create files. If intent is unclear, ask clarifying questions from user to decide which workflows are needed.

## Intent Detection

| Intent | Triggers | Action |
|--------|----------|--------|
| CONCEPTS | "syntax", "how to write", "data types", "policy definition" | Use `data-policy/L1_core_concepts.md` |
| PATTERNS | "example", "ABAC", "template", "show me how", "pattern" | Use `data-policy/L2_proven_patterns.md` |
| BEST_PRACTICES | "best practice", "should I", "anti-pattern", "governance", "memoizable" | Use `data-policy/L3_best_practices.md` |
| CREATE | "create policy", "new policy", "mask column", "restrict access", "extend policy", "same rules" | Use `data-policy/L4_workflow_create.md` |
| AUDIT | "audit policies", "review policies", "inventory", "health check", "scattered policies", "consolidate", "migrate" | Use `data-policy/L4_workflow_audit.md` |
| COMPLIANCE | "regulation", "HIPAA", "GDPR", "PCI", "CCPA", "SOX", "FERPA", "compliance", "healthcare", "financial", "privacy law" | Use `../reference/data-policy/compliance_reference.md` |

## Execution Rule

| SQL Type | Confirmation Required? |
|---|---|
| `SELECT` — discovery, audit queries, policy inventory, checking existing policies | Execute immediately, no confirmation |
| `CREATE`, `ALTER`, `DROP`, `APPLY` — creating/modifying/dropping policies or functions | Show what will be executed and wait for user confirmation |

## Workflow

### Step 0: Verify Session Context (once per session)

Run this **once at the start of the first query**. If you have already confirmed warehouse, database, and schema are set earlier in this conversation, skip this step.

Check the current session context:

```sql
SELECT
    CURRENT_USER()      AS current_user,
    CURRENT_ROLE()      AS current_role,
    CURRENT_DATABASE()  AS current_database,
    CURRENT_SCHEMA()    AS current_schema,
    CURRENT_WAREHOUSE() AS current_warehouse;
```

Fix any NULL or mismatched values before continuing:

| Field | Fix if NULL / wrong |
|-------|---------------------|
| `current_warehouse` | `USE WAREHOUSE <name>;` |
| `current_database`  | `USE DATABASE <database>;` |
| `current_schema`    | `USE SCHEMA <database>.<schema>;` |

**⚠️ STOP if warehouse is NULL** — policy `CREATE` and `APPLY` statements require an active warehouse.

### Step 1: Clarify intent
- Identify which layer the user needs: L1 (concepts), L2 (patterns), L3 (best practices), or L4 (workflows).
- If unclear, start with `L4_workflow_create.md`.

**⚠️ STOP**: Confirm the chosen track before drafting SQL.

### Step 2: Provide guidance
- Use the relevant document to respond.
- If drafting SQL, keep it minimal and ask for object names and roles.
- Run any discovery/audit SELECTs immediately to gather context; confirm before any state-changing SQL.

### Step 3: Offer Auto-Classification (CREATE workflows only)

After a masking or row access policy is successfully applied, check whether auto-classification is already enabled on the target database. This ensures the columns being protected stay classified over time as new data arrives.

**Actions:**
1. Execute immediately (no confirmation needed — this is a SELECT):
   ```sql
   SHOW PARAMETERS LIKE 'CLASSIFICATION_PROFILE' IN DATABASE <database>;
   ```
2. Inspect the `value` column in the result:
   - **Non-empty value** → auto-classification is already enabled. Inform the user and skip to Step 4.
   - **Empty value** → auto-classification is NOT enabled. Continue to step 3.

3. **⚠️ STOP** — Use `ask_user_question` to offer:

   > "Auto-classification is not enabled on `<database>`. Would you like to enable it so Snowflake automatically detects and tags sensitive columns on a schedule?"

   | Option | Description |
   |--------|-------------|
   | **Yes, enable auto-classification** | Set up a classification profile and attach it to the database. |
   | **No, skip for now** | Continue without enabling — you can set it up later. |

4. **If user confirms** → Load `workflows/sensitive-data-classification.md` and start at **Step 1** (create classification profile) through **Step 5** (attach profile to database).

5. **If user declines** → Skip to Step 4 (Verify).

### Step 4: Verify
- Ask how the user wants to validate outcomes (roles, test queries, or policy inventory).

## Stopping Points
- ✋ After Step 1 (track confirmed)
- ✋ After Step 2 (design or SQL drafted)
- ✋ Step 3: Before enabling auto-classification (user must confirm)
- ✋ After audit scope confirmed (audit workflow)
- ✋ After health report presented (audit workflow)
- ✋ Before any `CREATE`, `ALTER`, `DROP`, or `APPLY` statement (all workflows)

## Output
- Clear policy recommendation or draft SQL aligned to the chosen track
- Health report with recommendations (for audit workflow)
