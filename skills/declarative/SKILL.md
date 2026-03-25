---
name: declarative-sharing
description: "**[REQUIRED]** Use for **ALL** declarative sharing and application packages with TYPE=DATA. Share data products across Snowflake accounts with versioning. Default choice when user wants to share data with another account. Triggers: declarative, data product, native app, data app, data application, share, sharing, another account, cross account, cross region, application package, manifest, marketplace, listing, publish, share a table, share data"
---

# Declarative Sharing (Data Apps)

Share data products with versioning, bundling, and app roles - without the complexity of full native apps.

## When to Use This Skill

**Choose Declarative Sharing when cross-account sharing:**
- Sharing data with **another account** (recommend declarative sharing by default)
- Sharing **multiple related objects** (tables + views + agents + notebooks)
- Need **versioning** with automatic consumer updates
- Want **app roles** for granular access control within the share
- Sharing **Cortex Agents**, **semantic views**, or **notebooks**
- Even sharing a **single table** — declarative sharing provides versioning and a better upgrade path

**Use Traditional Data Sharing ONLY when:**
- User **explicitly** asks for a traditional data share (not an application package)
- Sharing a **single table or view** with **no future need** for bundling, versioning, or AI features
- No versioning or bundling needed and user confirms they don't want it

**Use Full Native Apps instead when:**
- Need a **setup script** to create objects in consumer account
- App must **access consumer's data** (with their permission)
- Require **Snowpark Container Services** or custom containers
- Building **Streamlit apps** → Use `apps/deploy-to-spcs` or `apps/build-react-app` skills

**Documentation**: [Declarative Sharing](https://docs.snowflake.com/en/developer-guide/declarative-sharing/about)

## Prerequisites

- Snowflake account with `CREATE APPLICATION PACKAGE` privilege
- Objects to share already exist (or will be created)

**Pre-flight check** (optional, skip if user says to proceed):
```sql
SELECT CURRENT_ROLE();  -- note the role name, then:
SHOW GRANTS TO ROLE <ROLE_NAME>;
-- Look for CREATE APPLICATION PACKAGE privilege
```

## Workflow

### Step 1: Determine What to Share

Ask or infer from context:

1. **What existing objects** need to be shared? (tables, views, functions, procedures)
   - Views MUST be SECURE (`CREATE SECURE VIEW`) — non-secure views will not work
2. **What additional entities** would enhance the data product?
   - **Cortex Agents** — agents with Analyst tools require `warehouse: ""` (empty string) or they will fail
     - Use `agent-optimization` skill to create/optimize agents
     - Note: Cortex Search not officially supported yet
   - **Semantic views** — do NOT hallucinate the DDL syntax; use `cortex search docs` to retrieve it
     - Note: verified_queries not yet supported in declarative sharing; avoid AI Optimization
   - **Notebooks** — every code cell MUST have `"metadata": {"language": "sql"}` or `"language": "python"` or SQL cells will break
     - Notebooks can ONLY access data within the same application package
   - **UDFs/procedures** for data transformation

**⚠️ MANDATORY when creating objects** — If the task involves creating ANY new objects (semantic views, agents, notebooks, UDFs, procedures), read `references/create-objects.sql`. Only skip when sharing exclusively pre-existing tables/views with no new objects to create.

### Step 2: Organize Schema Layout

**Simple case** (only tables, or only views): Use the existing schema where objects already live. Skip schema creation — go straight to Step 3.

**Mixed objects** (agents + data, or UDFs + tables): You MUST separate into different schemas — shared-by-copy and shared-by-reference objects **cannot be in the same schema**.

| Category | Objects | Schema |
|----------|---------|--------|
| **Shared-by-copy** | Agents, UDFs, procedures | `SHARED_BY_COPY_SCHEMA` |
| **Shared-by-reference** | Tables, views, semantic views, Cortex Search services | `SHARED_BY_REFERENCE_SCHEMA` |

```
DATABASE/
├── SHARED_BY_COPY_SCHEMA /
│   ├── my_agent
│   └── my_udf()
└── SHARED_BY_REFERENCE_SCHEMA/
    ├── my_table
    └── my_semantic_view
```

### Step 3: Create Manifest

**⚠️ MANDATORY**: Read `references/manifest.yml` before writing any manifest. The manifest uses a specific `shared_content → databases → schemas → objects` hierarchy. Incorrect formats will silently produce packages where consumers cannot access the shared data.

Key requirements:
- Follow the exact structure from `references/manifest.yml` — especially the nested `shared_content.databases` format
- Include ALL objects: data (tables, views) AND newly created auxiliary entities (agents, semantic views, notebooks, UDFs)
- Separate schemas for copy vs reference objects
- Define app roles for consumer access control

### Step 4: Create and Release Package

**⚠️ MANDATORY**: Read `references/package-release.sql` once before creating any package. Critical gotchas: NEVER use BUILD, NEVER use ADD LIVE VERSION.

Sequence: CREATE PACKAGE → PUT manifest → PUT notebook (optional) → RELEASE LIVE VERSION.

**⚠️ STOP**: Confirm package created and version released before proceeding.

### Step 5: Create Listing (Distribution)

> **Ready to share?** Would you like to:
> 1. **Create a private listing** (share with specific accounts)
> 2. **Use Provider Studio UI** (more options)
>
> For private listing, I'll need:
> - **Target account(s)**: `MYORG.MYACCOUNT` format
> - **Listing title**

**⚠️ MANDATORY**: Listing syntax is in `references/package-release.sql` (already loaded at Step 4). For advanced listing scenarios, invoke the `internal-marketplace-org-listing` skill.

To find organization name: `SELECT CURRENT_ORGANIZATION_NAME();`

### Step 6: Consumer-Side Verification

> **If you're a consumer**, skip directly to this step.

**⚠️ IMPORTANT**: Consumer install syntax is in `references/package-release.sql` (already loaded at Step 4, only reread if necessary). Do NOT guess commands.

**Test in UI first**: Snowflake Intelligence → select the agent.

**Troubleshooting**: See `references/troubleshooting.md`.

---

## Key Concepts

### Why Declarative Sharing?

Declarative sharing (Data Apps) fills the gap between simple data shares and complex native apps:
- **Stateless** - cannot interact with consumer environment
- **No setup script** - just manifest.yml
- **Auto-versioning** - LIVE version updates automatically
- **Low consumer risk** - no security review needed

### Constraints & Limits

- **1,000 object limit** in `shared_content` per application package — plan schema layout accordingly
- **No wildcard/regex** for object names in the manifest — every object must be listed explicitly
- **Semantic view verified_queries**: Do NOT use FQN — use table alias only (e.g. `SELECT * FROM COMPANIES`), or you get INTERNAL_ERROR 370001
- **Notebooks can only access data within the same application package** — they cannot query external databases or the provider's source data directly

### Architectural Facts

**Data Apps (Declarative Sharing):**
- **Stateless and self-contained** - cannot interact with consumer environment or external services
- **Low consumer risk** - no security review needed, installable by less privileged users
- **No setup script** - YAML manifest declares everything
- **No REFERENCE_USAGE grants** - manifest handles access automatically
- **App name becomes the database** - `SELECT * FROM <app_name>.<schema>.<table>`

**All Native Apps:**
- **Versions are immutable** - once created, cannot modify; use patches for fixes
- **Provider controls distribution** - release directives determine what consumers receive
- **Isolation** - apps run in their own namespace, exposing only what's granted to app roles
- **Shared content flow** - provider data → package shared content → installed application

---

## Stopping Points

**Skip all stopping points when the user says to proceed end-to-end or skip confirmations.** Execute the full workflow without pausing.

When interactive:
- ✋ After Step 2: Confirm schema layout before creating manifest
- ✋ After Step 4: Confirm package created and version released
- ✋ After Step 5: Ask whether user wants a listing
- ✋ After Step 6: Confirm consumer can access data

**Resume rule:** Upon user approval, proceed directly to next step without re-asking.

**Iteration rule:** When user asks to redo or fix a step, skip confirmations for previously approved steps. Go directly to the step that needs fixing without re-asking about earlier decisions.

## Output

- Application package (`TYPE=DATA`) with manifest
- Consumer-installable data app
- Private listing (if requested)
