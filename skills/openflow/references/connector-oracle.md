---
name: openflow-connector-oracle
description: Oracle CDC connector for Openflow. Covers licensing choice (Embedded vs BYOL), Oracle XStream prerequisites, connector configuration, DBA best practices, and troubleshooting. Use for Oracle database replication to Snowflake.
---

<!--
MAINTAINER NOTE:

This file is routed from two locations (added in the same PR):

1. connector-main.md — "Connectors with Specific Documentation" table:
   | Oracle, Oracle CDC, Oracle database replication | `oracle-embedded-license` or `oracle-independent-license` | `references/connector-oracle.md` |

2. SKILL.md — Reference Index under "Connector Operations":
   | `references/connector-oracle.md` | Oracle CDC connector (Embedded & BYOL licensing, XStream setup, troubleshooting) |
-->

# Oracle CDC Connector

The Openflow Connector for Oracle replicates data from an Oracle database to Snowflake in near-real-time using Oracle XStream. It supports two licensing models: Embedded (Snowflake-provided) and Independent (Bring Your Own License).

**Note:** These operations modify service state. Apply the Check-Act-Check pattern from `references/core-guidelines.md`.

## Scope

This reference covers:
- Licensing decision (Embedded vs BYOL) and ORGADMIN commercial activation
- Oracle database XStream prerequisites (the primary setup complexity)
- DBA best practices for safe XStream deployment
- Connector parameter configuration
- Troubleshooting XStream and replication issues

For other connectors, see `references/connector-main.md`.

## Workflow Summary

Complete ALL steps before starting the flow:

0. **Commercial Terms** — ORGADMIN enables Oracle Connector Terms; start trial (Embedded only)
1. **Network Access** — EAI attached to runtime (SPCS only)
2. **Network Validate** — Test connectivity to Oracle database endpoint
3. **Deploy** — Deploy the connector flow (`oracle-embedded-license` or `oracle-independent-license`)
4. **Parameters** — Configure source, destination, and ingestion parameters
5. **Asset Uploads** — None required (OCI driver is bundled)
6. **Verify Controllers** — Run `verify_config` before enabling
7. **Enable Controllers** — Enable after verification passes
8. **Verify Processors** — Run `verify_config` after controllers enabled
9. **Verify XStream Connectivity** — Single-processor verification on CaptureChangeOracle confirms XStream server is reachable and healthy
10. **Start** — Start the flow
11. **Validate** — Confirm data is flowing

**Common failure:** Skipping Oracle database prerequisites (Steps 1-8 in [Oracle Database Prerequisites](#oracle-database-prerequisites)) causes XStream connection errors at controller enable time.

See [Deployment Workflow](#deployment-workflow) for detailed instructions.

---

## Licensing Decision (Resolve First)

Unlike other CDC connectors, Oracle requires a licensing decision **before** any technical work. The wrong choice can cause deployment failure or unintended financial commitments.

### Decision Tree

Ask the user:

> "Does your organization already have an Oracle GoldenGate license (or another Oracle license that includes XStream entitlements)?"
>
> - **Yes** → Independent License (BYOL)
> - **No** → Check eligibility for Embedded License below

### Eligibility Check (Embedded License)

The Embedded License is **not available** if any of the following apply:

| Restriction | Impact |
|-------------|--------|
| Public Sector (Government, Education) | Must use BYOL |
| GCP Marketplace customer | Must use BYOL |
| Third-party reseller (e.g., CDW, Optiv) | Must use BYOL |
| Legacy Snowflake pricing (non-Snowspeed) | Must use BYOL |

If eligible, the customer can proceed with Embedded License through their Snowflake Capacity.

### Licensing Comparison

| Consideration | Embedded License | Independent License (BYOL) |
|---------------|-----------------|---------------------------|
| Oracle License | Snowflake provides XStream license | Customer's existing GoldenGate/XStream license |
| Connector Fee | $70/core/month (license) + $40/core/month (S&M) = **$110/core/month** | **$0** connector fee |
| Billing | Drawn from Snowflake Capacity balance | Standard Snowflake compute/storage only |
| Trial | 60-day free trial (max 16 licensed cores) | No trial (not needed) |
| Commitment | Non-cancelable 36-month term after trial | None from Snowflake |
| Core Factor | Customer must report core count × Oracle Processor Core Factor | Not required |
| Configuration | Requires core count and multiplier in connector parameters | No billing parameters |

**Core Factor Example:** A 24-core Intel server = 24 cores × 0.5 factor = 12 Licensed Cores → 12 × $110 = $1,320/month.

### Embedded License Lifecycle (Critical)

| Phase | Timeline | What Happens |
|-------|----------|--------------|
| Trial | Days 1-60 | Free for up to 16 licensed cores |
| Auto-conversion | Day 61 | Billing starts automatically. Must cancel before Day 60 to avoid charges |
| Commitment | Months 1-36 | Non-cancelable. Full remaining balance due if Snowflake agreement terminated early |
| Post-term | Month 37+ | License fee drops to $0. S&M ($40/core/month) continues, auto-renews annually |
| S&M opt-out | After month 36 | Connector processors permanently locked when S&M expires. New license required to resume (resets 36-month term) |

**WARNING:** Advise users to set a calendar reminder for Day 55 if they want the option to cancel the trial.

### ORGADMIN: Enable Commercial Terms

This step must be performed by the Organization Administrator (ORGADMIN) **before** the connector can be deployed. No connector deployment or Oracle-side setup is needed first — this is purely an administrative step in Snowsight.

#### Part 1: Accept Terms (Both License Types)

1. Log in to Snowsight with the **ORGADMIN** role.
2. Navigate to **Admin >> Terms**.
3. Locate **Oracle Connector Terms** in the list.
4. Click **Review & Enable**.

**Outcome:** Two things happen immediately:
- The Openflow Connector for Oracle becomes visible in the Connector Catalog.
- A new tab **Openflow for Oracle** appears in Admin >> Terms, showing a **Trial Status** card with status "Ready to Activate" (Embedded) or subscription inventory (BYOL).

#### Part 2a: Start Trial (Embedded License Only)

The trial can be started immediately after accepting terms — no connector deployment is needed first. However, the connector's capture processor will not run until the trial is active.

1. Navigate to **Admin >> Terms >> Openflow for Oracle** (available immediately after Part 1).
2. Locate the **Trial Status** card (status: "Ready to Activate").
3. Click **Start Trial**.
4. Confirm: Accept the terms to start the 60-day clock.

**Note:** You can start the trial now and proceed with Oracle database prerequisites and connector deployment in parallel. The trial clock runs regardless of whether the connector is deployed.

#### Part 2b: Independent License (BYOL)

No trial activation is needed. After accepting terms in Part 1, proceed directly to connector configuration.

#### Part 3: Verify (Both License Types)

After the connector is deployed, configured, and connects to the source database, return to **Admin >> Terms >> Openflow for Oracle** and verify:

| UI Section | What to Verify | Success Criteria |
|------------|----------------|------------------|
| Trial Status | Countdown timer (Embedded only) | Shows "X days remaining" or "Active" |
| Cost Projections | Total Oracle database CPU cores | Core count matches source Oracle system |
| Subscription Inventory | Database instance list | Instances listed, CPU counts correct, License Status "Active" |

**Note:** Cost Projections and Subscription Inventory only populate after the connector successfully connects to the Oracle source and reports core counts.

---

## Collect Checklist

Gather this information from the user **before** proceeding with deployment.

### Licensing & Commercial (Required)

| Item | Description | Collected |
|------|-------------|-----------|
| License type | Embedded or Independent (BYOL) | [ ] |
| ORGADMIN enabled terms | Admin >> Terms >> Oracle Connector Terms accepted | [ ] |
| Trial started (Embedded only) | Admin >> Terms >> Openflow for Oracle >> Start Trial | [ ] |

### Oracle Source Configuration (Required)

| Item | Example | Collected |
|------|---------|-----------|
| Oracle version | 19c, 21c, 23c (must be 12cR2+) | [ ] |
| Platform | On-premises, Exadata, OCI, AWS RDS Custom, AWS RDS Standard Single-tenant | [ ] |
| Connection URL | `jdbc:oracle:thin:@//host:1521/YOUR_PDB_NAME` (points to the **PDB** containing data; for non-CDB, use the database service name) | [ ] |
| XStream Out Server URL | `jdbc:oracle:oci:@host:1521/CDB_SERVICE` (points to the **CDB root** service — XStream Outbound Servers are registered at CDB$ROOT; for **non-CDB**, use the same database service name as the Connection URL) | [ ] |
| XStream Out Server Name | User-defined during Oracle prerequisite Step 6 (no default — must ask user) | [ ] |
| Connect username | e.g., `c##connectuser` | [ ] |
| Connect password | (sensitive) | [ ] |
| Tables to replicate | Always three-part `DATABASE_NAME.SCHEMA.TABLE`. The `DATABASE_NAME` is the Oracle `GLOBAL_DB_NAME` (for CDB this is the PDB name, e.g., `FREEPDB1.PROCUREMENT.ORDERS`; for non-CDB it is the database name, e.g., `ORCL.PROCUREMENT.ORDERS`). Query `SELECT property_value FROM database_properties WHERE property_name = 'GLOBAL_DB_NAME';` to obtain it. | [ ] |
| Core count (Embedded only) | Physical processor cores on source Oracle DB | [ ] |
| Core factor (Embedded only) | Oracle Processor Core Factor (e.g., 0.5 for Intel) | [ ] |

### Snowflake Configuration (Required)

| Item | Description | Collected |
|------|-------------|-----------|
| Destination Database | Database for replicated data (must already exist — see [Snowflake Account Prerequisites](#snowflake-account-prerequisites)) | [ ] |
| Snowflake Role | Role with CREATE SCHEMA privileges on destination database | [ ] |
| Snowflake Warehouse | Warehouse for processing | [ ] |

### Snowflake Prerequisites (User Must Complete)

| Prerequisite | Status |
|--------------|--------|
| Destination database created | [ ] |
| Role created with USAGE + CREATE SCHEMA on destination database | [ ] |
| Warehouse granted to role | [ ] |
| Role granted to service user (BYOC) or service role configured (SPCS) | [ ] |

See [Snowflake Account Prerequisites](#snowflake-account-prerequisites) for setup SQL.

### Oracle Prerequisites (User Must Complete)

| Prerequisite | Status |
|--------------|--------|
| ARCHIVELOG mode enabled | [ ] |
| XStream replication enabled (`enable_goldengate_replication=TRUE`) | [ ] |
| Supplemental logging enabled (on target tables) | [ ] |
| XStream administrator user created | [ ] |
| XStream connect user created with required privileges | [ ] |
| XStream Outbound Server created | [ ] |
| Tables have primary keys | [ ] |

**Do not proceed until all required items are collected and prerequisites confirmed.**

---

## Supported Platforms and Limitations

### Supported Oracle Versions & Platforms

- Oracle database versions **12cR2 and later** (including 23ai and 23ai Free)
- On-premises servers
- Oracle Exadata
- OCI VM/Bare Metal
- AWS Custom RDS for Oracle
- AWS Standard Single-tenant RDS for Oracle

**Note:** Oracle 23ai Free includes XStream support. Do not tell users that 23ai Free lacks XStream — it does.

### Unsupported

- AWS Standard **Multi-tenant** RDS for Oracle
- Oracle Autonomous Databases (ATP/ADW)
- Oracle SaaS (Fusion Cloud Applications, NetSuite)

### Limitations

- Only tables containing **primary keys** can be replicated.
- The connector works within a **single database/container** (PDB or CDB). To replicate tables from multiple containers, configure separate connector instances.
- The connector does **not** support re-adding a column after it is dropped.
- Runtime size must be at least **Medium**. Use larger for high data volumes.
- Multi-node runtimes are **not** supported. Set Min nodes and Max nodes to **1**.
- Requires Openflow deployment version **0.55.0 or later** for BYOC.

**Resilience Warning:** The connector relies on the specific SCN state of the source database. **Do not** perform RMAN DUPLICATE or database restores on a database actively connected to Openflow. Doing so will break the replication stream and may require a new license generation (and associated costs) to resolve.

---

## Official Documentation

Refer to the official Snowflake documentation for current requirements. These pages are the authoritative source; this skill reference provides operational guidance and troubleshooting beyond what the docs cover.

- **Oracle Connector Overview & Prerequisites:** [Set up the Openflow Connector for Oracle](https://docs.snowflake.com/en/user-guide/data-integration/openflow/connectors/oracle/about)

---

## Oracle Database Prerequisites

These steps must be completed by the **Oracle database administrator** before the connector can be configured.

**Note:** How you set up your Oracle database depends on your organization's security policies and database architecture (CDB, PDB, or combination). The instructions below are examples. Modify as required for your environment.

**Before starting, determine your Oracle architecture:**

```sql
SELECT CDB FROM V$DATABASE;
```

- **YES** → Multi-tenant (CDB with PDBs). Follow the CDB instructions below. Users require the `C##` prefix and `CONTAINER=ALL`.
- **NO** → Single-tenant (non-CDB). Follow the non-CDB alternatives noted in each step. Users are regular database users (no `C##` prefix). Both `Oracle Connection URL` and `XStream Out Server URL` point to the same database service.

### Step 1: Configure Archived Redo Log Retention

You must enable ARCHIVELOG mode to ensure change data is available for replication.

**Verify ARCHIVELOG mode:**

```sql
SELECT LOG_MODE, FORCE_LOGGING FROM V$DATABASE;
```

**For AWS RDS (Standard):**

```sql
BEGIN
  rdsadmin.rdsadmin_util.set_configuration(
    name  => 'archivelog retention hours',
    value => '24'
  );
END;
/
COMMIT;
```

**For AWS RDS Custom:**

Create `/opt/aws/rdscustomagent/config/redo_logs_custom_configuration.json`:
```json
{"archivedLogRetentionHours": "24"}
```

Determine the retention period based on the volume of changes in your source database and your storage capacity.

### Step 2: Enable XStream and Supplemental Logging

XStream is included with Oracle Database and does not require additional software.

**Enable XStream replication:**

```sql
ALTER SYSTEM SET enable_goldengate_replication=TRUE SCOPE=BOTH;
ALTER SYSTEM SET STREAMS_POOL_SIZE = 2560M;
```

Snowflake recommends setting the streams pool size to **2.5 GB** (1 GB for Capture + 1 GB for Apply + 25% buffer).

**Enable supplemental logging:**

Snowflake recommends forcing logging on the database or tablespace level:

**CDB architecture:**

```sql
ALTER SESSION SET CONTAINER = CDB$ROOT;
ALTER DATABASE ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
```

**Non-CDB architecture:**

```sql
ALTER DATABASE ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
```

Alternatively, enable logging only on specific tables (recommended for production — see [DBA Best Practices](#dba-best-practices)):

```sql
ALTER TABLE schema_name.table_name ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
```

### Step 3: Create the XStream Administrator User

An XStream administrator user is required to manage XStream components.

**CDB architecture:**

The following example creates a dedicated common user in the root container of a CDB with a PDB.

```sql
-- Switch to root container
ALTER SESSION SET CONTAINER = CDB$ROOT;

-- Create tablespace for XStream admin in CDB
CREATE TABLESPACE xstream_adm_tbs DATAFILE '/path/to/your/cdb/xstream_adm_tbs.dbf'
  SIZE 25M REUSE AUTOEXTEND ON MAXSIZE UNLIMITED;

-- Create tablespace in PDB
ALTER SESSION SET CONTAINER = YOUR_PDB_NAME;
CREATE TABLESPACE xstream_adm_tbs DATAFILE '/path/to/your/pdb/xstream_adm_tbs.dbf'
  SIZE 25M REUSE AUTOEXTEND ON MAXSIZE UNLIMITED;

-- Switch back to root and create common user
ALTER SESSION SET CONTAINER = CDB$ROOT;

CREATE USER c##xstreamadmin IDENTIFIED BY "YOUR_XSTREAM_ADMIN_PASSWORD"
  DEFAULT TABLESPACE xstream_adm_tbs
  QUOTA UNLIMITED ON xstream_adm_tbs
  CONTAINER=ALL;
```

Note: The `c##` prefix indicates a common user in a CDB environment. `CONTAINER=ALL` grants privileges across all containers.

**Non-CDB architecture:**

```sql
CREATE TABLESPACE xstream_adm_tbs DATAFILE '/path/to/your/xstream_adm_tbs.dbf'
  SIZE 25M REUSE AUTOEXTEND ON MAXSIZE UNLIMITED;

CREATE USER xstreamadmin IDENTIFIED BY "YOUR_XSTREAM_ADMIN_PASSWORD"
  DEFAULT TABLESPACE xstream_adm_tbs
  QUOTA UNLIMITED ON xstream_adm_tbs;
```

Note: No `C##` prefix or `CONTAINER=ALL` in a non-CDB environment.

### Step 4: Grant XStream Administrator Privileges

**CDB architecture — Oracle Database 19c and 21c:**

```sql
GRANT CREATE SESSION, SET CONTAINER, EXECUTE ANY PROCEDURE, LOGMINING
  TO c##xstreamadmin CONTAINER=ALL;

BEGIN
  DBMS_XSTREAM_AUTH.GRANT_ADMIN_PRIVILEGE(
    grantee                => 'c##xstreamadmin',
    privilege_type         => 'CAPTURE',
    grant_select_privileges => TRUE,
    container              => 'ALL'
  );
END;
/
```

**CDB architecture — Oracle Database 23c:**

```sql
GRANT CREATE SESSION, SET CONTAINER, EXECUTE ANY PROCEDURE, LOGMINING, XSTREAM_CAPTURE
  TO c##xstreamadmin CONTAINER=ALL;
```

**Non-CDB architecture — Oracle Database 19c and 21c:**

```sql
GRANT CREATE SESSION, EXECUTE ANY PROCEDURE, LOGMINING
  TO xstreamadmin;

BEGIN
  DBMS_XSTREAM_AUTH.GRANT_ADMIN_PRIVILEGE(
    grantee                => 'xstreamadmin',
    privilege_type         => 'CAPTURE',
    grant_select_privileges => TRUE
  );
END;
/
```

**Non-CDB architecture — Oracle Database 23c:**

```sql
GRANT CREATE SESSION, EXECUTE ANY PROCEDURE, LOGMINING, XSTREAM_CAPTURE
  TO xstreamadmin;
```

### Step 5: Configure XStream Server Connect User

The connect user establishes a connection to the XStream Outbound Server and receives change data. This user needs:

- Read from XStream Outbound Server
- SELECT on data dictionary views (`ALL_USERS`, `ALL_TABLES`, `ALL_TAB_COLS`, `ALL_CONS_COLUMNS`, `ALL_CONSTRAINTS`, `V$DATABASE`)
- SELECT on all source tables to be replicated

**CDB architecture:**

```sql
ALTER SESSION SET CONTAINER = CDB$ROOT;

CREATE USER c##connectuser IDENTIFIED BY "YOUR_CONNECT_USER_PASSWORD"
  CONTAINER=ALL;

GRANT CREATE SESSION, SELECT_CATALOG_ROLE TO c##connectuser CONTAINER=ALL;
GRANT SELECT ANY TABLE TO c##connectuser CONTAINER=ALL;
GRANT LOCK ANY TABLE TO c##connectuser CONTAINER=ALL;
```

**Non-CDB architecture:**

```sql
CREATE USER connectuser IDENTIFIED BY "YOUR_CONNECT_USER_PASSWORD";

GRANT CREATE SESSION, SELECT_CATALOG_ROLE TO connectuser;
GRANT SELECT ANY TABLE TO connectuser;
GRANT LOCK ANY TABLE TO connectuser;
```

For more granular control, grant SELECT on specific tables instead of `SELECT ANY TABLE`.

### Step 6: Create XStream Outbound Server

The XStream Outbound Server captures changes from redo logs. Define which schemas or tables to replicate.

**Important:**
- A table in the XStream filtering rules must **also** be listed in the connector's ingestion parameters to be replicated.
- You can include an entire schema here and later specify only certain tables in the connector parameters.
- In a CDB, the Outbound Server can only be created from the root container (except Oracle 23ai which supports PDB-level creation).
- The `CREATE_OUTBOUND` command is the same for both CDB and non-CDB architectures. The only CDB-specific parameter is `source_container_name` (used to scope capture to a specific PDB).
- **Be selective** in production. Capturing everything impacts CPU, network, and queue performance. Use `DBMS_XSTREAM_ADM.ADD_TABLE_RULES` for granular table selection.

**Example 1: Capture all tables from all schemas (CDB: root + all PDBs; non-CDB: entire database):**

```sql
SET SERVEROUTPUT ON;
DECLARE
  tables  DBMS_UTILITY.UNCL_ARRAY;
  schemas DBMS_UTILITY.UNCL_ARRAY;
BEGIN
  tables(1)  := NULL;
  schemas(1) := NULL;
  DBMS_XSTREAM_ADM.CREATE_OUTBOUND(
    server_name  => 'XOUT1',
    table_names  => tables,
    schema_names => schemas,
    include_ddl  => TRUE
  );
  DBMS_OUTPUT.PUT_LINE('XStream Outbound Server created.');
EXCEPTION
  WHEN OTHERS THEN
    DBMS_OUTPUT.PUT_LINE('Error: ' || SQLERRM);
    RAISE;
END;
/
```

**Example 2: Capture all tables from a single schema in a specific PDB (CDB only):**

```sql
SET SERVEROUTPUT ON;
DECLARE
  tables  DBMS_UTILITY.UNCL_ARRAY;
  schemas DBMS_UTILITY.UNCL_ARRAY;
BEGIN
  tables(1)  := NULL;
  schemas(1) := 'schema_name';
  DBMS_XSTREAM_ADM.CREATE_OUTBOUND(
    server_name            => 'XOUT1',
    table_names            => tables,
    schema_names           => schemas,
    include_ddl            => TRUE,
    source_container_name  => 'YOUR_PDB_NAME'
  );
  DBMS_OUTPUT.PUT_LINE('XStream Outbound Server created.');
EXCEPTION
  WHEN OTHERS THEN
    DBMS_OUTPUT.PUT_LINE('Error: ' || SQLERRM);
    RAISE;
END;
/
```

### Step 7: Set Up the XStream Outbound Server Connect User

Associate the connect user with the Outbound Server:

**CDB architecture:**

```sql
BEGIN
  DBMS_XSTREAM_ADM.ALTER_OUTBOUND(
    server_name  => 'XOUT1',
    connect_user => 'c##connectuser'
  );
END;
/
```

**Non-CDB architecture:**

```sql
BEGIN
  DBMS_XSTREAM_ADM.ALTER_OUTBOUND(
    server_name  => 'XOUT1',
    connect_user => 'connectuser'
  );
END;
/
```

Note: The connect user name must match exactly what was created in Step 5 — with `C##` prefix for CDB, without for non-CDB.

### Step 8: Set Up the XStream Outbound Server Capture User (Optional)

If you configured a separate capture user, associate it with the Outbound Server. Skip this step if you want data captured by the user who created the server (the administrator).

```sql
BEGIN
  DBMS_XSTREAM_ADM.ALTER_OUTBOUND(
    server_name  => 'XOUT1',
    capture_user => 'yourcaptureuser'
  );
END;
/
```

---

## Snowflake Account Prerequisites

These steps must be completed in Snowflake **before** configuring the connector's destination parameters.

### Step 1: Create Destination Database

The connector writes replicated data into this database. It must already exist — the connector does **not** create it.

```sql
CREATE DATABASE IF NOT EXISTS <destination_database>;
```

### Step 2: Create a Role for the Connector

Create a dedicated role with the minimum privileges needed. The connector creates schemas and tables within the destination database automatically.

```sql
-- Create a dedicated role
CREATE ROLE IF NOT EXISTS OPENFLOW_ORACLE_ROLE;

-- Grant database-level privileges
GRANT USAGE ON DATABASE <destination_database> TO ROLE OPENFLOW_ORACLE_ROLE;
GRANT CREATE SCHEMA ON DATABASE <destination_database> TO ROLE OPENFLOW_ORACLE_ROLE;

-- Grant warehouse usage
GRANT USAGE ON WAREHOUSE <warehouse_name> TO ROLE OPENFLOW_ORACLE_ROLE;
```

**On SPCS:** The runtime's service role needs these grants. The connector runs as the service role associated with the Openflow runtime compute pool. **Load** `references/ops-snowflake-auth.md` for details on SPCS authentication.

**On BYOC:** Grant the role to the service user that holds the key-pair credentials:

```sql
GRANT ROLE OPENFLOW_ORACLE_ROLE TO USER <service_user>;
```

### Step 3: Verify Permissions

Confirm the role can create schemas in the destination database:

```sql
USE ROLE OPENFLOW_ORACLE_ROLE;
USE DATABASE <destination_database>;
CREATE SCHEMA IF NOT EXISTS _openflow_test;
DROP SCHEMA _openflow_test;
```

If either statement fails, check the grants above.

For full Snowflake authentication configuration (key-pair, session token, account identifier), **Load** `references/ops-snowflake-auth.md`.

---

## DBA Best Practices

These recommendations are based on stress-testing Oracle XStream under high-throughput OLTP workloads. They help DBAs enable CDC safely without risking production stability.

### 1. Check I/O Headroom First

Before enabling CDC, check current `log_file_sync` waits:

- If you are already seeing **>5-10ms** waits regularly, **solve your storage I/O bottleneck first**.
- CDC adds redo volume (approximately 1.5x), not latency — but volume becomes latency if the pipe is full.
- The cost of CDC is primarily in **I/O, not CPU**. CPU overhead from XStream itself is negligible (~3%).

### 2. Resize Your Redo Logs

Legacy 500 MB Redo Log files will be insufficient.

- **Recommendation:** Increase Online Redo Log size to **4 GB - 8 GB**.
- **Why:** With increased redo volume from supplemental logging, small logs cause frequent log switching (checkpoints), which pauses the database.

### 3. Set STREAMS_POOL_SIZE (Safety Valve)

Do not let Oracle manage this automatically via the Shared Pool. Isolate XStream memory.

- **Recommendation:** Allocate a dedicated `STREAMS_POOL_SIZE` of **2.5 GB** (already set in Step 2 above).
- **Why:** This acts as a circuit breaker. If the replication pipeline slows or transaction volume spikes, the pool fills and XStream pauses. It will **not** eat into the Buffer Cache or crash the instance — it will simply lag. This prioritizes OLTP stability over replication latency.

### 4. Use Surgical Logging, Not Database-Wide

In production, do **not** run `ALTER DATABASE ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS` on the entire database.

- **Recommendation:** Enable supplemental logging only on specific tables being replicated:

```sql
ALTER TABLE schema_name.table_name ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
```

- **Why:** The connector requires ALL columns to be logged to fully reconstruct the payload, but this should be applied surgically to the tables in scope only.

### 5. Monitor XStream Health

Use these views to ensure XStream is healthy and respecting resource boundaries:

| View | What to Check | Healthy State |
|------|---------------|---------------|
| `V$XSTREAM_CAPTURE` | `STATE` and `LATENCY_SECONDS` | `CAPTURING CHANGES`, low latency |
| `V$STREAMS_POOL_STATISTICS` | `TOTAL_MEMORY_ALLOCATED` | Below `STREAMS_POOL_SIZE` cap |
| `V$XSTREAM_OUTBOUND_SERVER` | Connection state | `SENDING CHANGES` |

### 6. Consider Downstream Capture for Extreme Scale

If your production database runs at **>80% CPU** consistently or generates massive redo volumes (1 TB+ daily), running any additional process is a risk.

- **Recommendation:** Use the **Downstream Capture** model — ship redo logs to a secondary, passive Oracle instance where XStream runs.
- **Result:** Zero CPU or memory footprint on the production source. The only impact is network bandwidth for log shipping.

---

## Flow Names

| Licensing Model | Flow Name |
|-----------------|-----------|
| Embedded (Snowflake-provided) | `oracle-embedded-license` |
| Independent (BYOL) | `oracle-independent-license` |

---

## Deployment Workflow

Follow the main workflow in `references/connector-main.md`. This section provides Oracle-specific details for each step.

### 0. Enable Commercial Terms (Unique to Oracle)

**Before any technical work**, the ORGADMIN must enable commercial terms and (for Embedded) start the trial. See [ORGADMIN: Enable Commercial Terms](#orgadmin-enable-commercial-terms) above.

### 1. Network Access (SPCS Only)

**Load** `references/platform-eai.md` for EAI setup.

### 2. Network Validate (SPCS Only)

**Load** `references/ops-network-testing.md` and test connectivity to the Oracle database endpoint.

Test targets (replace with actual values):

```python
targets = [
    {"host": "oracle-host.example.com", "port": 1521, "type": "JDBC/Oracle"},
]
```

**Important:** Network rules are host:port specific. A `SocketTimeoutException` after DNS success indicates the port is not in the network rule.

### 3. Deploy

**Load** `references/ops-flow-deploy.md`. Flow names: `oracle-embedded-license` or `oracle-independent-license`.

### 4. Handle Parameters

Configure parameters in order:

1. **Source Parameters** — See [Oracle Source Parameters](#oracle-source-parameters) below
2. **Destination Parameters** — **Load** `references/ops-snowflake-auth.md`
3. **Ingestion Parameters** — See [Oracle Ingestion Parameters](#oracle-ingestion-parameters) below

Use `references/ops-parameters-main.md` for configuration commands.

### 5. Asset Uploads

No JDBC driver upload is required for Oracle. The connector uses the Oracle OCI driver which is bundled.

### 6. Verify Controllers

```bash
nipyapi --profile <profile> ci verify_config --process_group_id "<pg-id>" --verify_processors=false
```

**If verification fails:** Fix parameter configuration (connection URL, credentials) before proceeding.

### 7. Enable Controllers

**Load** `references/ops-flow-lifecycle.md` (Enable Controllers Only section).

After enabling, check for errors:
- All controllers show `ENABLED`
- Check bulletins for Oracle connection or authentication errors

### 8. Verify Processors

```bash
nipyapi --profile <profile> ci verify_config --process_group_id "<pg-id>" --verify_controllers=false
```

### 9. Verify XStream Connectivity (Oracle-Specific)

After processor verification passes, run a targeted single-processor verification on the CaptureChangeOracle processor. This triggers the processor's internal XStream health checks — it queries `dba_capture`, `V$LOGMNR_SESSION`, and `V$XSTREAM_CAPTURE` to confirm the XStream Outbound Server is reachable and healthy **before** starting the flow. See `references/ops-config-verification.md` for background on single-component verification.

```python
import nipyapi
nipyapi.profiles.switch()

# Find the CaptureChangeOracle processor
processors = nipyapi.canvas.list_all_processors("<pg-id>")
capture_proc = [p for p in processors if "CaptureChangeOracle" in p.component.type][0]

# Run single-processor verification (processor must be STOPPED)
results = nipyapi.canvas.verify_processor(capture_proc)

# Check results
for r in results:
    print(f"{r.verification_step_name}: {r.outcome}")
    if r.outcome == "FAILED":
        print(f"  Reason: {r.explanation}")
```

**If verification fails**, the XStream server is not reachable or not configured correctly. Common causes:
- XStream Outbound Server not started — run `SELECT STATUS FROM dba_capture WHERE CLIENT_NAME = '<xstream_server_name>';` on the Oracle database
- LogMiner session not active — check `V$LOGMNR_SESSION`
- Network connectivity — the SPCS container cannot reach the Oracle host on the OCI port
- Wrong XStream Out Server Name parameter — verify it matches the actual server name in Oracle

Do not proceed to Start until this verification passes.

### 10. Start

**Load** `references/ops-flow-lifecycle.md` for starting the flow.

### 11. Validate

After starting, validate data is flowing. See [Validate Data Flow](#validate-data-flow) below.

---

## Oracle Source Parameters

**Sensitive values:** Passwords are marked (sensitive). Ask user to provide directly. Never display these values — use `[REDACTED]` in confirmations.

| Parameter | Required | Description |
|-----------|----------|-------------|
| Oracle Connection URL | Yes | JDBC URL to the PDB holding the data. Example: `jdbc:oracle:thin:@//host:1521/YOUR_PDB_NAME`. Must point to the **PDB** (not the CDB root). For **non-CDB**, use the database service name (e.g., `jdbc:oracle:thin:@//host:1521/ORCL`). |
| Oracle Username | Yes | Username of the connect user with XStream Server access (e.g., `c##connectuser`). |
| Oracle Password | Yes | Password of the connect user (sensitive). |
| XStream Out Server Name | Yes | Name of the XStream Outbound Server created in Oracle prerequisite Step 6. **There is no default — always ask the user.** The examples in this file use `XOUT1` as a placeholder; do not assume this is the actual name. |
| XStream Out Server URL | Yes | JDBC URL for the XStream connection. Must use OCI driver. **CDB architecture:** must point to the **CDB root service** (not the PDB) — XStream Outbound Servers are registered at CDB$ROOT, so connecting to a PDB causes ORA-26701. Example: `jdbc:oracle:oci:@host:1521/CDB_SERVICE_NAME`. For Oracle 23ai Free the CDB service is typically `FREE`; for other editions check `SELECT NAME FROM V$SERVICES WHERE CON_ID = 1`. **Non-CDB architecture:** use the same database service name as the Oracle Connection URL (e.g., `jdbc:oracle:oci:@host:1521/ORCL`). |
| Oracle Database Processor Cores | Embedded only | Number of physical processor cores on the source Oracle database. |
| Oracle Database Processor Multiplier | Embedded only | Oracle Processor Core Factor (e.g., `0.5` for Intel). See Oracle Processor Core Factor Table. |
| XStream Billing Acknowledgement | Embedded only | Confirmation of the licensing agreement. |

---

## Oracle Ingestion Parameters

| Parameter | Required | Description |
|-----------|----------|-------------|
| Included Table Names | No* | Comma-separated fully-qualified table paths. Always uses three-part format: `DATABASE_NAME.SCHEMA.TABLE`. The `DATABASE_NAME` is the Oracle `GLOBAL_DB_NAME` — for CDB this is the PDB name, for non-CDB it is the database name. Example: `FREEPDB1.SALES.CUSTOMERS, FREEPDB1.SALES.ORDERS` |
| Included Table Regex | No* | Regex to match fully-qualified table paths (three-part format). Example: `FREEPDB1\.SALES\..*` to match all tables in the SALES schema within the FREEPDB1 database. |
| Filter JSON | No | JSON array to include specific columns based on regex for given tables. |
| Merge Task Schedule CRON | No | CRON expression for merge operations. Example: `* * * * * ?` for continuous merge. |
| Object Identifier Resolution | No | `Default, case-insensitive` (recommended — uppercases all identifiers) or `case-sensitive` (preserves case, requires double quotes in SQL). **Do not change after ingestion has begun.** |
| Snapshot Fetching Strategy | No | `SEQUENTIAL_BY_PRIMARY_KEY` (default) or `CONCURRENT_BY_ROWID` (parallel fetching for large tables). |
| Ingestion Type | No | `full` (default — snapshot then incremental) or `incremental` (skip snapshot, useful for reinstalling over existing data). |

*One of Included Table Names or Included Table Regex is required.

**CRITICAL — Table Name Format:** Oracle tables must always be specified using the **three-part** fully-qualified format: `DATABASE_NAME.SCHEMA_NAME.TABLE_NAME`. This differs from other CDC connectors which use two-part names. The `DATABASE_NAME` is determined by Oracle's `GLOBAL_DB_NAME` property:

```sql
SELECT property_value FROM database_properties WHERE property_name = 'GLOBAL_DB_NAME';
```

- **CDB (multi-tenant):** The `DATABASE_NAME` is the **PDB name** (e.g., `FREEPDB1`). Run the query from within the PDB.
- **Non-CDB (single-tenant):** The `DATABASE_NAME` is the **database name** (e.g., `ORCL`). The same three-part format applies.

**Gotcha:** Some databases return a name with a domain suffix (e.g., `FOO.EXAMPLE.COM` instead of just `FOO`). If this happens, the full domain-qualified name must be used and **double-quoted** in the table name specification because it contains dots.

**Snowflake schema naming:** The connector maps the three-part name to a Snowflake schema by joining the database name and schema with an underscore. For example, tables in `FREEPDB1.PROCUREMENT` land in Snowflake schema `FREEPDB1_PROCUREMENT`.

---

## Validate Data Flow

### Step 1: Check Flow Status

```bash
nipyapi --profile <profile> ci get_status --process_group_id "<pg-id>"
```

Expect:
- `running_processors` > 0
- `invalid_processors` = 0
- `bulletin_errors` = 0

### Step 2: Validate Target Objects Created

```sql
-- Check schema exists
SHOW SCHEMAS IN DATABASE <destination_database>;

-- Check tables exist
SHOW TABLES IN SCHEMA <destination_database>.<schema>;

-- Validate rows appearing
SELECT COUNT(*) FROM <destination_database>.<schema>.<table>;
```

### Step 3: Check Table Replication State

In the Openflow runtime canvas, right-click a processor group >> **Controller Services** >> find **Table State Store** >> click **More** >> **View State**.

| State | Meaning |
|-------|---------|
| `NEW` | Scheduled for replication, not started |
| `SNAPSHOT_REPLICATION` | Copying initial data |
| `INCREMENTAL_REPLICATION` | Streaming real-time changes |
| `FAILED` | Permanent failure (see Troubleshooting) |

State changes are logged: `Replication state for table <db>.<schema>.<table> changed from <old> to <new>`

---

## Known Issues

### StandardPrivateKeyService INVALID on SPCS

This controller is for BYOC KEY_PAIR authentication. On SPCS, SESSION_TOKEN auth is used instead.

**Impact:** None. Connector works correctly.

**Workaround:** Ignore (recommended), or delete the controller (causes local modifications).

---

## Troubleshooting

### Table Added but Doesn't Appear in Snowflake

1. **Check FQN format** in Oracle Ingestion Parameters. It must be `DATABASE_NAME.SCHEMA_NAME.TABLE_NAME` (three-part with database prefix).

2. **Verify the database name.** The connector uses the value from:

```sql
SELECT property_value FROM database_properties WHERE property_name = 'GLOBAL_DB_NAME';
```

Some databases return a domain-suffixed name (e.g., `FOO.EXAMPLE.COM` instead of `FOO`). The full name must be used and double-quoted.

3. **Data must reside in the same database instance** as the one specified in Oracle Connection URL. Cross-database replication within a single connector instance is not supported.

### No Changes in Incremental Load

Walk through these checks in order:

**1. Check XStream capture process status:**

```sql
SELECT CLIENT_NAME, STATUS, ERROR_MESSAGE FROM ALL_CAPTURE;
```

The status should be `ENABLED`.

- **If `DISABLED`:** The capture was stopped manually or the database was restarted. Restart it:

```sql
BEGIN
  DBMS_XSTREAM_ADM.START_OUTBOUND('<xstream_server_name>');
END;
/
```

- **If `ABORTED` with `ORA-01031: insufficient privileges`:** Redo logs needed for capture have been deleted. Start the outbound server (same command as above).

**2. Check logminer session status:**

```sql
SELECT SESSION_STATE
FROM V$LOGMNR_SESSION
WHERE SESSION_NAME = (
  SELECT CAPTURE_NAME FROM ALL_CAPTURE WHERE CLIENT_NAME = '<xstream_server_name>'
);
```

Status should be `ACTIVE`. If `UNKNOWN`, archived logs that logminer depended on were deleted. Verify:

```sql
SELECT * FROM V$ARCHIVED_LOG ORDER BY RECID;
```

Check the `DELETED` column for value `YES`.

**3. Check XStream capture state:**

```sql
SELECT STATE
FROM V$XSTREAM_CAPTURE
WHERE CAPTURE_NAME = (
  SELECT CAPTURE_NAME FROM ALL_CAPTURE WHERE CLIENT_NAME = '<xstream_server_name>'
);
```

- `CAPTURING CHANGES` or `WAITING FOR TRANSACTION` — Normal. If large redo volume, logminer may take time to catch up.
- `WAITING FOR REDO: FILE NA, THREAD X, SEQUENCE Y, SCN Z` — Logminer is waiting for an archived log file that was deleted.

**4. Verify XStream rules include target schemas/tables:**

```sql
SELECT STREAMS_NAME, SCHEMA_NAME, OBJECT_NAME, RULE_TYPE
FROM DBA_XSTREAM_RULES
WHERE STREAMS_NAME = '<xstream_server_name>';
```

### XStream Errors

**`ORA-21560: argument last_position is null, invalid, or out of range`**
The connector attempted to connect to an SCN position for which redo logs are no longer available. Redo log retention must be increased.

**`ORA-26701: Streams process <name> does not exist`**
Verify that:
- **CDB architecture:** The `XStream Out Server URL` points to the **CDB root service** (e.g., `jdbc:oracle:oci:@host:1521/FREE`), **not** the PDB. XStream Outbound Servers are registered at CDB$ROOT; connecting to a PDB will fail with this error even if the Outbound Server exists. To find the CDB root service name: `SELECT NAME FROM V$SERVICES WHERE CON_ID = 1;`
- **Non-CDB architecture:** The `XStream Out Server URL` should use the same database service name as the `Oracle Connection URL`. Both URLs point to the same instance.
- The XStream Outbound Server has been created on this instance with the expected name. Verify: `SELECT SERVER_NAME, CONNECT_USER, CAPTURE_NAME, SOURCE_DATABASE FROM DBA_XSTREAM_OUTBOUND;`
- In a CDB, the `Oracle Connection URL` (thin driver) should still point to the **PDB** — only the XStream OCI URL needs to point to CDB root.

**`ORA-26812: An active session currently attached to XStream server "<name>"`**
XStream allows only one client attached to an Outbound Server at a time. This error occurs when:
- A previous connector instance was stopped but its Oracle session was not released cleanly (common with ungraceful shutdowns or network disconnects).
- Two connector instances are trying to use the same XStream Outbound Server simultaneously.

To resolve:
1. Identify the stale session:
   ```sql
   SELECT SID, SERIAL#, USERNAME, PROGRAM, STATUS
   FROM V$SESSION
   WHERE USERNAME = '<connect_user>';
   ```
2. Kill the stale session:
   ```sql
   ALTER SYSTEM KILL SESSION 'SID,SERIAL#' IMMEDIATE;
   ```
3. If the session persists, wait for Oracle's dead connection detection (DCD) timeout to expire, or restart the Oracle listener.

**`ORA-01722: invalid number` when executing `DBMS_XSTREAM_ADM.CREATE_OUTBOUND`**
This misleading error typically means the outbound server **already exists**. Check:

```sql
SELECT * FROM ALL_XSTREAM_OUTBOUND WHERE SERVER_NAME = '<xstream_server_name>';
```

### SCN Diagnostics

Use this query to compare SCN values across capture, logminer, and database. Large gaps between consecutive SCN values indicate where bottlenecks exist:

```sql
WITH scn_values AS (
  SELECT 'CAPTURE' AS source, scn_type, scn_value,
    CASE scn_type
      WHEN 'FIRST_SCN' THEN 'Lowest SCN for capture restart'
      WHEN 'START_SCN' THEN 'SCN from which capture starts'
      WHEN 'CAPTURED_SCN' THEN 'Last redo log record scanned'
      WHEN 'LAST_ENQUEUED_SCN' THEN 'Last enqueued SCN'
      WHEN 'APPLIED_SCN' THEN 'Most recent dequeued SCN'
      WHEN 'REQUIRED_CHECKPOINT_SCN' THEN 'Lowest checkpoint SCN needing redo'
      WHEN 'MAX_CHECKPOINT_SCN' THEN 'Last checkpoint SCN'
    END AS description
  FROM all_capture
  UNPIVOT (
    scn_value FOR scn_type IN (
      first_scn, start_scn, captured_scn, last_enqueued_scn,
      applied_scn, required_checkpoint_scn, max_checkpoint_scn
    )
  )
  UNION ALL
  SELECT 'LOGMINER', scn_type, scn_value,
    CASE scn_type
      WHEN 'RESET_SCN' THEN 'SCN when session started'
      WHEN 'PROCESSED_SCN' THEN 'Builder mined redo up to this SCN'
      WHEN 'PREPARED_SCN' THEN 'Preparers transformed redo to LCRs below this SCN'
      WHEN 'READ_SCN' THEN 'Reader read all redo below this SCN'
      WHEN 'LOW_MARK_SCN' THEN 'All committed txns below this SCN delivered'
      WHEN 'CONSUMED_SCN' THEN 'Client consumed all txns below this SCN'
      WHEN 'SPILL_SCN' THEN 'On restart, redo below this SCN skipped'
    END AS description
  FROM V$LOGMNR_SESSION
  UNPIVOT (
    scn_value FOR scn_type IN (
      RESET_SCN, PROCESSED_SCN, PREPARED_SCN, READ_SCN,
      LOW_MARK_SCN, CONSUMED_SCN, SPILL_SCN
    )
  )
  UNION ALL
  SELECT 'DB', 'CURRENT_SCN', CURRENT_SCN, 'Current system change number'
  FROM V$DATABASE
)
SELECT source, scn_type, scn_value,
       scn_value - LAG(scn_value) OVER (ORDER BY scn_value) AS diff,
       description
FROM scn_values
ORDER BY scn_value, scn_type;
```

### Restart Table Replication

If a table enters `FAILED` state:

1. **Remove the table** from Ingestion Parameters (Included Table Names or adjust regex).
2. **Wait** until the table's state is fully removed from the Table State Store. **Do not proceed until complete.**
3. **DROP the destination table** in Snowflake. The connector will not overwrite an existing table during snapshot.
4. **Optionally** remove the journal table and stream.
5. **Re-add the table** to Ingestion Parameters.
6. **Verify** the table appears with status `NEW` → `SNAPSHOT_REPLICATION` → `INCREMENTAL_REPLICATION`.

---

## Incremental Replication Without Snapshots

For reinstalling the connector over previously replicated data, you can skip the snapshot phase.

**On a new connector:** Set `Ingestion Type` to `incremental` in Oracle Ingestion Parameters before adding tables.

**On an existing connector:** Change `Ingestion Type` from `full` to `incremental`, then add new tables. Existing in-progress tables are not affected.

**Important:**
- Return `Ingestion Type` to `full` once incremental-only needs are satisfied, to ensure future tables get full snapshots.
- In incremental mode, the connector creates the destination table via `CREATE TABLE IF NOT EXISTS` only if no destination table exists.

---

## Next Step

After deployment and configuration, return to `references/connector-main.md` or the calling workflow.

## See Also

- [Set up the Openflow Connector for Oracle](https://docs.snowflake.com/en/user-guide/data-integration/openflow/connectors/oracle/about) — Official Snowflake documentation
- `references/connector-main.md` — Connector workflow overview
- `references/ops-parameters-main.md` — Parameter configuration
- `references/ops-snowflake-auth.md` — Snowflake destination authentication
- `references/platform-eai.md` — Network access for database connectivity
- `references/ops-component-state.md` — Inspect and clear table replication state
- `references/ops-flow-lifecycle.md` — Start, stop, monitor
- `references/ops-config-verification.md` — Configuration verification (single-component and batch)
