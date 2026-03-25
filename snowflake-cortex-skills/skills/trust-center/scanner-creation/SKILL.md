---
name: trust-center-scanner-creation
description: "Creates custom Trust Center scanner packages and scanners within the Snowflake account. Use when users ask about: creating a custom scanner package, custom scanner, security monitoring specific needs for an account"
---

# Trust Center Scanner Creation

Helps users create and manage custom Trust Center scanner and scanner packages

## Background

Trust Center scanners are the smallest unit of Snowflake's detection/vulnerability ecosystem. Each scanner checks a specific area or configuration in the customer's Snowflake account for security issues. This skill will assist in the customer journey of creating their own account specific security monitoring.

**Scanner types:** Scanners produce two types of findings:
- **Vulnerability** - Persistent configuration issues (e.g., CIS Benchmark checks). Remediation is a specific config change.
- **Detection** - Event-based threat detections (e.g., unusual logins, admin privilege grants). Remediation requires investigation.

**Cost model:** Custom Packages/scanners only incur credit costs when they are enabled and actively running. Simply having them installed (disabled) is free.

Custom scanners can be created within the account via a series of proper USAGE grants and Trust Center API calls. This allows users to expand the default offering of the Trust Center security monitoring product to their specific monitoring needs. These custom scanners exist at the account level. They cannot be transferred to another account natively, via an organization account, or via the Snowflake marketplace.

## When to Use

- User asks about creating their own security monitoring solution
- User asks about why a certain Trust Center scanner does not cover their problem area
- User wants a routine check over data in their account

## When NOT to Use

- User asks about specific findings from scanners (use findings-analysis skill)
- User wants to enable/disable scanners or change schedules/notifications (use api-management skill)
- User wants to analyze existing scanner coverage in the account


## Data Sources

### Scanners View

All scanners are accessible in `snowflake.trust_center.scanners`.

**Key columns:**

| Column | Type | Description |
|--------|------|-------------|
| `NAME` | varchar | Name of the scanner |
| `ID` | varchar | Unique ID for the scanner (aliased from `scanner_id`) |
| `SHORT_DESCRIPTION` | varchar | Scanner short description |
| `DESCRIPTION` | varchar | Full description of the scanner |
| `SCANNER_PACKAGE_ID` | varchar | Unique ID for the scanner package |
| `STATE` | varchar | `TRUE` = enabled, `FALSE` or NULL = not enabled (case may vary — use `UPPER()` in comparisons) |
| `SCHEDULE` | varchar | Cron schedule (e.g., `USING CRON 7 6 * * 2 America/Los_Angeles`). NULL for event-driven scanners. |
| `NOTIFICATION` | varchar | JSON object with keys: `NOTIFY_ADMINS` (TRUE/FALSE), `SEVERITY_THRESHOLD`, `USERS` (array). Empty `{}` means no notification configured. |
| `LAST_SCAN_TIMESTAMP` | timestamp_ltz | When the scanner last completed a scan. NULL for event-driven scanners or scanners that haven't run yet. |

**Scanner execution types:**
- **Scheduled scanners** - Have a `SCHEDULE` value (cron expression). Run periodically.
- **Event-driven scanners** - Have no `SCHEDULE` (NULL). Trigger on relevant events. These will also have NULL `LAST_SCAN_TIMESTAMP` even when enabled — this is normal.

Additional columns exist for extension/plugin scanners (e.g., `SOURCE_DATABASE_ID`, `SOURCE_DATABASE`, `EXTENSION_ID`, `EXTENSION_NAME`, `SCANNER_PACKAGE_SOURCE_TYPE`, `SCANNER_PACKAGE_SOURCE`). These are relevant for third-party marketplace scanner packages and are typically NULL for built-in scanners.

### Scanner Packages View

Scanner packages are accessible in `snowflake.trust_center.scanner_packages`.

**Key columns:**

| Column | Type | Description |
|--------|------|-------------|
| `NAME` | varchar | Name of the scanner package |
| `ID` | varchar | Unique ID for the scanner package (aliased from `scanner_package_id`) |
| `DESCRIPTION` | varchar | Description of the scanner package |
| `DEFAULT_SCHEDULE` | varchar | Default schedule as per the package manifest |
| `STATE` | varchar | `TRUE` = enabled, `FALSE` or NULL = not enabled (case may vary — use `UPPER()` in comparisons) |
| `SCHEDULE` | varchar | Current active schedule. May differ from `DEFAULT_SCHEDULE` if user customized it (CIS Benchmarks and Threat Intelligence are configurable; Security Essentials is not). |
| `NOTIFICATION` | varchar | JSON object with keys: `NOTIFY_ADMINS`, `SEVERITY_THRESHOLD`, `USERS`. Package-level notification config. |
| `LAST_ENABLED_TIMESTAMP` | timestamp_ltz | When the package was last enabled |
| `LAST_DISABLED_TIMESTAMP` | timestamp_ltz | When the package was last disabled |

Additional columns exist for extension/plugin packages (e.g., `PROVIDER`, `SOURCE_DATABASE_ID`, `SOURCE_DATABASE`, `EXTENSION_ID`, `EXTENSION_NAME`, `SCANNER_PACKAGE_SOURCE_TYPE`, `SCANNER_PACKAGE_SOURCE`). These are relevant for third-party marketplace scanner packages.

**Required roles:** `trust_center_admin` or `trust_center_viewer`.

## Workflow

### Step 1: Understand User Intent

Determine what custom scanner the user wants to build. If the user does not mention a specific data source, feel free to search the snowflake public documentation of **account_usage** views

- Note if a customer notes snowflake entities such as Users, Tables, Policies, Native Apps, Usage Data, etc.

### Step 2: Create The Custom Scanner Scan Query

The scan SQL query is the runtime logic that will determine if the account is proper security posture or not.

The query must have this format to be compatible with the Trust Center product platform:

SELECT
    '<RISK_ID>' AS risk_id,
    '<RISK_NAME>' AS risk_name,
    COUNT(*) AS total_at_risk_count,
    '<One of [Vulnerability, Detection]>' AS scanner_type,
    '<General Description of Event>' AS risk_description,
    '<What the security admin should do about finding>' AS suggested_action,
    '<What a finding might mean in this context>' AS impact,
    '<One of [LOW, MEDIUM, HIGH, CRITICAL]>' AS severity,
    ARRAY_AGG(OBJECT_CONSTRUCT('entity_id', <id of the data entity>, 'entity_name', <name of the data entity>, 'entity_object_type', '<Type of data entity>')) AS at_risk_entities,
    OBJECT_CONSTRUCT() AS metadata
FROM <DATA_SOURCE>;

**Description of each field:**

| Field | Description |
|-------|-------------|
| `risk_id` | A unique identifier string for the risk this scanner detects (e.g., `'CUSTOM_MFA_CHECK'`). Must be unique across all scanners in the package. Used internally to track and deduplicate findings. |
| `risk_name` | A short, human-readable name for the risk (e.g., `'Users Without MFA Enabled'`). This is the display name shown in the Trust Center UI. |
| `total_at_risk_count` | The total number of entities affected by this finding. Typically computed as `COUNT(*)` from the data source query. A count of `0` means no risk was found. |
| `scanner_type` | Either `'Vulnerability'` or `'Detection'`. Use **Vulnerability** for persistent configuration issues that require a specific config change to remediate (e.g., missing network policies). Use **Detection** for event-based threat detections that require investigation (e.g., suspicious login activity). |
| `risk_description` | A general description of what this risk represents (e.g., `'Users in the account that do not have multi-factor authentication enabled'`). Provides context for anyone reviewing the finding. |
| `suggested_action` | Recommended remediation steps the security admin should take when this finding is reported (e.g., `'Enable MFA for all identified users using ALTER USER ... SET MINS_TO_BYPASS_MFA = 0'`). |
| `impact` | Explains the potential security consequences of this finding (e.g., `'Accounts without MFA are vulnerable to credential-based attacks and unauthorized access'`). Helps admins prioritize response. |
| `severity` | One of `'LOW'`, `'MEDIUM'`, `'HIGH'`, or `'CRITICAL'`. Indicates the severity of the risk. Used for prioritization and notification threshold filtering. |
| `at_risk_entities` | An array of objects identifying the specific entities at risk. Each object must have an `'entity_id'` key (the unique identifier of the data entity) if the id is not available use the `'entity_name`' as the `'entity_id`', an `'entity_name'` key (the human-readable name of the entity, e.g., a username or table name), and an `'entity_object_type'` key (the entity category, e.g., `'USER'`, `'TABLE'`, `'ROLE'`). Built using `ARRAY_AGG(OBJECT_CONSTRUCT('entity_id', <id>, 'entity_name', <name>, 'entity_object_type', '<TYPE>'))`. |
| `metadata` | An optional object for any additional key-value pairs providing extra context about the finding (e.g., `OBJECT_CONSTRUCT('source_query', 'login_history', 'check_date', CURRENT_DATE())`). Use `OBJECT_CONSTRUCT()` for an empty object if no extra metadata is needed. |

### Step 3: Test the Custom Scanner Scan Query

Before attempting registration, it is important to make sure that the inner scan query is properly formatted and will not cause registration errors.

Run the generated SQL from Step 2 to verify that it is correct SQL syntax.

Return the result of the generated SQL to the user

TODO: If Step 3, fails stop and report that to the customer


## Step 4: Create the prerequisite data objects needed

Check if the TRUST_CENTER_COCO_SCANNERS database exists using this query. Create the database if it doesn't exist. It is very important that the database is not recreated.

```SQL
SHOW DATABASES like 'TRUST_CENTER_COCO_SCANNERS';
```

Generate a short identifier name that makes sense for the name of the scanner and create a schema under for it. If the schema already exists, do not replace it. Generate another identifier and do the same process again.

```SQL
SHOW SCHEMAS like '<GENERATED_IDENTIFIER>' in TRUST_CENTER_COCO_SCANNERS;
```

Create the custom scanner scan() procedure inside of the TRUST_CENTER_COCO_SCANNERS.<GENERATED_IDENTIFIER> schema. This procedure is a wrapper around the earlier generated scane query


```SQL
CREATE IF NOT EXISTS PROCEDURE TRUST_CENTER_COCO_SCANNERS.<GENERATED_IDENTIFIER>.scan(run_id varchar)
RETURNS TABLE(
    risk_id VARCHAR,
    risk_name VARCHAR,
    total_at_risk_count NUMBER,
    scanner_type VARCHAR,
    risk_description VARCHAR,
    suggested_action VARCHAR,
    impact VARCHAR,
    severity VARCHAR,           -- 'CRITICAL', 'HIGH', 'MEDIUM', 'LOW'
    at_risk_entities ARRAY,
    metadata OBJECT
)
LANGUAGE SQL
AS
$$
Declare res resultset;
BEGIN
    res :=(
        <QUERY FROM STEP 3>
    );
    RETURN TABLE(res);
END;
$$
```

## Step 5: Grant Proper Usage on the Data Objects To The Snowflake Application

The Trust Center product runs from within the Snowflake Application. Data Objects created in Step 4 need their usage granted to the Snowflake Application for Trust Center to use them effectively. Without doing this, the custom scanner will fail to run each time for privilege/access errors.

```sql
GRANT USAGE ON DATABASE TRUST_CENTER_COCO_SCANNERS TO APPLICATION SNOWFLAKE;
GRANT USAGE ON SCHEMA TRUST_CENTER_COCO_SCANNERS.<GENERATED_IDENTIFIER> TO APPLICATION SNOWFLAKE;
GRANT USAGE ON PROCEDURE TRUST_CENTER_COCO_SCANNERS.<GENERATED_IDENTIFIER>.scan(VARCHAR) TO APPLICATION SNOWFLAKE;
```

## Step 6: Create the Custom Scanner Package if it doesn't exist

By default, all custom scanners created via CoCo should be registered to the COCO_GENERATED_SCANNERS package. If a custom requests a different name for the scanner package that is okay.

Run the following to check if the scanner package already exists

```SQL
SELECT name
FROM snowflake.trust_center.scanner_packages;
```

If it does exist, proceed to Step 7. If it does not exist, the package needs to be created via this API

```SQL
CALL snowflake.trust_center.register_custom_scanner_package('{
  "manifest_version": "1.0",
  "name": "Cortex Code (CoCo) Generated Scanners",
  "id": "COCO_GENERATED_SCANNERS",
  "description": "A series of additional monitoring scanners that have been generated using Cortex Code (CoCo)."
}');
```
The name, id, and description fields above can be updated if the user specifically asks for them.

## Step 7: Enable the Custom Scanner Package

The custom scanner package needs to be enabled for the scanner to be useable once installed and registered. Make sure that the customer is aware that this may incur serverless compute cost in accordance with the Trust Center cost model. This is necessary to allow the new custom scanner to be used once registered

```SQL
SELECT state
FROM snowflake.trust_center.scanner_packages
WHERE id = '<PACKAGE_ID>';
```

If the query returns no rows, stop executing immediately and use the scanner-analysis skill to investigate the current state of Trust Center. If the query returns True, move onto Step 8. If the query returns False, run the query below.

```SQL
CALL snowflake.trust_center.set_configuration('ENABLED', 'True', 'COCO_GENERATED_SCANNERS');
```

## Step 8: Register the Custom Scanner Created Earlier

Now that the proper database, schema, scan() method, data object grants, and custom scanner package are in place, the custom scanner can be created using the below command.

```SQL
CALL snowflake.trust_center.add_custom_scanner(
  '<SCANNER_PACKAGE_ID>',
  '{
    "manifest_version": "1.0",
    "id": "<SCANNER_ID>",
    "name": "<SCANNER_DESCRIPTION>",
    "description": "<Scanner Full Description>",
    "short_description": "<Scanner Short Description>",
    "type": "<One of [Vulnerability, Detection]>",
    "database_name": "TRUST_CENTER_COCO_SCANNERS",
    "schema": "<Schema Generated from Step 4>"
  }'
);
```

**Description of each field in the `add_custom_scanner` JSON manifest:**

| Field | Description |
|-------|-------------|
| `manifest_version` | The version of the scanner manifest schema. Always set to `"1.0"` for current Trust Center custom scanners. |
| `id` | A unique identifier for the scanner within the package (e.g., `"CUSTOM_MFA_CHECK"`). Must be unique across all scanners in the same scanner package. Use uppercase alphanumeric characters and underscores. |
| `name` | A human-readable display name for the scanner shown in the Trust Center UI (e.g., `"Users Without MFA Enabled"`). |
| `description` | A detailed description of what the scanner checks and why it matters (e.g., `"Scans all user accounts to identify those without multi-factor authentication enabled, which increases risk of credential-based attacks"`). Displayed when a user views scanner details. |
| `short_description` | A brief one-line summary of the scanner's purpose (e.g., `"Checks for users without MFA"`). Used in scanner list views and summary tables. |
| `type` | The scanner finding type — either `"Vulnerability"` for persistent configuration issues requiring a specific remediation, or `"Detection"` for event-based threat detections requiring investigation. Must match the `scanner_type` value used in the scan query from Step 2. |
| `database_name` | The database where the scanner's `scan()` procedure is stored. For CoCo-generated scanners, this is always `"TRUST_CENTER_COCO_SCANNERS"` (created in Step 4). |
| `schema` | The schema within `database_name` that contains the `scan()` procedure. Use the `<GENERATED_IDENTIFIER>` schema created in Step 4. |

The first argument `<SCANNER_PACKAGE_ID>` is passed separately (not part of the JSON) and specifies which scanner package the new scanner should be registered under (e.g., `'COCO_GENERATED_SCANNERS'` from Step 6).

## Step 9: Generate a Summary Of What The Skill Has Done

Present the user with a summary of everything that was created during this workflow. The summary should include:

1. **Scanner overview** — The scanner name, ID, type (Vulnerability/Detection), and a brief description of what it monitors.
2. **Objects created** — List all database objects that were created:
   - Database: `TRUST_CENTER_COCO_SCANNERS` (note if it already existed)
   - Schema: `TRUST_CENTER_COCO_SCANNERS.<GENERATED_IDENTIFIER>`
   - Procedure: `TRUST_CENTER_COCO_SCANNERS.<GENERATED_IDENTIFIER>.scan(VARCHAR)`
3. **Scanner package** — The package the scanner was registered under (name and ID), and whether the package was newly created or already existed.
4. **Grants applied** — Confirm that `USAGE` was granted on the database, schema, and procedure to `APPLICATION SNOWFLAKE`.
5. **How to view the scanner** — Provide the query to verify the scanner is registered:
   ```sql
   SELECT name, id, short_description, state
   FROM snowflake.trust_center.scanners
   WHERE id = '<SCANNER_ID>';
   ```
6. **How to read the scan procedure** — Provide the command to inspect the scan logic:
   ```sql
   DESCRIBE PROCEDURE TRUST_CENTER_COCO_SCANNERS.<GENERATED_IDENTIFIER>.scan(VARCHAR);
   ```
7. **Next steps** — Remind the user that:
   - The scanner will run according to the package schedule (or can be triggered on-demand).
   - Offer to run the scanner for the user
   - They can configure notifications for the scanner via Trust Center.
   - Custom scanners incur serverless compute costs when they run.
