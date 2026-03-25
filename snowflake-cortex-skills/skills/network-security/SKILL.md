---
name: network-security
description: "Manage Snowflake network policies and network rules. Use when: listing network policies or rules, creating or modifying network rules and policies, understanding hybrid policies with SaaS rules, checking SaaS IP coverage, assigning or unassigning policies to users or accounts. Triggers: network policy, network rule, list network policies, show network rules, create network policy, create network rule, hybrid policy, SaaS rules, IP allowlist, network access, assign policy, unassign policy."
---

# Network Security

Foundational knowledge for managing Snowflake network rules and policies.

## Core Concepts

- **Network rules** define lists of IP addresses (IPV4, INGRESS/EGRESS). They live in a database and schema.
- **Network policies** reference one or more network rules to allow or block traffic. Policies are account-level objects (no database/schema).
- **Hybrid policies** combine custom network rules with Snowflake-managed SaaS rules. This is the recommended pattern because SaaS rules are automatically updated by Snowflake when providers change their IP ranges.
- **Snowflake SaaS rules** are pre-built network rules in `SNOWFLAKE.NETWORK_SECURITY` for common integrations (dbt, Tableau, Power BI, Qlik, GitHub Actions, Sigma, ThoughtSpot, etc.).

### Internal vs External IPs

- **Internal IPs**: `10.x.x.x`, `172.16-31.x.x`, `192.168.x.x`, `0.0.0.0` (Snowflake infrastructure/VPN). These won't match SaaS rules — this is expected. Include them in custom rules.
- **External IPs**: All other public IPs. These may be covered by Snowflake SaaS rules.

### Creation Order

**Network rules MUST be created BEFORE the network policy that references them.** The policy creation will fail if a referenced network rule does not exist.

---

## List Workflow

View existing network policies and network rules in the account.

### Step 1: Choose What to List

**Ask user:**
```
What would you like to list?
1. Network policies
2. Network rules
3. Both
```

### Step 2: Execute List Commands

**List network policies:**
```sql
SHOW NETWORK POLICIES IN ACCOUNT;
```

**List network rules (requires database context):**
```sql
SHOW NETWORK RULES IN ACCOUNT;
```

**Show details of a specific policy:**
```sql
DESCRIBE NETWORK POLICY <policy_name>;
```

**Show details of a specific rule:**
```sql
DESCRIBE NETWORK RULE <database>.<schema>.<rule_name>;
```

### Step 3: Present Results

Display the results in a table format showing:
- Policy/Rule name
- Created date
- Comment
- Number of allowed/blocked entries

---

## SaaS Coverage Check

Use this query to determine which IPs are covered by Snowflake's pre-built SaaS network rules.

```sql
WITH input_ips AS (
    -- Replace with the IPs to check
    SELECT column1 as ip FROM VALUES
        ('<ip1>'), ('<ip2>'), ('<ip3>')
),
snowflake_saas_rules AS (
    SELECT name, value_list
    FROM snowflake.account_usage.network_rules 
    WHERE database = 'SNOWFLAKE' AND schema = 'NETWORK_SECURITY'
    AND deleted IS NULL
),
flattened_cidrs AS (
    SELECT 
        name as rule_name,
        TRIM(f.value::STRING) as cidr_block
    FROM snowflake_saas_rules,
    LATERAL FLATTEN(input => SPLIT(value_list, ',')) f
),
ip_to_int AS (
    SELECT 
        ip,
        (SPLIT_PART(ip, '.', 1)::INT * 16777216) + 
        (SPLIT_PART(ip, '.', 2)::INT * 65536) + 
        (SPLIT_PART(ip, '.', 3)::INT * 256) + 
        (SPLIT_PART(ip, '.', 4)::INT) as ip_int
    FROM input_ips
),
cidr_ranges AS (
    SELECT 
        rule_name,
        cidr_block,
        (SPLIT_PART(SPLIT_PART(cidr_block, '/', 1), '.', 1)::INT * 16777216) + 
        (SPLIT_PART(SPLIT_PART(cidr_block, '/', 1), '.', 2)::INT * 65536) + 
        (SPLIT_PART(SPLIT_PART(cidr_block, '/', 1), '.', 3)::INT * 256) + 
        (SPLIT_PART(SPLIT_PART(cidr_block, '/', 1), '.', 4)::INT) as network_int,
        COALESCE(TRY_TO_NUMBER(SPLIT_PART(cidr_block, '/', 2)), 32) as prefix_len
    FROM flattened_cidrs
)
SELECT 
    i.ip as checked_ip,
    c.rule_name as snowflake_saas_rule,
    c.cidr_block as matching_cidr
FROM ip_to_int i
JOIN cidr_ranges c 
    ON i.ip_int >= c.network_int 
    AND i.ip_int <= c.network_int + POW(2, 32 - c.prefix_len)::INT - 1
ORDER BY i.ip, c.rule_name;
```

**Interpreting results:**

| Result | Action |
|--------|--------|
| IPs covered by SaaS rules | Use Snowflake-provided rules in hybrid policy |
| No coverage | IPs go into a custom network rule |
| Mixed (most common) | Create hybrid policy combining SaaS rules + custom rule |

---

## Creating a Hybrid Network Policy

A hybrid policy uses both custom network rules (for environment-specific IPs) and Snowflake-managed SaaS rules (auto-updated).

### Step 1: Gather Database Context

Network rules require a database and schema.

**Ask user:**
```
To create the network rule, I need:
1. **Database name**: Which database should contain the network rule?
2. **Schema name**: Which schema in that database? (e.g., PUBLIC)
```

### Step 2: Create Custom Network Rule

```sql
CREATE OR REPLACE NETWORK RULE <db>.<schema>.<RULE_NAME>
    TYPE = IPV4
    MODE = INGRESS
    VALUE_LIST = (
        -- Internal IPs (Snowflake infrastructure/VPN)
        '<internal_ip1>/32', '<internal_ip2>/32',
        -- External IPs NOT covered by SaaS rules
        '<external_ip1>/32', '<external_ip2>/32'
    );
```

### Step 3: Verify Rule Creation

```sql
SHOW NETWORK RULES LIKE '<RULE_NAME>' IN <db>.<schema>;
```

### Step 4: Create Hybrid Policy

```sql
CREATE OR REPLACE NETWORK POLICY <POLICY_NAME>
    ALLOWED_NETWORK_RULE_LIST = (
        '<db>.<schema>.<RULE_NAME>',
        'SNOWFLAKE.NETWORK_SECURITY.<SAAS_RULE_1>',
        'SNOWFLAKE.NETWORK_SECURITY.<SAAS_RULE_2>'
    )
    COMMENT = 'Hybrid policy: custom IPs + SaaS rules';
```

**Common Error:** If you see `Network rule 'X' does not exist or not authorized`, ensure the network rule was created successfully and the fully qualified name is correct.

---

## Updating Existing Policies

To update a network rule that is already referenced by a policy:

1. **Drop the policy first** (cannot modify rule while attached):
```sql
DROP NETWORK POLICY IF EXISTS <policy_name>;
```

2. **Update or recreate the network rule:**
```sql
CREATE OR REPLACE NETWORK RULE <db>.<schema>.<rule_name>
    TYPE = IPV4
    MODE = INGRESS
    VALUE_LIST = ('<new_ip1>', '<new_ip2>');
```

3. **Recreate the policy:**
```sql
CREATE OR REPLACE NETWORK POLICY <policy_name>
    ALLOWED_NETWORK_RULE_LIST = ('<db>.<schema>.<rule_name>');
```

---

## Policy Assignment

```sql
-- Apply to a specific user
ALTER USER <username> SET NETWORK_POLICY = '<policy_name>';

-- Remove from user
ALTER USER <username> UNSET NETWORK_POLICY;

-- Apply to entire account (ACCOUNTADMIN required)
ALTER ACCOUNT SET NETWORK_POLICY = '<policy_name>';

-- Remove from account
ALTER ACCOUNT UNSET NETWORK_POLICY;
```

**View assignments:**
```sql
-- Check user's network policy
SHOW PARAMETERS LIKE 'NETWORK_POLICY' FOR USER <username>;

-- Check account network policy
SHOW PARAMETERS LIKE 'NETWORK_POLICY' FOR ACCOUNT;

-- List all users with policies
SELECT USER_NAME, NETWORK_POLICY 
FROM SNOWFLAKE.ACCOUNT_USAGE.USERS
WHERE NETWORK_POLICY IS NOT NULL;
```

---

## DDL Reference

**Load** [references/ddl-reference.md](references/ddl-reference.md) for full DDL syntax:
- Network Rules: CREATE, ALTER, DROP
- Network Policies: CREATE, ALTER, DROP
- Apply Policies: ALTER USER/ACCOUNT SET/UNSET
- View Assignments: SHOW PARAMETERS, ACCOUNT_USAGE queries
