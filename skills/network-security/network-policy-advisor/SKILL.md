---
name: network-policy-advisor
description: "Recommend and evaluate Snowflake network policies. Use when: generating network policy recommendations from access history, evaluating candidate policies before deployment, migrating existing policies to use Snowflake-managed SaaS rules. Triggers: recommend network policy, evaluate network policy, candidate policy, migrate policy, replace IP addresses."
---

# Network Policy Advisor

Advisory workflows for Snowflake network policies using built-in security procedures.

**Load** [../SKILL.md](../SKILL.md) for foundational network security concepts, DDL reference, SaaS coverage checks, and hybrid policy creation patterns.

## Intent Detection

**Ask user:**
```
What would you like to do?
1. Generate network policy recommendations
2. Evaluate a candidate network policy
3. Migrate existing policy to use SaaS rules
```

- **Option 1** → Continue to [Recommend Workflow](#recommend-workflow)
- **Option 2** → Continue to [Evaluate Workflow](#evaluate-workflow)
- **Option 3** → Continue to [Migrate Workflow](#migrate-workflow)

---

## Recommend Workflow

### Step 1: Gather Parameters

**Ask user:**
```
To generate network policy recommendations:

1. **Scope**: User-level or Account-level?
   - User-level: Provide username (e.g., "JOHN_DOE")
   - Account-level: Skip this paramter

2. **Lookback period**: How many days of history? (default: 90)
```

**⚠️ STOP**: Confirm parameters before proceeding.

### Step 2: Execute Recommendation Procedure

**For user-level recommendation:**
```sql
CALL snowflake.network_security.recommend_network_policy('<USERNAME>', <LOOKBACK_DAYS>);
```

**For account-level recommendation:**
```sql
CALL snowflake.network_security.recommend_network_policy(lookback_days => <LOOKBACK_DAYS>);
```

### Step 3: Present Results

1. **ALWAYS display the complete raw output** from the procedure in a code block:
   ```
   <full procedure output here - do not truncate or summarize>
   ```

2. **Identify external IPs** from the recommendation (see [Internal vs External IPs](../SKILL.md#internal-vs-external-ips) in the parent skill).

### Step 4: Automatic SaaS Coverage Check

**ALWAYS automatically check** whether any external IPs are covered by Snowflake SaaS rules. Use the [SaaS Coverage Check](../SKILL.md#saas-coverage-check) query from the parent skill with all external IPs from the recommendation.

### Step 5: Present Hybrid Policy Recommendation

**ALWAYS recommend a hybrid policy by default.** Present the recommendation to the user:

```
Based on the analysis, I recommend creating a **hybrid network policy**:

**SaaS Rules (auto-updated by Snowflake):**
- SNOWFLAKE.NETWORK_SECURITY.<MATCHING_RULE_1>
- SNOWFLAKE.NETWORK_SECURITY.<MATCHING_RULE_2>
- ... (list all matching SaaS rules)

**Custom Rule (for remaining IPs):**
- X internal IPs (Snowflake infrastructure/VPN)
- Y external IPs (not covered by SaaS rules)

This approach ensures:
1. SaaS provider IPs stay automatically updated by Snowflake
2. You only manage custom IPs that are specific to your environment
```

**⚠️ STOP**: Get user approval before creating the policy.

### Step 6: Create Hybrid Network Policy

Follow the [Creating a Hybrid Network Policy](../SKILL.md#creating-a-hybrid-network-policy) pattern from the parent skill to:
1. Gather database/schema context
2. Create the custom network rule
3. Create the hybrid policy referencing both the custom rule and matched SaaS rules

### Step 7: Offer to Evaluate the Policy

After creating the hybrid policy, **always offer to evaluate it**:

```
The hybrid policy has been created. Would you like me to evaluate it against 
the same lookback period to confirm 100% coverage?
```

If user agrees, run:
```sql
CALL snowflake.network_security.evaluate_candidate_network_policy(
    '<USERNAME>_HYBRID_NETWORK_POLICY',
    '<USERNAME>',
    <LOOKBACK_DAYS>
);
```

Present evaluation results showing allowed vs blocked IPs. If any IPs would be blocked, offer to expand the custom rule.

## Stopping Points (Recommend)

- ✋ Step 1: After gathering parameters
- ✋ Step 5: After presenting hybrid policy recommendation (get approval)
- ✋ Step 7: After offering evaluation

## Notes (Recommend)

- The procedure executes with CALLER privileges and accesses sensitive user activity data
- Recommended lookback periods:
  - Quick review: 7-14 days
  - Standard analysis: 30 days
  - Comprehensive audit: 90+ days

---

## Evaluate Workflow

Evaluate a candidate network policy against user activity to simulate the effect if that policy had been applied to either the account level or a specific user.

### Step 1: Gather Parameters

**Ask user:**
```
To evaluate a network policy:

1. **Policy name**: Name of the network policy to evaluate
2. **User scope**: Specific user or all users?
   - Specific user: Provide username (e.g., "JOHN_DOE")
   - All users: Skip this parameter
3. **Lookback period**: How many days of history? (default: 90)
```

**⚠️ STOP**: Confirm parameters before proceeding.

### Step 2: Execute Evaluation Procedure

**IMPORTANT**: Use `CALL` syntax (not `SELECT * FROM TABLE()`).

```sql
CALL snowflake.network_security.evaluate_candidate_network_policy(
    '<POLICY_NAME>',
    '<USERNAME>',  -- or NULL for all users
    <LOOKBACK_DAYS>
);
```

### Step 3: Present Results

1. **ALWAYS display the complete tabular output** from the procedure
2. **Then** provide analysis:
   - Users/IPs that would be blocked
   - Users/IPs that would be allowed
   - Potential access disruptions
   - Compliance summary

**⚠️ STOP**: Review results with user.

### Step 4: Recommendations

Based on results, suggest:
- Policy adjustments if too restrictive
- Additional IP ranges to include/exclude
- Users who may need exceptions

## Stopping Points (Evaluate)

- ✋ Step 1: After gathering parameters
- ✋ Step 3: After presenting evaluation results

## Notes (Evaluate)

- Executes with CALLER privileges - access to sensitive security data
- Use cases:
  - Test policies before deployment to avoid lockouts

---

## Migrate Workflow

Analyze an existing network policy to identify IP addresses that can be replaced with Snowflake-managed SaaS rules (auto-updated).

### Step 1: Select Existing Policy

**List policies:**
```sql
SHOW NETWORK POLICIES IN ACCOUNT;
```

**Ask user:** Which policy would you like to analyze for SaaS migration?

**⚠️ STOP**: Confirm policy selection.

### Step 2: Extract IP Addresses

**Get policy details:**
```sql
DESCRIBE NETWORK POLICY <selected_policy>;
```

Parse the `ALLOWED_IP_LIST` column to extract all IP addresses. If the policy uses `ALLOWED_NETWORK_RULE_LIST`, describe each rule:
```sql
DESCRIBE NETWORK RULE <db>.<schema>.<rule_name>;
```

### Step 3: SaaS Coverage Check

Use the [SaaS Coverage Check](../SKILL.md#saas-coverage-check) query from the parent skill with the extracted IPs.

### Step 4: Present Migration Recommendation

Present results:
```
**SaaS Coverage Analysis for <policy_name>:**

IPs covered by SaaS rules (can be replaced):
- <ip1> -> SNOWFLAKE.NETWORK_SECURITY.<RULE_NAME>
- <ip2> -> SNOWFLAKE.NETWORK_SECURITY.<RULE_NAME>

IPs not covered (keep in custom rule):
- <ip3>, <ip4>, ...

**Recommendation:** Create hybrid policy with:
- SaaS rules: <list matching rules>
- Custom rule: <remaining IPs>
```

**⚠️ STOP**: Get user approval before creating replacement policy.

### Step 5: Create Replacement Policy

Follow the [Creating a Hybrid Network Policy](../SKILL.md#creating-a-hybrid-network-policy) pattern from the parent skill to create:
1. Custom network rule (non-SaaS IPs only)
2. Hybrid network policy (custom rule + SaaS rules)

### Step 6: Evaluate and Swap

1. **Evaluate** new policy using [Evaluate Workflow](#evaluate-workflow)
2. If successful, swap policies (see [Policy Assignment](../SKILL.md#policy-assignment) in the parent skill):
```sql
-- If assigned to user
ALTER USER <username> SET NETWORK_POLICY = '<new_hybrid_policy>';

-- If assigned to account
ALTER ACCOUNT SET NETWORK_POLICY = '<new_hybrid_policy>';

-- Remove old policy
DROP NETWORK POLICY <old_policy>;
```

**⚠️ STOP**: Confirm before swapping policies.

## Stopping Points (Migrate)

- ✋ Step 1: After selecting policy
- ✋ Step 4: After presenting migration recommendation
- ✋ Step 6: Before swapping policies
