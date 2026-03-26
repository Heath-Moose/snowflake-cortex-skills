---
name: trust-center
description: "Use for ALL Snowflake Trust Center requests: security findings, scanner analysis, scanner management, finding remediation, severity distribution, CIS benchmarks, Security Essentials, Threat Intelligence, enable/disable scanners, scanner schedules, notifications, at-risk entities, security posture, vulnerability analysis, detection analysis, remediation guidance."
---

# Trust Center

Helps users analyze, manage, and remediate security findings from Snowflake Trust Center.

## When to Use

- User asks about Trust Center findings, scanners, or security posture
- User wants to enable/disable scanners or change schedules/notifications
- User wants to fix or remediate a specific security finding
- User asks about CIS Benchmarks, Security Essentials, or Threat Intelligence

## Workflow

```
Start
  ↓
Intent Detection
  ├─→ ANALYZE FINDINGS  → Load findings-analysis/SKILL.md
  ├─→ ANALYZE SCANNERS  → Load scanner-analysis/SKILL.md
  ├─→ MANAGE SCANNERS   → Load api-management/SKILL.md
  └─→ REMEDIATE FINDING → Load finding-remediation/SKILL.md
```

### Step 1: Detect Intent

| User Intent | Keywords | Route |
|-------------|----------|-------|
| Analyze findings | findings, severity, new findings, resolved, trend, security posture, categories | [findings-analysis/SKILL.md](findings-analysis/SKILL.md) |
| Analyze scanners | scanners, scanner packages, coverage, disabled scanners, CIS, what scanners | [scanner-analysis/SKILL.md](scanner-analysis/SKILL.md) |
| Manage scanners | enable, disable, schedule, notification, run scanner, execute | [api-management/SKILL.md](api-management/SKILL.md) |
| Remediate findings | fix, remediate, at-risk entities, suggested action, how to fix | [finding-remediation/SKILL.md](finding-remediation/SKILL.md) |

If intent is unclear, ask:
```
What would you like to do with Trust Center?
1. Analyze findings (severity, trends, categories)
2. Review scanners (inventory, coverage, health)
3. Manage scanners (enable/disable, schedules, notifications)
4. Remediate a finding (fix a specific security issue)
```

### Step 2: Load Sub-Skill and Execute

Route to the appropriate sub-skill based on detected intent. Each sub-skill contains the full workflow for its domain.

## Stopping Points

- ✋ Step 1: If user intent is unclear, ask for clarification

## Output

This skill routes to sub-skills, each of which produces its own output:

| Sub-Skill | Output |
|-----------|--------|
| findings-analysis | Findings counts, severity distribution, trends, categories |
| scanner-analysis | Scanner inventory, coverage gaps, health checks |
| api-management | Confirmation of scanner configuration changes |
| finding-remediation | Remediation steps, SQL, verification |
