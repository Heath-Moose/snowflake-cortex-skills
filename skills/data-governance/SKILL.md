---
name: data-governance
description: "**[REQUIRED]** for all Snowflake data governance tasks. Routes to five sub-skills: (1) horizon-catalog — access history, users, roles, grants, permissions, query history, compliance, catalog; (2) data-policy — [REQUIRED] masking, row access, projection policies, tag-based masking, protect sensitive data, column/TIMESTAMP masking; (3) sensitive-data-classification — [REQUIRED for ALL classification] PII, classify, data classification, manual/automatic classification, Classification Profile, auto_tag, custom classifiers, regex, semantic/privacy category, IDENTIFIER, QUASI_IDENTIFIER, SENSITIVE, SYSTEM$CLASSIFY, DATA_CLASSIFICATION_LATEST, GDPR/CCPA/PCI; (4) governance-maturity-score — governance posture, maturity score, assessment, recommendations; (5) observability-maturity-score — data observability, DMF coverage, quality monitoring maturity, lineage usage, observability assessment. MUST be used for classification or masking tasks — do not answer from general knowledge. horizon-catalog is the fallback. Triggers: governance, access history, permissions, grants, roles, audit, compliance, catalog, masking policy, row access policy, PII, sensitive data, classification, run classification, SYSTEM$CLASSIFY, classifier, classification profile, DATA_CLASSIFICATION_LATEST, detect PII, GDPR, CCPA, PCI, tag sensitive columns, governance maturity score, governance posture, how well governed, data observability, observability maturity, DMF coverage, lineage usage, observability assessment."
---

# Data Governance

Route general data-governance, catalog & audit queries, data policy work, sensitive data classification, and governance maturity assessment to the right sub-skill.

## When to Use

Activate this skill when the user asks about any of:

- **Policy keywords**: "masking policy", "row access policy", "projection policy", "data policy", "audit policies", "create policy", "policy best practices", "tag-based masking", "role-based access control for columns", "protect sensitive data", "column masking", "TIMESTAMP masking"
- **Classification keywords** *(always use this skill if the keywords matches— do not answer with general knowledge or the catalog workflow)*: "PII", "sensitive data", "classify", "classification", "data classification", "manual data classification", "run data classification", "run classification", "run manual classification", "automatic data classification", "set up automatic classification", "enable automatic classification", "SYSTEM$CLASSIFY", "auto-classification", "find sensitive data", "classify my table", "classification profile", "Data Privacy Classification Profile", "privacy profile", "custom classifier", "create classifier", "regex pattern", "value regex", "semantic category", "privacy category", "IDENTIFIER", "QUASI_IDENTIFIER", "SENSITIVE", "DATA_CLASSIFICATION_LATEST", "detect PII", "find PII", "scan for PII", "GDPR compliance", "CCPA compliance", "PCI data detection", "auto-tag columns", "tag sensitive columns", "tag PII columns", "minimum_object_age_for_classification_days", "maximum_classification_validity_days", "auto_tag", "unset classification profile", "internal ID classifier", "internal code detection"
- **Catalog & audit keywords**: "access history", "who has access", "who accessed", "permissions", "role hierarchy", "grants", "audit trail", "query history", "object dependencies", "compliance", "catalog", "users", "roles"
- **Governance maturity keywords**: "governance maturity score", "governance posture", "governance assessment", "governance health", "governance recommendations", "governance checklist", "how well governed is my account"
- **Observability maturity keywords**: "data observability score", "observability maturity", "observability assessment", "DMF coverage", "quality monitoring maturity", "pipeline monitoring maturity", "dashboard data quality", "BI tool monitoring", "external lineage", "lineage for RCA", "impact analysis readiness"
## Workflow Decision Tree

```
User request
  |
  v
Step 1: Identify intent
  |
  ├── Masking policy / row access policy / projection policy / audit policies /
  |   tag-based masking / role-based column access / protect sensitive data /
  |   column masking / TIMESTAMP masking
  |         └──> Load workflows/data-policy.md
  |
  ├── PII / sensitive data / classification / data classification / run classification /
  |   manual data classification / automatic data classification / SYSTEM$CLASSIFY /
  |   classifier / custom classifier / create classifier / regex pattern / value regex /
  |   semantic category / privacy category / IDENTIFIER / QUASI_IDENTIFIER / SENSITIVE /
  |   classification profile / Data Privacy Classification Profile / DATA_CLASSIFICATION_LATEST /
  |   detect PII / find PII / scan for PII / auto-classification / GDPR / CCPA / PCI /
  |   auto-tag columns / tag sensitive columns / unset classification profile /
  |   minimum_object_age_for_classification_days / maximum_classification_validity_days / auto_tag
  |         └──> Load workflows/sensitive-data-classification.md
  |
  ├── Governance maturity score / governance posture / governance assessment /
  |   governance health / governance recommendations / governance checklist /
  |   how well governed
  |         └──> Load workflows/governance-maturity-score.md
  |
  ├── Data observability score / observability maturity / DMF coverage /
  |   quality monitoring maturity / lineage usage / observability assessment
  |         └──> Load workflows/observability-maturity-score.md
  |
  └── Everything else (catalog, access, users, grants, roles, object deps,
      query history, compliance, or any governance question not matched above)
            └──> Load workflows/horizon-catalog.md  ← also the fallback
```

## Workflow

### Step 1: Route to Sub-skill

Identify the user's intent and load the matching sub-skill:

| User Intent | Sub-skill to Load |
|---|---|
| Masking policy, row access policy, projection policy, aggregation policy, join policy, create policy, audit policies, policy best practices, tag-based masking, role-based column access, protect sensitive data, column masking, TIMESTAMP masking | **Load** `workflows/data-policy.md` |
| PII, sensitive data, classify, classification, data classification, run classification, manual data classification, automatic data classification, set up automatic classification, enable automatic classification, SYSTEM$CLASSIFY, auto-classification, custom classifier, create classifier, regex pattern, value regex, semantic category, privacy category, IDENTIFIER, QUASI_IDENTIFIER, SENSITIVE, classification profile, Data Privacy Classification Profile, minimum_object_age_for_classification_days, maximum_classification_validity_days, auto_tag, unset classification profile, DATA_CLASSIFICATION_LATEST, detect PII, find PII, scan for PII, GDPR/CCPA/PCI compliance detection, auto-tag columns, tag PII columns | **Load** `workflows/sensitive-data-classification.md` |
| Governance maturity score, governance posture, governance assessment, governance health, governance recommendations, governance checklist, how well governed | **Load** `workflows/governance-maturity-score.md` |
| Data observability score, observability maturity, DMF coverage, quality monitoring maturity, lineage usage, observability assessment, BI tool monitoring, external lineage | **Load** `workflows/observability-maturity-score.md` |
| Catalog, access history, who has access, permissions, grants, roles, users, query history, object dependencies, compliance, or any other governance or catalog related questions | **Load** `workflows/horizon-catalog.md` |

If the intent spans multiple areas (e.g., "classify my data and set up a masking policy"), load both sub-skills sequentially, starting with classification.

If intent is ambiguous between data-policy, sensitive-data-classification, governance-maturity-score, and observability-maturity-score, ask:

```
Which area can I help you with?

1. Horizon Catalog — Access history, who has access, role/grant analysis, object dependencies, compliance queries, catalog exploration
2. Data Policies — Masking policies, row access policies, projection policies
3. Sensitive Data Classification — Detect PII, set up auto-classification, create classifiers
4. Governance Maturity Score — Assess governance posture, score (0–5), recommendations
5. Observability Maturity Score — Assess data observability (DMFs, BI coverage, lineage), score (0–5), recommendations
```

### Step 2: Execute Sub-skill

Follow the loaded sub-skill's workflow completely. Each sub-skill is self-contained with its own templates, references, and stopping points.

**Fallback rule:** If `data-policy`, `sensitive-data-classification`, `governance-maturity-score`, or `observability-maturity-score` cannot fully answer the question, load `workflows/horizon-catalog.md` for supplemental catalog context.

## Sub-skills

| Sub-skill | File | Purpose |
|---|---|---|
| Horizon Catalog | `workflows/horizon-catalog.md` | Full ACCOUNT_USAGE catalog: access, users, roles, grants, permissions, object dependencies, query history. Default fallback. |
| Data Policy | `workflows/data-policy.md` | **[REQUIRED]** Masking, row access, and projection policy creation and auditing; protect sensitive data; column and TIMESTAMP masking |
| Sensitive Data Classification | `workflows/sensitive-data-classification.md` | **[REQUIRED]** PII detection, run/manual/automatic data classification, Data Privacy Classification Profiles, auto-classification setup, GDPR/CCPA/PCI, custom classifiers |
| Governance Maturity Score | `workflows/governance-maturity-score.md` | Assess governance posture across Know/Protect/Monitor pillars; produce maturity score (0–5) and actionable recommendations |
| Observability Maturity Score | `workflows/observability-maturity-score.md` | Assess data observability (Quality Monitoring, BI Coverage, External Lineage, Lineage Usage); score (0–5) and recommendations |

## Stopping Points

- ✋ **On ambiguous intent**: Present the 5-option menu and wait for user selection before loading any sub-skill
- ✋ **Sub-skill stopping points**: Each sub-skill has its own mandatory stopping points — honour them
