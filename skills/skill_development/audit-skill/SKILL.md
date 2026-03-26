---
name: audit-skill
description: "Audit and lint skills against best practices. Use when: reviewing skills, checking quality, improving skills, linting skill text. Triggers: audit skill, review skill, improve skill, lint skill, proofread skill, skill quality check."
---

# Audit Existing Skill

Review and lint a skill against best practices, and provide improvements.

## Workflow

### Step 1: Load Skill

Ask for skill path or name. Search `.cortex/skills/` or `$SNOWFLAKE_HOME/cortex/skills/` or `$HOME/.snowflake/cortex/skills/` if name provided.

Load and parse: frontmatter, sections, workflow steps, and tools.

If present, also load:
- `references/` files (lint wording and verify referenced files exist)
- `scripts/` directory listing (verify cross-references; do not lint code style)

### Step 2: Audit Checklist

**Frontmatter:**
| Check | Severity |
|-------|----------|
| `name` present, kebab-case | 🔴 |
| `description` with triggers | 🔴 |
| Purpose explained | 🟡 |

**Structure:**
| Check | Severity |
|-------|----------|
| < 500 lines | 🟡 |
| Workflow section | 🔴 |
| Stopping points | 🔴 |
| Output section | 🟡 |

**Workflow:**
| Check | Severity |
|-------|----------|
| Numbered steps | 🟡 |
| ⚠️ checkpoints marked | 🔴 |
| No chaining without approval | 🔴 |
| Clear actions | 🟡 |

**Linting (language + misleading text):**
| Check | Severity |
|-------|----------|
| Typos, doubled words, and spelling mistakes | 🟡 |
| Awkward or ambiguous phrasing that slows the agent down | 🟡 |
| Conflicting instructions between sections | 🔴 |
| Broken markdown (unclosed code fences, malformed tables, bad links) | 🔴 |
| Wrong file paths / cross-references that point to non-existent files | 🔴 |
| Trigger keywords or “When to use” criteria that could cause misrouting | 🔴 |
| Tool/command examples that are likely to fail as written (e.g., relative paths where absolute paths are required) | 🔴 |

**Tools (if applicable):**
| Check | Severity |
|-------|----------|
| All tools documented | 🟡 |
| Usage examples | 🟡 |
| Absolute paths for scripts | 🟡 |

### Step 3: Generate Report

```
# Audit Report: <skill-name>

## Summary
| Category | 🔴 | 🟡 | 🟢 |
|----------|---|---|---|
| Frontmatter | X | X | X |
| Structure | X | X | X |
| Workflow | X | X | X |

## Critical 🔴
1. [Issue] → [Fix]

## Warnings 🟡
1. [Issue] → [Fix]

## Suggestions 🟢
1. [Improvement]
```

**⚠️ STOP**: Present report.

### Step 4: Apply Fixes (Optional)

Ask:
```
1. Fix critical only
2. Fix critical + warnings
3. Fix all
4. Skip
```

For each fix: show change → approve → apply.

## Severity Guide

- 🔴 **Critical**: Skill may not work
- 🟡 **Warning**: Quality issue
- 🟢 **Suggestion**: Enhancement

## Stopping Points

- ✋ Step 1: Confirm skill loaded
- ✋ Step 3: Present report
- ✋ Step 4: Approve each fix

## Output

Audit report with categorized findings and optional fixes.

---

## Appendix: Linting

Use this appendix to decide what counts as a lint issue and how to grade severity.

### Lint Severity

- 🟡 **Lint (quality):** typos, spelling, doubled words, non-native phrasing, awkward or ambiguous wording, redundant or repetitive text.
  - **Why it matters:** slows the agent down, reduces confidence, increases the odds of misinterpretation.

- 🔴 **Lint (misleading / backtracking risk):** anything that is likely to send the agent down the wrong path or waste time.
  - Conflicting or self-contradictory instructions (e.g., Step 2 says “always X”, Step 4 says “never X”)
  - Broken text from merges/edits (sentence fragments, truncated bullets)
  - Broken markdown that changes meaning (unclosed code fences, malformed tables)
  - Incorrect file paths, missing targets, broken cross-references
  - Trigger keywords that overlap too much or don’t match the actual skill scope
  - Tool examples that won’t work as written (missing required flags, wrong env var names, relative paths where absolute paths are required)

### Lint Checklist (detailed)

1. **Typos and spelling**
   - Misspelled words (including technical terms and proper nouns)
   - Wrong word (e.g., “compliment” vs. “complement”)
   - Doubled words (“the the”, “is is”)

2. **Repetition**
   - Near-duplicate instructions repeated across sections
   - Copy/paste leftovers

3. **Broken text / merge artifacts**
   - Sentence fragments; abrupt starts/ends
   - Unclosed backticks / code fences
   - Tables with the wrong number of columns
   - YAML frontmatter syntax errors

4. **Inconsistencies**
   - Terminology shifts without defining the canonical term
   - Placeholder style inconsistency (`{{var}}` vs `{var}` vs `<var>`)
   - Inconsistent stopping point markers

5. **Cross-references**
   - References to other skills use the wrong name/format (mis-typed kebab-case)
   - Mentioned files/directories do not exist
   - Internal step references (“Step 3”, “step 8d”, etc.) don’t match the current document
