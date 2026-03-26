---
name: dashboard
description: "Create, modify, and answer questions about interactive dashboards with charts, tables, and markdown widgets. Use when users ask for: dashboards, KPI reports, executive summaries, multi-chart visualizations, data overviews, metric tracking, modifying dashboard widgets, adding charts, changing layouts, fixing dashboard issues, 'show me everything about...'. Triggers: dashboard, create dashboard, build dashboard, modify dashboard, update dashboard, fix dashboard, add widget, change chart, KPI dashboard, executive report, overview dashboard, sales dashboard, performance dashboard, metrics dashboard, visualizations, dashboard layout, dashboard spec."
---

# Dashboard

Create, modify, and troubleshoot `DashboardSpec` JSON objects for interactive dashboards with charts, tables, and markdown content.

## File Format

Dashboard files use the `.dash` extension with DashboardSpec JSON content.

| Context | Format |
|---------|--------|
| Local files / Snowsight Workspaces | Save as `.dash` file (viewable in Snowsight UI) |
| Snowflake Intelligence / Cortex streaming | Return DashboardSpec JSON object directly |

## When to Use

Use this skill when users ask to:
- Create new dashboards with multiple data points
- Modify existing dashboards (add/remove/update widgets, change layout, update queries)
- Fix or troubleshoot dashboard rendering or data issues
- Answer questions about dashboard structure or capabilities
- Executive-level reporting or KPI tracking
- "Show me everything about..." type queries

## Workflow

### Step 1: Gather Requirements

**Ask** user for:
- What data/topic the dashboard should cover
- Key metrics or KPIs to highlight
- Data sources (SQL queries, table names, or object IDs)
- Any specific chart types or layout preferences

If user provides a vague request, infer reasonable widgets:
- A **markdown widget** for KPI summary
- 1-2 **chart widgets** for trends/comparisons
- A **table widget** for detailed data

### Step 2: Design the Dashboard

Plan the dashboard structure:

1. **Choose widgets** - Each widget needs:
   - A semantic ID (e.g., `revenue-trend`, not `widget-1`)
   - A type: `markdown`, `chart`, `table`, or `scorecard`
   - A data source for chart/table/scorecard widgets

2. **Plan layout** using a 12-column grid:
   - Full width: `w=12`, Half: `w=6`, Third: `w=4`, Quarter: `w=3`
   - Ensure `x + w <= 12` for every row

3. **Define variables** if interactive filtering is needed
   - **Load** [references/variables-and-filters.md](references/variables-and-filters.md) for variable types and filter syntax

### Step 3: Generate DashboardSpec

**Load** [references/complete-example.md](references/complete-example.md) for a full example.

Generate a JSON object following this exact structure:

```json
{
  "title": "Dashboard Title",
  "variables": [
    { "id": "region", "label": "Region", "type": "selection", "options": ["A", "B"] },
    { "id": "status", "label": "Status", "type": "selection", "options": ["X", "Y"], "multiple": true }
  ],
  "widgets": {
    "my-chart": {
      "id": "my-chart",
      "type": "chart",
      "title": "Chart Title",
      "source": { "sql": "SELECT REGION, SUM(AMOUNT) AS TOTAL FROM orders GROUP BY REGION" },
      "filters": [
        { "column": "REGION", "variableId": "region" }
      ],
      "vega": {
        "mark": "bar",
        "encoding": {
          "x": { "field": "REGION", "type": "nominal" },
          "y": { "field": "TOTAL", "type": "quantitative" }
        }
      }
    }
  },
  "layout": {
    "default": [
      { "i": "my-chart", "x": 0, "y": 0, "w": 12, "h": 4 }
    ]
  }
}
```

**Critical format rules:**
- `widgets` is an **object keyed by widget ID**, not a list
- `layout` must have a `"default"` array of tiles
- Widget types are exactly: `"markdown"`, `"chart"`, `"table"`, or `"scorecard"`
- Variable types are exactly: `"selection"`, `"range"`, or `"nullity"` (not "select", "dropdown", etc.)
- Each variable must have an `"id"` field (not "name")
- Widget `filters` use `{"column": "...", "variableId": "..."}` to link to variables
- Multi-select uses `"multiple": true` (not "multiSelect" or "multi_select")
- Selection variable `value` is always an **array** (e.g., `"value": ["North"]`, not `"value": "North"`)

**Omit** `variables`, `collections`, and layout `config` if not needed.

### Step 4: Validate

Before presenting to user, verify:
- All widget IDs in `layout` exist in `widgets`
- All `x + w <= 12`
- All filter `variableId` references exist in `variables`
- Field names in Vega specs match column names exactly (case-sensitive)
- Filter column names match data column names exactly (case-sensitive)

**Present** the complete DashboardSpec JSON to user.

---

## Widget Types

### Markdown Widget

```json
{
  "id": "exec-summary",
  "type": "markdown",
  "title": "Executive Summary",
  "content": "# Q4 2023\n\n## Revenue\n**$2.4M** +15%\n\n## Orders\n**1,234** +23%"
}
```

### Scorecard Widget

For displaying a single highlighted metric with optional diff and note. Query-backed like chart/table — formatting is delegated to SQL.

```json
{
  "id": "total-revenue",
  "type": "scorecard",
  "title": "Total Revenue",
  "source": {
    "sql": "SELECT SUM(REVENUE) AS TOTAL, CONCAT('+', ROUND(PCT_CHANGE, 1), '%') AS CHANGE FROM sales_summary"
  },
  "valueColumn": "TOTAL",
  "diffColumn": "CHANGE",
  "note": "vs Previous Quarter"
}
```

- `valueColumn` (required): Column whose value is displayed as large centered text (first row)
- `diffColumn` (optional): Shown below the main value. Positive values (+, ↑) render green, negative (-, ↓) render red
- `note` (optional): Static text rendered below the diff in muted style
- **Layout tip:** Scorecards work well in small tiles (`w=3` or `w=4`, `h=2`) arranged side-by-side for KPI rows

### Chart Widget

Uses Vega-Lite specs. The `data` property can be omitted or set to `{"name": "values"}`.

```json
{
  "id": "revenue-trend",
  "type": "chart",
  "title": "Monthly Revenue",
  "source": { "sql": "SELECT MONTH, REVENUE FROM sales_data" },
  "vega": {
    "mark": "bar",
    "encoding": {
      "x": { "field": "MONTH", "type": "nominal" },
      "y": { "field": "REVENUE", "type": "quantitative", "aggregate": "sum" }
    }
  }
}
```

**Marks:** `bar`, `line`, `point`, `arc`, `rect`, `area`, `text`
**Encoding types:** `temporal`, `nominal`, `quantitative`, `ordinal`
**Encodings:** `x`, `y`, `color`, `size`, `shape`, `theta` (pie charts), `tooltip`
**Aggregations:** `sum`, `mean`, `count`, `min`, `max`, `median`
**Sort bars descending:** `"sort": "-x"`

### Table Widget

```json
{
  "id": "customer-list",
  "type": "table",
  "title": "Top Customers",
  "source": { "sql": "SELECT * FROM customers LIMIT 10" },
  "columns": ["customer_name", "total_revenue", "last_order_date"]
}
```

If `columns` is omitted, all columns are displayed.

---

## Data Sources

Each chart/table/scorecard widget needs exactly one data source:

| Source | When to Use | Example |
|--------|-------------|---------|
| `sql` | Quick prototypes, ad-hoc queries | `{"sql": "SELECT * FROM sales"}` |
| `objectId` | Production dashboards (better caching) | `{"objectId": "db.schema.table"}` |
| `queryId` | Session-specific data (expires 14d) | `{"queryId": "abc123"}` |

---

## Widget Collections

Group related widgets for visual emphasis using highlights:

```json
"collections": [
  {
    "id": "revenue-growth",
    "type": "highlight",
    "label": "Revenue increased 25% this quarter",
    "widgetIds": ["revenue-chart", "kpi-metrics"]
  }
]
```

- Use descriptive IDs and clear labels
- Order matters: most important first
- Multiple collections get auto-assigned colors
- Use cases: AI insights, guided analysis, alerting, storytelling, recommendations

---

## Layout

```json
// Full width
{ "i": "widget-id", "x": 0, "y": 0, "w": 12, "h": 3 }

// Side by side
[
  { "i": "left", "x": 0, "y": 0, "w": 6, "h": 3 },
  { "i": "right", "x": 6, "y": 0, "w": 6, "h": 3 }
]

// Three columns
[
  { "i": "w1", "x": 0, "y": 0, "w": 4, "h": 3 },
  { "i": "w2", "x": 4, "y": 0, "w": 4, "h": 3 },
  { "i": "w3", "x": 8, "y": 0, "w": 4, "h": 3 }
]
```

**Do NOT include `config` in layout** unless overriding defaults (12 cols, 100px rows, vertical compact).

---

## References (Load On Demand)

| Reference | When to Load |
|-----------|--------------|
| [references/variables-and-filters.md](references/variables-and-filters.md) | When dashboard needs interactive filtering |
| [references/complete-example.md](references/complete-example.md) | For a full end-to-end DashboardSpec example |

---

## Common Pitfalls

1. Missing required fields in widgets (id, type, source for chart/table/scorecard, valueColumn for scorecard)
2. Layout tile references non-existent widget ID
3. Widget position exceeds grid: `x + w > 12`
4. Field name case mismatch between Vega spec and data columns
5. Filter references non-existent variable ID
6. Using single value for selection variable instead of array
7. Including `config` in layout when defaults are sufficient
8. Unescaped quotes in SQL strings

## Stopping Points

- After Step 1 if requirements are unclear
- After Step 3 to present dashboard for user review

**Resume rule:** After approval, apply requested changes without re-asking previous decisions.

## Output

Complete `DashboardSpec` JSON ready for rendering.
