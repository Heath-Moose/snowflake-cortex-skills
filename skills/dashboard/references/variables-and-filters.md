# Variables and Filters

Define global variables for interactive filtering across widgets.

## Variable Types

### Selection - dropdown or text input

| options | creatable | multiple | Result |
|---------|-----------|----------|--------|
| no | (ignored) | no | Single text input |
| no | (ignored) | yes | Multiple text input (tags) |
| yes | no | no | Single-select dropdown |
| yes | no | yes | Multi-select dropdown |
| yes | yes | no | Dropdown + custom input |
| yes | yes | yes | Multi-select + custom values |

Examples:
```json
// Free text input
{ "id": "search", "label": "Search", "type": "selection" }

// Tags input (multiple free text)
{ "id": "tags", "label": "Tags", "type": "selection", "multiple": true }

// Strict dropdown
{ "id": "region", "label": "Region", "type": "selection", "options": ["North", "South"] }

// Flexible dropdown with custom input
{ "id": "category", "label": "Category", "type": "selection", "options": ["A", "B"], "creatable": true }

// With preset value (always an array)
{ "id": "region", "label": "Region", "type": "selection", "options": ["North", "South"], "value": ["North"] }
```

### Range - numeric/date min/max

```json
{ "id": "revenue_range", "label": "Revenue", "type": "range", "value": { "min": "1000", "max": "50000" } }
```
Value boundaries are always strings. Use `null` for unbounded: `{ "min": null, "max": "50000" }`.

### Nullity - null/non-null filter

```json
{ "id": "has_notes", "label": "Has Notes", "type": "nullity", "value": false }
```
`true` = show only nulls, `false` = exclude nulls. Omit `value` for no filtering.

## Widget Filters

Map widget columns to dashboard variables:
```json
"filters": [
  { "column": "REGION", "variableId": "region" },
  { "column": "REVENUE", "variableId": "revenue_range" }
]
```

- Filters apply client-side with AND logic
- Column names must match data exactly (case-sensitive)
- Values are always strings (including range boundaries)
- Not all widgets need to use all variables
- Variables without a `value` field start with no filtering applied

## Complete Example with Variables

```json
{
  "title": "Sales Dashboard",
  "variables": [
    {
      "id": "region",
      "label": "Region",
      "type": "selection",
      "options": ["North", "South", "East", "West"]
    },
    {
      "id": "categories",
      "label": "Product Categories",
      "type": "selection",
      "multiple": true,
      "options": ["Electronics", "Clothing", "Food"],
      "creatable": true
    },
    {
      "id": "revenue_range",
      "label": "Revenue Range",
      "type": "range"
    },
    {
      "id": "has_notes",
      "label": "Has Notes",
      "type": "nullity"
    }
  ],
  "widgets": {
    "sales-chart": {
      "id": "sales-chart",
      "type": "chart",
      "title": "Sales by Region",
      "source": { "sql": "SELECT * FROM sales" },
      "filters": [
        { "column": "REGION", "variableId": "region" },
        { "column": "CATEGORY", "variableId": "categories" },
        { "column": "REVENUE", "variableId": "revenue_range" }
      ],
      "vega": {
        "mark": "bar",
        "encoding": {
          "x": { "field": "REGION", "type": "nominal" },
          "y": { "field": "REVENUE", "type": "quantitative", "aggregate": "sum" }
        }
      }
    },
    "customer-table": {
      "id": "customer-table",
      "type": "table",
      "title": "Customers",
      "source": { "sql": "SELECT * FROM customers" },
      "filters": [
        { "column": "REGION", "variableId": "region" },
        { "column": "NOTES", "variableId": "has_notes" }
      ],
      "columns": ["customer_name", "region", "total_orders"]
    }
  },
  "layout": {
    "default": [
      { "i": "sales-chart", "x": 0, "y": 0, "w": 12, "h": 4 },
      { "i": "customer-table", "x": 0, "y": 4, "w": 12, "h": 3 }
    ]
  }
}
```
