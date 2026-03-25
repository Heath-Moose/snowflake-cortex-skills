# Complete Dashboard Example

A full `DashboardSpec` with all features: scorecard KPIs, charts, tables, collections, and layout.

```json
{
  "title": "Sales Performance Dashboard",
  "collections": [
    {
      "id": "revenue-growth",
      "type": "highlight",
      "label": "Revenue increased 25% this quarter",
      "widgetIds": ["revenue-kpi", "revenue-chart"]
    }
  ],
  "layout": {
    "default": [
      { "i": "revenue-kpi", "x": 0, "y": 0, "w": 3, "h": 2 },
      { "i": "orders-kpi", "x": 3, "y": 0, "w": 3, "h": 2 },
      { "i": "avg-order-kpi", "x": 6, "y": 0, "w": 3, "h": 2 },
      { "i": "customer-kpi", "x": 9, "y": 0, "w": 3, "h": 2 },
      { "i": "revenue-chart", "x": 0, "y": 2, "w": 8, "h": 4 },
      { "i": "top-products", "x": 8, "y": 2, "w": 4, "h": 4 },
      { "i": "customer-table", "x": 0, "y": 6, "w": 12, "h": 3 }
    ]
  },
  "widgets": {
    "revenue-kpi": {
      "id": "revenue-kpi",
      "type": "scorecard",
      "title": "Revenue",
      "source": {
        "sql": "SELECT TO_VARCHAR(SUM(REVENUE), '$999,999,999') AS VALUE, CONCAT('+', ROUND(PCT_CHANGE, 1), '%') AS DIFF FROM sales_summary"
      },
      "valueColumn": "VALUE",
      "diffColumn": "DIFF",
      "note": "vs Previous Quarter"
    },
    "orders-kpi": {
      "id": "orders-kpi",
      "type": "scorecard",
      "title": "Orders",
      "source": {
        "sql": "SELECT TO_VARCHAR(COUNT(*), '999,999') AS VALUE, CONCAT('+', ROUND(PCT_CHANGE, 1), '%') AS DIFF FROM order_summary"
      },
      "valueColumn": "VALUE",
      "diffColumn": "DIFF",
      "note": "vs Previous Quarter"
    },
    "avg-order-kpi": {
      "id": "avg-order-kpi",
      "type": "scorecard",
      "title": "Avg Order Value",
      "source": {
        "sql": "SELECT TO_VARCHAR(AVG(ORDER_TOTAL), '$999,999') AS VALUE FROM order_summary"
      },
      "valueColumn": "VALUE"
    },
    "customer-kpi": {
      "id": "customer-kpi",
      "type": "scorecard",
      "title": "Active Customers",
      "source": {
        "sql": "SELECT TO_VARCHAR(COUNT(DISTINCT CUSTOMER_ID), '999,999') AS VALUE, CONCAT('+', NEW_CUSTOMERS) AS DIFF FROM customer_summary"
      },
      "valueColumn": "VALUE",
      "diffColumn": "DIFF",
      "note": "new this quarter"
    },
    "revenue-chart": {
      "id": "revenue-chart",
      "type": "chart",
      "title": "Monthly Revenue Trend",
      "source": {
        "sql": "SELECT MONTH, PRODUCT_CATEGORY, SUM(REVENUE) as TOTAL FROM sales GROUP BY 1, 2"
      },
      "vega": {
        "mark": "line",
        "encoding": {
          "x": { "field": "MONTH", "type": "temporal", "title": "Month" },
          "y": { "field": "TOTAL", "type": "quantitative", "title": "Revenue ($)" },
          "color": { "field": "PRODUCT_CATEGORY", "type": "nominal", "title": "Category" }
        },
        "data": { "name": "values" }
      }
    },
    "top-products": {
      "id": "top-products",
      "type": "chart",
      "title": "Top 10 Products by Revenue",
      "source": { "objectId": "analytics.sales.top_products_view" },
      "vega": {
        "mark": "bar",
        "encoding": {
          "x": { "field": "revenue", "type": "quantitative" },
          "y": { "field": "product_name", "type": "nominal", "sort": "-x" }
        }
      }
    },
    "customer-table": {
      "id": "customer-table",
      "type": "table",
      "title": "Top Customers",
      "source": { "sql": "SELECT * FROM top_customers_current_quarter LIMIT 10" },
      "columns": ["customer_name", "total_orders", "total_revenue", "last_order_date", "customer_since"]
    }
  }
}
```
