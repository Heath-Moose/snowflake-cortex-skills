# Data Contract: UI Context Bridge

This document defines the expected schema for context data provided by the UI when invoking the performance analysis skill.

## Overview

In UI mode, the skill receives structured context data via `${queryHistoryListContext}` injected into the prompt. In CLI mode, this context is absent and the skill fetches data from ACCOUNT_USAGE views instead.

## Detection

The skill detects UI mode when structured query/warehouse data is present at the start of the prompt. The format is controlled by UI TypeScript code.

**Important:** Adding new fields requires a UI code update and release cycle. The skill must work with whatever fields are currently available.

## Expected Fields

The context data may include some or all of the following fields per query:

| Field | Type | Description |
|---|---|---|
| `query_id` | string | Unique query identifier |
| `query_text` | string | SQL text (may be truncated) |
| `query_type` | string | Type of SQL statement (SELECT, INSERT, etc.) |
| `user_name` | string | User who executed the query |
| `warehouse_name` | string | Warehouse used |
| `warehouse_size` | string | Warehouse size |
| `execution_status` | string | SUCCESS, FAILED, etc. |
| `execution_time` | number | Execution time in milliseconds |
| `total_elapsed_time` | number | Total elapsed time in milliseconds |
| `bytes_scanned` | number | Bytes scanned |
| `percentage_scanned_from_cache` | number | Cache hit rate (0.0 to 1.0) |
| `bytes_spilled_to_local_storage` | number | Local spilling in bytes |
| `bytes_spilled_to_remote_storage` | number | Remote spilling in bytes |
| `partitions_scanned` | number | Partitions scanned |
| `partitions_total` | number | Total partitions |
| `start_time` | string | Query start timestamp |
| `query_parameterized_hash` | string | Parameterized query hash |

## Latency Differences

| Environment | Data Source | Latency |
|---|---|---|
| **UI** | Query Profile API response | Near real-time |
| **CLI** | `QUERY_HISTORY` view | ~45 minutes |
| **CLI** | `QUERY_INSIGHTS` view | ~2 hours |
| **CLI** | `TABLE_QUERY_PRUNING_HISTORY` | ~6 hours |
| **CLI** | `GET_QUERY_OPERATOR_STATS` | Real-time (14-day retention) |

## Notes

- The UI may provide additional fields not listed here — the skill should use whatever is available
- The skill should NOT fail if expected fields are missing — gracefully degrade
- Field names and formats match the Snowflake QUERY_HISTORY view conventions
