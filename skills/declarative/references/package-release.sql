-- ============================================
-- PACKAGE CREATION & RELEASE
-- ============================================
-- After creating all objects and writing the manifest, create and release the package.

-- ============================================
-- COMMON MISTAKES — DO NOT DO THESE
-- ============================================
-- ALTER APPLICATION PACKAGE <PKG> ADD LIVE VERSION    -- WRONG: LIVE version is auto-created, NEVER run this
-- ALTER APPLICATION PACKAGE <PKG> BUILD               -- WRONG: BUILD is for versioned CI/CD, NOT for LIVE version declarative sharing
-- CREATE OR REPLACE APPLICATION PACKAGE ...           -- WRONG: no OR REPLACE for APPLICATION PACKAGES
-- CREATE OR REPLACE APPLICATION ...                   -- WRONG: no OR REPLACE for APPLICATIONS (use DROP + CREATE)
-- CREATE TEMPORARY STAGE ... (inside an app package)  -- WRONG: temp stages cannot be created inside application packages
--
-- For declarative sharing with LIVE version: use RELEASE LIVE VERSION, NOT BUILD.

-- ============================================
-- CORRECT SEQUENCE
-- ============================================

-- 1. Create package (TYPE=DATA required) — LIVE version is auto-created
CREATE APPLICATION PACKAGE <PACKAGE_NAME> TYPE = DATA;

-- 2. Upload manifest via PUT (works as a SQL statement — no CLI needed)
--    Use absolute path for file:// (e.g., file:///workspace/manifest.yml)
--    ALWAYS name the file manifest.yml, NOT manifest-example.yml
PUT file://<PATH>/manifest.yml
    snow://package/<PACKAGE_NAME>/versions/LIVE/
    OVERWRITE=TRUE AUTO_COMPRESS=false;

-- 3. Upload notebooks (optional)
PUT file://<PATH>/NOTEBOOK.ipynb
    snow://package/<PACKAGE_NAME>/versions/LIVE/
    OVERWRITE=TRUE AUTO_COMPRESS=false;

-- 4. Release (makes available to consumers)
ALTER APPLICATION PACKAGE <PACKAGE_NAME> RELEASE LIVE VERSION;

-- ============================================
-- LISTINGS (Private Sharing)
-- ============================================
-- Do NOT guess this syntax — use exactly as shown.

-- Check org name first:
SELECT CURRENT_ORGANIZATION_NAME();

-- Create private listing for specific consumers
CREATE EXTERNAL LISTING <LISTING_NAME>
APPLICATION PACKAGE <PACKAGE_NAME> AS
$$
title: "Listing Title"
subtitle: "Short description"
description: "Detailed description of what the app does."
listing_terms:
  type: "OFFLINE"
targets:
  accounts: ["<ORG_NAME>.<ACCOUNT_NAME>"]
$$
PUBLISH = FALSE
REVIEW = FALSE;

-- Publish the listing after creation
ALTER LISTING <LISTING_NAME> PUBLISH;

-- View all listings
SHOW LISTINGS;

-- For more extensive listing scenarios (cross-region, paid listings, trial listings, etc.),
-- do NOT guess the syntax. Run:
--   cortex search docs "CREATE EXTERNAL LISTING Snowflake"
--   cortex search docs "CREATE ORGANIZATION LISTING Snowflake"

-- ============================================
-- CONSUMER INSTALL WORKFLOW
-- ============================================
-- IMPORTANT: There is NO "CREATE OR REPLACE APPLICATION" syntax!
-- To reinstall, you must DROP first, then CREATE.

-- PREREQUISITES (check BEFORE installing)

-- 1. User profile must be configured (required for listing installs):
ALTER USER <USERNAME> SET
    first_name = '<FIRST_NAME>',
    last_name = '<LAST_NAME>',
    email = '<EMAIL>';
-- Error if missing: "090655 (P0002): Please add your first/last name and email..."

-- 2. Default warehouse MUST exist for tools to work (UDFs, procedures, agents, search):
SHOW PARAMETERS LIKE 'WAREHOUSE' IN USER;
-- If empty or NULL, tools will FAIL silently or error out!

-- Create warehouse if needed:
CREATE WAREHOUSE IF NOT EXISTS <WH_NAME>
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE;

-- Set as default for current user:
ALTER USER <USERNAME> SET DEFAULT_WAREHOUSE = '<WH_NAME>';

-- INSTALL

-- Install from an application package (same account)
CREATE APPLICATION <APP_NAME> FROM APPLICATION PACKAGE <PACKAGE_NAME>;

-- Install from a listing (cross-account)
CREATE APPLICATION <APP_NAME> FROM LISTING '<LISTING_ID>';

-- To reinstall / replace an existing app:
DROP APPLICATION IF EXISTS <APP_NAME>;
CREATE APPLICATION <APP_NAME> FROM APPLICATION PACKAGE <PACKAGE_NAME>;

-- POST-INSTALL VERIFICATION

-- Check schemas are accessible:
SHOW SCHEMAS IN APPLICATION <APP_NAME>;

-- Check tables/views:
SHOW TABLES IN <APP_NAME>.<SCHEMA_NAME>;
SHOW VIEWS IN <APP_NAME>.<SCHEMA_NAME>;

-- Check functions/procedures:
SHOW USER FUNCTIONS IN <APP_NAME>.<SCHEMA_NAME>;
SHOW PROCEDURES IN <APP_NAME>.<SCHEMA_NAME>;

-- Check Cortex Search services:
SHOW CORTEX SEARCH SERVICES IN <APP_NAME>.<SCHEMA_NAME>;

-- Test a UDF (requires warehouse):
SELECT <APP_NAME>.<SCHEMA>.<FUNCTION_NAME>('<PARAM>');

-- Test a procedure (requires warehouse):
CALL <APP_NAME>.<SCHEMA>.<PROCEDURE_NAME>(<PARAMS>);

