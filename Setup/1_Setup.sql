/*==============================================================================
  Project: Securities Master Resolution Platform for UK Asset Management
  Phase: 1 - Environment Setup
  Author: Rahul Sahay
  GitHub: https://github.com/rahulsahay123/securities-master-resolution
  Reference: Snowflake ABT-BestBuy Entity Resolution Guide
  Date: January 2026
  
  Description:
  This script sets up the Snowflake environment for entity resolution using
  Cortex AI. It creates the database, schema, warehouse, tables, and Git 
  integration needed for matching securities across Bloomberg, Refinitiv, 
  and FCA data sources.
  
  Prerequisites:
  - Snowflake account with ACCOUNTADMIN role (Account: tob61939)
  - Cortex AI enabled (trial or enterprise)
  - GitHub repository: rahulsahay123/securities-master-resolution
  
  Usage:
  Run all sections sequentially in Snowflake Snowsight interface
  
  Sections:
  1. Cortex AI Verification
  2. Database & Schema Setup
  3. Warehouse Configuration
  4. Source Tables Creation (Bloomberg, Refinitiv, FCA)
  5. Processing Tables Creation (Harmonized, Embeddings, Matches)
  6. Git Integration
  7. Environment Verification
==============================================================================*/

-- =============================================================================
-- SECTION 1: CORTEX AI VERIFICATION
-- =============================================================================
-- Purpose: Verify that Snowflake Cortex AI is available and working
-- Expected: Response should contain "Cortex AI is working"

SELECT SNOWFLAKE.CORTEX.COMPLETE(
    'mistral-large', 
    'Say "Cortex AI is working" if you can read this'
) AS test_response;


-- =============================================================================
-- SECTION 2: DATABASE & SCHEMA SETUP
-- =============================================================================
-- Purpose: Create the main database and schema for entity resolution

-- Create database for securities resolution
CREATE DATABASE IF NOT EXISTS SECURITIES_MASTER;

-- Create schema for our work
CREATE SCHEMA IF NOT EXISTS SECURITIES_MASTER.ENTITY_RESOLUTION;

-- Verify creation
SHOW DATABASES LIKE 'SECURITIES_MASTER';
SHOW SCHEMAS IN DATABASE SECURITIES_MASTER;


-- =============================================================================
-- SECTION 3: WAREHOUSE CONFIGURATION
-- =============================================================================
-- Purpose: Create compute warehouse optimized for AI operations
-- Size: LARGE (suitable for trial, can scale to X-LARGE/2X-LARGE for production)

CREATE WAREHOUSE IF NOT EXISTS SECURITIES_WH
WITH 
    WAREHOUSE_SIZE = 'LARGE'  
    AUTO_SUSPEND = 300        -- Auto-suspend after 5 minutes of inactivity
    AUTO_RESUME = TRUE        -- Auto-resume when query submitted
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Warehouse for securities entity resolution with Cortex AI';

-- Verify warehouse creation
SHOW WAREHOUSES LIKE 'SECURITIES_WH';

-- Set session context to use our new objects
USE WAREHOUSE SECURITIES_WH;
USE DATABASE SECURITIES_MASTER;
USE SCHEMA ENTITY_RESOLUTION;


-- =============================================================================
-- SECTION 4: SOURCE TABLES CREATION
-- =============================================================================
-- Purpose: Create tables to store securities data from three sources
-- Sources: Bloomberg, Refinitiv, FCA

-- Table 1: Bloomberg-style securities data
-- Standard market data provider format
CREATE OR REPLACE TABLE BLOOMBERG_SECURITIES (
    SECURITY_ID VARCHAR(50),
    SECURITY_NAME VARCHAR(500),
    ISIN VARCHAR(12),           -- International Securities Identification Number
    SEDOL VARCHAR(7),           -- Stock Exchange Daily Official List
    TICKER VARCHAR(20),
    ASSET_CLASS VARCHAR(50),
    ISSUER_NAME VARCHAR(500),
    CURRENCY VARCHAR(3),
    SOURCE VARCHAR(50) DEFAULT 'BLOOMBERG'
);

-- Table 2: Refinitiv-style securities data
-- Alternative data provider with different naming conventions
CREATE OR REPLACE TABLE REFINITIV_SECURITIES (
    RIC_CODE VARCHAR(50),            -- Reuters Instrument Code
    INSTRUMENT_NAME VARCHAR(500),
    ISIN_CODE VARCHAR(12),
    SEDOL_CODE VARCHAR(7),
    TICKER_SYMBOL VARCHAR(20),
    INSTRUMENT_TYPE VARCHAR(50),
    ISSUER VARCHAR(500),
    CURRENCY_CODE VARCHAR(3),
    SOURCE VARCHAR(50) DEFAULT 'REFINITIV'
);

-- Table 3: FCA reference data
-- UK Financial Conduct Authority regulatory perspective
CREATE OR REPLACE TABLE FCA_SECURITIES (
    FCA_REF_NUMBER VARCHAR(50),      -- FCA Reference Number
    FUND_NAME VARCHAR(500),
    ISIN VARCHAR(12),
    SEDOL VARCHAR(7),
    FUND_TYPE VARCHAR(50),
    MANAGER_NAME VARCHAR(500),
    CURRENCY VARCHAR(3),
    SOURCE VARCHAR(50) DEFAULT 'FCA'
);

-- Verify source tables created
SHOW TABLES IN SCHEMA SECURITIES_MASTER.ENTITY_RESOLUTION;


-- =============================================================================
-- SECTION 5: PROCESSING TABLES CREATION
-- =============================================================================
-- Purpose: Create tables for entity resolution pipeline stages
-- Pipeline: Harmonize → Embed → Match → Validate

-- Table 4: Harmonized data
-- Purpose: Store cleaned and standardized security data from all sources
CREATE OR REPLACE TABLE HARMONIZED_SECURITIES (
    HARMONIZED_ID VARCHAR(50),          -- Unique identifier for harmonized record
    ORIGINAL_SOURCE VARCHAR(50),        -- Source system (BLOOMBERG, REFINITIV, FCA)
    ORIGINAL_ID VARCHAR(50),            -- Original ID from source
    SECURITY_NAME_CLEAN VARCHAR(500),   -- Cleaned security name
    ISIN VARCHAR(12),
    SEDOL VARCHAR(7),
    TICKER VARCHAR(20),
    ASSET_TYPE VARCHAR(50),             -- Standardized asset type
    ISSUER_CLEAN VARCHAR(500),          -- Cleaned issuer name
    CURRENCY VARCHAR(3)
);

-- Table 5: Embeddings storage
-- Purpose: Store vector embeddings generated by Cortex AI
CREATE OR REPLACE TABLE SECURITY_EMBEDDINGS (
    EMBEDDING_ID VARCHAR(50),           -- Unique identifier for embedding
    HARMONIZED_ID VARCHAR(50),          -- Link to harmonized data
    SECURITY_DESCRIPTION VARCHAR(1000), -- Text description used for embedding
    EMBEDDING_VECTOR VECTOR(FLOAT, 768),-- Cortex embeddings are 768 dimensions
    CREATED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- Table 6: Matched securities
-- Purpose: Store entity resolution results
CREATE OR REPLACE TABLE MATCHED_SECURITIES (
    MATCH_ID VARCHAR(50),               -- Unique identifier for match
    SOURCE_1 VARCHAR(50),               -- First source system
    ID_1 VARCHAR(50),                   -- First security ID
    SOURCE_2 VARCHAR(50),               -- Second source system
    ID_2 VARCHAR(50),                   -- Second security ID
    SIMILARITY_SCORE FLOAT,             -- Cosine similarity (0-1)
    MATCH_METHOD VARCHAR(50),           -- 'VECTOR' or 'AI_VALIDATED'
    MATCH_STATUS VARCHAR(20),           -- 'APPROVED', 'REJECTED', 'PENDING'
    CREATED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- Verify all tables (should be 6 total)
SHOW TABLES IN SCHEMA SECURITIES_MASTER.ENTITY_RESOLUTION;


-- =============================================================================
-- SECTION 6: GIT INTEGRATION
-- =============================================================================
-- Purpose: Connect Snowflake to GitHub repository for version control

-- Create API integration for Git
CREATE OR REPLACE API INTEGRATION git_api_integration
  API_PROVIDER = git_https_api
  API_ALLOWED_PREFIXES = ('https://github.com/')
  ENABLED = TRUE;

-- Create Git repository object linked to GitHub
CREATE OR REPLACE GIT REPOSITORY SECURITIES_MASTER.ENTITY_RESOLUTION.SECURITIES_REPO
  API_INTEGRATION = git_api_integration
  ORIGIN = 'https://github.com/rahulsahay123/securities-master-resolution.git';

-- Verify Git repository connection
SHOW GIT REPOSITORIES IN SCHEMA SECURITIES_MASTER.ENTITY_RESOLUTION;
DESCRIBE GIT REPOSITORY SECURITIES_MASTER.ENTITY_RESOLUTION.SECURITIES_REPO;


-- =============================================================================
-- SECTION 7: ENVIRONMENT VERIFICATION
-- =============================================================================
-- Purpose: Comprehensive verification that all objects were created successfully

-- Simple verification using SHOW commands
SHOW WAREHOUSES LIKE 'SECURITIES_WH';
SHOW DATABASES LIKE 'SECURITIES_MASTER';
SHOW SCHEMAS IN DATABASE SECURITIES_MASTER;
SHOW TABLES IN SCHEMA SECURITIES_MASTER.ENTITY_RESOLUTION;

-- Count total tables (should be 6)
SELECT COUNT(*) AS TOTAL_TABLES 
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA = 'ENTITY_RESOLUTION' 
  AND TABLE_CATALOG = 'SECURITIES_MASTER';

-- List all tables with details
SELECT 
    TABLE_NAME,
    TABLE_TYPE,
    ROW_COUNT,
    CREATED AS CREATED_DATE
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA = 'ENTITY_RESOLUTION' 
  AND TABLE_CATALOG = 'SECURITIES_MASTER'
ORDER BY TABLE_NAME;


-- =============================================================================
-- SETUP COMPLETE
-- =============================================================================
-- Phase 1 Environment Setup: ✅ COMPLETE
--
-- Created Objects:
-- ✅ Database: SECURITIES_MASTER
-- ✅ Schema: ENTITY_RESOLUTION  
-- ✅ Warehouse: SECURITIES_WH (LARGE)
-- ✅ Source Tables: BLOOMBERG_SECURITIES, REFINITIV_SECURITIES, FCA_SECURITIES
-- ✅ Processing Tables: HARMONIZED_SECURITIES, SECURITY_EMBEDDINGS, MATCHED_SECURITIES
-- ✅ Git Integration: Connected to GitHub repository
--
-- Next Steps:
-- → Phase 2: Generate synthetic UK securities data
-- → File: 2_Data_Generation.sql
-- =============================================================================
