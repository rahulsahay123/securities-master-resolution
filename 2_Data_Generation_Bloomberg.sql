/*==============================================================================
  Project: Securities Master Resolution Platform for UK Asset Management
  Phase: 2A - Bloomberg Data Generation
  File: 2_Data_Generation_Bloomberg.sql
  Author: Rahul Sahay
  GitHub: https://github.com/rahulsahay123/securities-master-resolution
  Date: January 2026
  
  Description:
  This script creates a Python stored procedure to generate realistic 
  Bloomberg-style UK securities data with proper UK identifiers (ISINs with 
  GB prefix, SEDOLs, etc.) and intentional variations for entity resolution 
  testing.
  
  Prerequisites:
  - Phase 1 (1_Setup.sql) completed successfully
  - SECURITIES_MASTER database and ENTITY_RESOLUTION schema exist
  - BLOOMBERG_SECURITIES table created
  - SECURITIES_WH warehouse running
  
  Usage:
  Run all sections sequentially in Snowflake Snowsight interface
  
  Sections:
  1. Environment Setup
  2. Create Bloomberg Data Generator Procedure
  3. Test with 5 Records
  4. Generate Full 1000 Records
  5. Data Verification & Quality Checks
==============================================================================*/

-- =============================================================================
-- SECTION 1: ENVIRONMENT SETUP
-- =============================================================================

USE WAREHOUSE SECURITIES_WH;
USE DATABASE SECURITIES_MASTER;
USE SCHEMA ENTITY_RESOLUTION;

-- Verify we're in the correct context
SELECT CURRENT_WAREHOUSE() AS WAREHOUSE,
       CURRENT_DATABASE() AS DATABASE,
       CURRENT_SCHEMA() AS SCHEMA;


-- =============================================================================
-- SECTION 2: CREATE BLOOMBERG DATA GENERATOR PROCEDURE
-- =============================================================================
-- Purpose: Python stored procedure to generate realistic UK securities data

CREATE OR REPLACE PROCEDURE GENERATE_BLOOMBERG_SECURITIES(NUM_RECORDS INT)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'generate_data'
AS
$$
import random
import string
from datetime import datetime

def generate_data(session, num_records):
    """
    Generate realistic Bloomberg-style UK securities data
    """
    
    # UK company names for realistic securities
    uk_companies = [
        'HSBC Holdings', 'BP', 'Shell', 'AstraZeneca', 'Unilever',
        'GlaxoSmithKline', 'British American Tobacco', 'Diageo', 
        'Rio Tinto', 'BHP Group', 'Barclays', 'Lloyds Banking Group',
        'National Grid', 'Vodafone Group', 'Prudential', 'Legal & General',
        'Aviva', 'Standard Chartered', 'Tesco', 'Sainsbury',
        'Marks & Spencer', 'Next', 'Rolls-Royce Holdings', 'BAE Systems',
        'Compass Group', 'InterContinental Hotels', 'Whitbread', 'Burberry',
        'Reckitt Benckiser', 'Associated British Foods', 'Imperial Brands',
        'SSE', 'Centrica', 'Severn Trent', 'United Utilities',
        'Bunzl', 'Smith & Nephew', 'Smiths Group', 'Melrose Industries',
        'Schroders', 'St James Place', 'Hargreaves Lansdown', 'Man Group'
    ]
    
    # Asset classes
    asset_classes = ['Equity', 'Bond', 'Fund', 'ETF', 'Derivative']
    
    # UK currency
    currency = 'GBP'
    
    securities = []
    
    for i in range(num_records):
        # Select random company
        company = random.choice(uk_companies)
        
        # Generate unique ISIN (GB prefix for UK)
        # Format: GB + 10 alphanumeric characters
        isin = 'GB' + ''.join(random.choices(string.digits + string.ascii_uppercase, k=10))
        
        # Generate SEDOL (7 characters)
        # Real SEDOLs use a check digit, we'll simplify
        sedol = ''.join(random.choices(string.digits + string.ascii_uppercase, k=7))
        
        # Generate ticker (3-4 characters)
        ticker = ''.join(random.choices(string.ascii_uppercase, k=random.randint(3, 4)))
        
        # Select asset class
        asset_class = random.choice(asset_classes)
        
        # Build security name with variations
        security_name = f"{company} {asset_class}"
        
        # Add some variations to make matching interesting
        variations = [
            f"{company} {asset_class}",
            f"{company} plc {asset_class}",
            f"{company} PLC {asset_class}",
            f"{company.upper()} {asset_class}",
            f"{company} - {asset_class}"
        ]
        security_name = random.choice(variations)
        
        # Create security ID
        security_id = f"BBG{str(i+1).zfill(6)}"
        
        securities.append({
            'SECURITY_ID': security_id,
            'SECURITY_NAME': security_name,
            'ISIN': isin,
            'SEDOL': sedol,
            'TICKER': ticker,
            'ASSET_CLASS': asset_class,
            'ISSUER_NAME': company,
            'CURRENCY': currency,
            'SOURCE': 'BLOOMBERG'
        })
    
    # Create DataFrame from list of dictionaries
    from snowflake.snowpark import Row
    df = session.create_dataframe([Row(**sec) for sec in securities])
    
    # Write to table
    df.write.mode("overwrite").save_as_table("BLOOMBERG_SECURITIES")
    
    return f"Successfully generated {num_records} Bloomberg securities"
$$;

-- Verify procedure was created
SHOW PROCEDURES LIKE 'GENERATE_BLOOMBERG_SECURITIES';


-- =============================================================================
-- SECTION 3: TEST WITH 5 RECORDS
-- =============================================================================
-- Purpose: Verify the procedure works before generating full dataset

-- Generate 5 test records
CALL GENERATE_BLOOMBERG_SECURITIES(5);

-- Verify test data
SELECT COUNT(*) AS TEST_RECORD_COUNT 
FROM BLOOMBERG_SECURITIES;

-- View test data
SELECT 
    SECURITY_ID,
    SECURITY_NAME,
    ISIN,
    SEDOL,
    TICKER,
    ASSET_CLASS,
    ISSUER_NAME,
    CURRENCY,
    SOURCE
FROM BLOOMBERG_SECURITIES
ORDER BY SECURITY_ID;


-- =============================================================================
-- SECTION 4: GENERATE FULL 1000 RECORDS
-- =============================================================================
-- Purpose: Generate complete Bloomberg dataset

-- Generate 1000 Bloomberg securities
CALL GENERATE_BLOOMBERG_SECURITIES(1000);

-- Confirm record count
SELECT COUNT(*) AS TOTAL_BLOOMBERG_RECORDS 
FROM BLOOMBERG_SECURITIES;


-- =============================================================================
-- SECTION 5: DATA VERIFICATION & QUALITY CHECKS
-- =============================================================================
-- Purpose: Comprehensive data quality validation

-- Check 1: Total record count (should be 1000)
SELECT 'Total Records' AS CHECK_NAME,
       COUNT(*) AS RESULT,
       CASE WHEN COUNT(*) = 1000 THEN 'PASS' ELSE 'FAIL' END AS STATUS
FROM BLOOMBERG_SECURITIES;

-- Check 2: No NULL values in key fields
SELECT 'NULL Check' AS CHECK_NAME,
       COUNT(*) AS NULL_COUNT,
       CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS STATUS
FROM BLOOMBERG_SECURITIES
WHERE SECURITY_ID IS NULL 
   OR SECURITY_NAME IS NULL 
   OR ISIN IS NULL;

-- Check 3: All ISINs start with GB (UK prefix)
SELECT 'ISIN Format Check' AS CHECK_NAME,
       COUNT(*) AS NON_GB_COUNT,
       CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS STATUS
FROM BLOOMBERG_SECURITIES
WHERE LEFT(ISIN, 2) != 'GB';

-- Check 4: All sources are BLOOMBERG
SELECT 'Source Check' AS CHECK_NAME,
       COUNT(DISTINCT SOURCE) AS UNIQUE_SOURCES,
       CASE WHEN COUNT(DISTINCT SOURCE) = 1 THEN 'PASS' ELSE 'FAIL' END AS STATUS
FROM BLOOMBERG_SECURITIES;

-- Check 5: Distribution by asset class
SELECT 
    'Asset Class Distribution' AS CHECK_NAME,
    ASSET_CLASS,
    COUNT(*) AS COUNT,
    ROUND(COUNT(*) * 100.0 / 1000, 2) AS PERCENTAGE
FROM BLOOMBERG_SECURITIES
GROUP BY ASSET_CLASS
ORDER BY COUNT DESC;

-- Check 6: Sample of unique companies
SELECT 
    'Top 10 Companies' AS CHECK_NAME,
    ISSUER_NAME,
    COUNT(*) AS SECURITY_COUNT
FROM BLOOMBERG_SECURITIES
GROUP BY ISSUER_NAME
ORDER BY SECURITY_COUNT DESC
LIMIT 10;

-- Check 7: ISIN uniqueness (should be 1000 unique)
SELECT 'ISIN Uniqueness' AS CHECK_NAME,
       COUNT(DISTINCT ISIN) AS UNIQUE_ISINS,
       CASE WHEN COUNT(DISTINCT ISIN) = 1000 THEN 'PASS' ELSE 'FAIL' END AS STATUS
FROM BLOOMBERG_SECURITIES;

-- Check 8: Sample data preview
SELECT 
    SECURITY_ID,
    SECURITY_NAME,
    ISIN,
    SEDOL,
    TICKER,
    ASSET_CLASS,
    ISSUER_NAME
FROM BLOOMBERG_SECURITIES
ORDER BY SECURITY_ID
LIMIT 20;


-- =============================================================================
-- BLOOMBERG DATA GENERATION COMPLETE
-- =============================================================================
-- Phase 2A Bloomberg Data Generation: ✅ COMPLETE
--
-- Summary:
-- ✅ Procedure Created: GENERATE_BLOOMBERG_SECURITIES
-- ✅ Records Generated: 1000 Bloomberg securities
-- ✅ UK Identifiers: ISINs (GB prefix), SEDOLs, Tickers
-- ✅ Data Quality: Verified with 8 quality checks
-- ✅ Variations: Multiple name formats for entity matching
--
-- Next Steps:
-- → Phase 2B: Generate Refinitiv securities data
-- → File: 2_Data_Generation_Refinitiv.sql
-- =============================================================================
