/*==============================================================================
  Project: Securities Master Resolution Platform for UK Asset Management
  Phase: 2B - Refinitiv Data Generation
  File: 2.1_Data_Generation_Refinitiv.sql
  Author: Rahul Sahay
  GitHub: https://github.com/rahulsahay123/securities-master-resolution
  Date: January 2026
  
  Description:
  This script creates a Python stored procedure to generate realistic 
  Refinitiv-style UK securities data with different naming conventions
  and formatting compared to Bloomberg. Uses same UK companies but with
  variations to enable entity resolution matching.
  
  Prerequisites:
  - Phase 1 (1_Setup.sql) completed successfully
  - Phase 2A (2_Data_Generation_Bloomberg.sql) completed
  - SECURITIES_MASTER database and ENTITY_RESOLUTION schema exist
  - REFINITIV_SECURITIES table created
  - SECURITIES_WH warehouse running
  
  Usage:
  Run all sections sequentially in Snowflake Snowsight interface
  
  Sections:
  1. Environment Setup
  2. Create Refinitiv Data Generator Procedure
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
-- SECTION 2: CREATE REFINITIV DATA GENERATOR PROCEDURE
-- =============================================================================
-- Purpose: Python stored procedure to generate realistic Refinitiv-style data

CREATE OR REPLACE PROCEDURE GENERATE_REFINITIV_SECURITIES(NUM_RECORDS INT)
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
    Generate realistic Refinitiv-style UK securities data
    with different naming conventions from Bloomberg
    """
    
    # UK company names - same as Bloomberg but with Refinitiv variations
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
    
    # Instrument types (Refinitiv terminology)
    instrument_types = ['Equity', 'Bond', 'Fund', 'ETF', 'Derivative']
    
    # UK currency
    currency_code = 'GBP'
    
    securities = []
    
    for i in range(num_records):
        # Select random company
        company = random.choice(uk_companies)
        
        # Generate unique ISIN (GB prefix for UK)
        # Format: GB + 10 alphanumeric characters
        isin_code = 'GB' + ''.join(random.choices(string.digits + string.ascii_uppercase, k=10))
        
        # Generate SEDOL (7 characters)
        sedol_code = ''.join(random.choices(string.digits + string.ascii_uppercase, k=7))
        
        # Generate ticker symbol (3-4 characters)
        ticker_symbol = ''.join(random.choices(string.ascii_uppercase, k=random.randint(3, 4)))
        
        # Select instrument type
        instrument_type = random.choice(instrument_types)
        
        # Build instrument name with Refinitiv-style variations
        # Refinitiv uses different formats than Bloomberg
        variations = [
            f"{company} {instrument_type}",
            f"{company} Ltd {instrument_type}",
            f"{company} Limited {instrument_type}",
            f"{company.replace(' ', '')} {instrument_type}",  # No spaces
            f"{company} ({instrument_type})",
            f"{instrument_type} - {company}",  # Reversed order
            f"{company} Ord {instrument_type}" if instrument_type == 'Equity' else f"{company} {instrument_type}"
        ]
        instrument_name = random.choice(variations)
        
        # Create RIC code (Reuters Instrument Code)
        # Format: TICKER.EXCHANGE (simplified)
        ric_code = f"{ticker_symbol}.L"  # .L for London Stock Exchange
        
        # Refinitiv uses "Issuer" instead of "Issuer Name"
        issuer = company
        
        # Add some abbreviations and variations for entity matching
        issuer_variations = [
            company,
            company.replace('Holdings', 'Hldgs'),
            company.replace('Group', 'Grp'),
            company.replace('Limited', 'Ltd'),
            company.replace('&', 'and'),
            company.upper()
        ]
        issuer = random.choice(issuer_variations)
        
        securities.append({
            'RIC_CODE': ric_code,
            'INSTRUMENT_NAME': instrument_name,
            'ISIN_CODE': isin_code,
            'SEDOL_CODE': sedol_code,
            'TICKER_SYMBOL': ticker_symbol,
            'INSTRUMENT_TYPE': instrument_type,
            'ISSUER': issuer,
            'CURRENCY_CODE': currency_code,
            'SOURCE': 'REFINITIV'
        })
    
    # Create DataFrame from list of dictionaries
    from snowflake.snowpark import Row
    df = session.create_dataframe([Row(**sec) for sec in securities])
    
    # Write to table
    df.write.mode("overwrite").save_as_table("REFINITIV_SECURITIES")
    
    return f"Successfully generated {num_records} Refinitiv securities"
$$;

-- Verify procedure was created
SHOW PROCEDURES LIKE 'GENERATE_REFINITIV_SECURITIES';


-- =============================================================================
-- SECTION 3: TEST WITH 5 RECORDS
-- =============================================================================
-- Purpose: Verify the procedure works before generating full dataset

-- Generate 5 test records
CALL GENERATE_REFINITIV_SECURITIES(5);

-- Verify test data
SELECT COUNT(*) AS TEST_RECORD_COUNT 
FROM REFINITIV_SECURITIES;

-- View test data
SELECT 
    RIC_CODE,
    INSTRUMENT_NAME,
    ISIN_CODE,
    SEDOL_CODE,
    TICKER_SYMBOL,
    INSTRUMENT_TYPE,
    ISSUER,
    CURRENCY_CODE,
    SOURCE
FROM REFINITIV_SECURITIES
ORDER BY RIC_CODE;


-- =============================================================================
-- SECTION 4: GENERATE FULL 1000 RECORDS
-- =============================================================================
-- Purpose: Generate complete Refinitiv dataset

-- Generate 1000 Refinitiv securities
CALL GENERATE_REFINITIV_SECURITIES(1000);

-- Confirm record count
SELECT COUNT(*) AS TOTAL_REFINITIV_RECORDS 
FROM REFINITIV_SECURITIES;


-- =============================================================================
-- SECTION 5: DATA VERIFICATION & QUALITY CHECKS
-- =============================================================================
-- Purpose: Comprehensive data quality validation

-- Check 1: Total record count (should be 1000)
SELECT 'Total Records' AS CHECK_NAME,
       COUNT(*) AS RESULT,
       CASE WHEN COUNT(*) = 1000 THEN 'PASS' ELSE 'FAIL' END AS STATUS
FROM REFINITIV_SECURITIES;

-- Check 2: No NULL values in key fields
SELECT 'NULL Check' AS CHECK_NAME,
       COUNT(*) AS NULL_COUNT,
       CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS STATUS
FROM REFINITIV_SECURITIES
WHERE RIC_CODE IS NULL 
   OR INSTRUMENT_NAME IS NULL 
   OR ISIN_CODE IS NULL;

-- Check 3: All ISINs start with GB (UK prefix)
SELECT 'ISIN Format Check' AS CHECK_NAME,
       COUNT(*) AS NON_GB_COUNT,
       CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS STATUS
FROM REFINITIV_SECURITIES
WHERE LEFT(ISIN_CODE, 2) != 'GB';

-- Check 4: All sources are REFINITIV
SELECT 'Source Check' AS CHECK_NAME,
       COUNT(DISTINCT SOURCE) AS UNIQUE_SOURCES,
       CASE WHEN COUNT(DISTINCT SOURCE) = 1 THEN 'PASS' ELSE 'FAIL' END AS STATUS
FROM REFINITIV_SECURITIES;

-- Check 5: All RIC codes end with .L (London)
SELECT 'RIC Format Check' AS CHECK_NAME,
       COUNT(*) AS NON_LONDON_COUNT,
       CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS STATUS
FROM REFINITIV_SECURITIES
WHERE RIGHT(RIC_CODE, 2) != '.L';

-- Check 6: Distribution by instrument type
SELECT 
    'Instrument Type Distribution' AS CHECK_NAME,
    INSTRUMENT_TYPE,
    COUNT(*) AS COUNT,
    ROUND(COUNT(*) * 100.0 / 1000, 2) AS PERCENTAGE
FROM REFINITIV_SECURITIES
GROUP BY INSTRUMENT_TYPE
ORDER BY COUNT DESC;

-- Check 7: Sample of unique issuers
SELECT 
    'Top 10 Issuers' AS CHECK_NAME,
    ISSUER,
    COUNT(*) AS SECURITY_COUNT
FROM REFINITIV_SECURITIES
GROUP BY ISSUER
ORDER BY SECURITY_COUNT DESC
LIMIT 10;

-- Check 8: ISIN uniqueness (should be 1000 unique)
SELECT 'ISIN Uniqueness' AS CHECK_NAME,
       COUNT(DISTINCT ISIN_CODE) AS UNIQUE_ISINS,
       CASE WHEN COUNT(DISTINCT ISIN_CODE) = 1000 THEN 'PASS' ELSE 'FAIL' END AS STATUS
FROM REFINITIV_SECURITIES;

-- Check 9: Sample data preview
SELECT 
    RIC_CODE,
    INSTRUMENT_NAME,
    ISIN_CODE,
    SEDOL_CODE,
    TICKER_SYMBOL,
    INSTRUMENT_TYPE,
    ISSUER
FROM REFINITIV_SECURITIES
ORDER BY RIC_CODE
LIMIT 20;


-- =============================================================================
-- REFINITIV DATA GENERATION COMPLETE
-- =============================================================================
-- Phase 2B Refinitiv Data Generation: ✅ COMPLETE
--
-- Summary:
-- ✅ Procedure Created: GENERATE_REFINITIV_SECURITIES
-- ✅ Records Generated: 1000 Refinitiv securities
-- ✅ UK Identifiers: ISINs (GB prefix), SEDOLs, RIC codes
-- ✅ Data Quality: Verified with 9 quality checks
-- ✅ Variations: Different naming conventions from Bloomberg
-- ✅ RIC Format: All codes end with .L (London Stock Exchange)
--
-- Key Differences from Bloomberg:
-- • Field names: RIC_CODE, INSTRUMENT_NAME, ISIN_CODE, SEDOL_CODE
-- • Name variations: Ltd, Limited, no spaces, reversed order
-- • Issuer abbreviations: Holdings→Hldgs, Group→Grp, &→and
-- • RIC codes follow Refinitiv format (TICKER.L)
--
-- Next Steps:
-- → Phase 2C: Generate FCA securities data
-- → File: 2.2_Data_Generation_FCA.sql
-- =============================================================================
