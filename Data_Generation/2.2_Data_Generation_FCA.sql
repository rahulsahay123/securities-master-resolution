/*==============================================================================
  Project: Securities Master Resolution Platform for UK Asset Management
  Phase: 2C - FCA Data Generation
  File: 2.2_Data_Generation_FCA.sql
  Author: Rahul Sahay
  GitHub: https://github.com/rahulsahay123/securities-master-resolution
  Date: January 2026
  
  Description:
  This script creates a Python stored procedure to generate realistic 
  FCA (Financial Conduct Authority) UK securities data representing the 
  regulatory perspective. Uses same UK companies but with FCA-specific
  naming conventions and fund types.
  
  Prerequisites:
  - Phase 1 (1_Setup.sql) completed successfully
  - Phase 2A (2_Data_Generation_Bloomberg.sql) completed
  - Phase 2B (2.1_Data_Generation_Refinitiv.sql) completed
  - SECURITIES_MASTER database and ENTITY_RESOLUTION schema exist
  - FCA_SECURITIES table created
  - SECURITIES_WH warehouse running
  
  Usage:
  Run all sections sequentially in Snowflake Snowsight interface
  
  Sections:
  1. Environment Setup
  2. Create FCA Data Generator Procedure
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
-- SECTION 2: CREATE FCA DATA GENERATOR PROCEDURE
-- =============================================================================
-- Purpose: Python stored procedure to generate realistic FCA-style regulatory data

CREATE OR REPLACE PROCEDURE GENERATE_FCA_SECURITIES(NUM_RECORDS INT)
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
    Generate realistic FCA-style UK securities data
    representing regulatory perspective
    """
    
    # UK company names - same as Bloomberg and Refinitiv
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
    
    # FCA fund types (regulatory classification)
    fund_types = [
        'OEIC',              # Open-Ended Investment Company
        'Unit Trust',        # Unit Trust
        'Investment Trust',  # Investment Trust
        'Pension Fund',      # Pension Fund
        'ETF'               # Exchange Traded Fund
    ]
    
    # UK currency
    currency = 'GBP'
    
    securities = []
    
    for i in range(num_records):
        # Select random company
        company = random.choice(uk_companies)
        
        # Generate FCA Reference Number
        # Format: FRN followed by 6 digits
        fca_ref_number = f"FRN{str(random.randint(100000, 999999))}"
        
        # Generate unique ISIN (GB prefix for UK)
        # Format: GB + 10 alphanumeric characters
        isin = 'GB' + ''.join(random.choices(string.digits + string.ascii_uppercase, k=10))
        
        # Generate SEDOL (7 characters)
        sedol = ''.join(random.choices(string.digits + string.ascii_uppercase, k=7))
        
        # Select fund type
        fund_type = random.choice(fund_types)
        
        # Build fund name with FCA-style variations
        # FCA uses more formal regulatory naming
        variations = [
            f"{company} {fund_type}",
            f"{company} Fund ({fund_type})",
            f"The {company} {fund_type}",
            f"{company} Investment {fund_type}",
            f"{company} Asset Management {fund_type}",
            f"{company.replace('Holdings', 'Asset Management')} {fund_type}",
            f"{company} UK {fund_type}"
        ]
        fund_name = random.choice(variations)
        
        # Manager name variations (FCA tracks fund managers)
        manager_variations = [
            f"{company} Asset Management",
            f"{company} Fund Management",
            f"{company} Investment Management",
            f"{company} Investments",
            f"{company.replace('Holdings', 'Asset Management')}",
            company
        ]
        manager_name = random.choice(manager_variations)
        
        securities.append({
            'FCA_REF_NUMBER': fca_ref_number,
            'FUND_NAME': fund_name,
            'ISIN': isin,
            'SEDOL': sedol,
            'FUND_TYPE': fund_type,
            'MANAGER_NAME': manager_name,
            'CURRENCY': currency,
            'SOURCE': 'FCA'
        })
    
    # Create DataFrame from list of dictionaries
    from snowflake.snowpark import Row
    df = session.create_dataframe([Row(**sec) for sec in securities])
    
    # Write to table
    df.write.mode("overwrite").save_as_table("FCA_SECURITIES")
    
    return f"Successfully generated {num_records} FCA securities"
$$;

-- Verify procedure was created
SHOW PROCEDURES LIKE 'GENERATE_FCA_SECURITIES';


-- =============================================================================
-- SECTION 3: TEST WITH 5 RECORDS
-- =============================================================================
-- Purpose: Verify the procedure works before generating full dataset

-- Generate 5 test records
CALL GENERATE_FCA_SECURITIES(5);

-- Verify test data
SELECT COUNT(*) AS TEST_RECORD_COUNT 
FROM FCA_SECURITIES;

-- View test data
SELECT 
    FCA_REF_NUMBER,
    FUND_NAME,
    ISIN,
    SEDOL,
    FUND_TYPE,
    MANAGER_NAME,
    CURRENCY,
    SOURCE
FROM FCA_SECURITIES
ORDER BY FCA_REF_NUMBER;


-- =============================================================================
-- SECTION 4: GENERATE FULL 1000 RECORDS
-- =============================================================================
-- Purpose: Generate complete FCA dataset

-- Generate 1000 FCA securities
CALL GENERATE_FCA_SECURITIES(1000);

-- Confirm record count
SELECT COUNT(*) AS TOTAL_FCA_RECORDS 
FROM FCA_SECURITIES;


-- =============================================================================
-- SECTION 5: DATA VERIFICATION & QUALITY CHECKS
-- =============================================================================
-- Purpose: Comprehensive data quality validation

-- Check 1: Total record count (should be 1000)
SELECT 'Total Records' AS CHECK_NAME,
       COUNT(*) AS RESULT,
       CASE WHEN COUNT(*) = 1000 THEN 'PASS' ELSE 'FAIL' END AS STATUS
FROM FCA_SECURITIES;

-- Check 2: No NULL values in key fields
SELECT 'NULL Check' AS CHECK_NAME,
       COUNT(*) AS NULL_COUNT,
       CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS STATUS
FROM FCA_SECURITIES
WHERE FCA_REF_NUMBER IS NULL 
   OR FUND_NAME IS NULL 
   OR ISIN IS NULL;

-- Check 3: All ISINs start with GB (UK prefix)
SELECT 'ISIN Format Check' AS CHECK_NAME,
       COUNT(*) AS NON_GB_COUNT,
       CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS STATUS
FROM FCA_SECURITIES
WHERE LEFT(ISIN, 2) != 'GB';

-- Check 4: All sources are FCA
SELECT 'Source Check' AS CHECK_NAME,
       COUNT(DISTINCT SOURCE) AS UNIQUE_SOURCES,
       CASE WHEN COUNT(DISTINCT SOURCE) = 1 THEN 'PASS' ELSE 'FAIL' END AS STATUS
FROM FCA_SECURITIES;

-- Check 5: All FCA reference numbers start with FRN
SELECT 'FCA Ref Format Check' AS CHECK_NAME,
       COUNT(*) AS NON_FRN_COUNT,
       CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS STATUS
FROM FCA_SECURITIES
WHERE LEFT(FCA_REF_NUMBER, 3) != 'FRN';

-- Check 6: Distribution by fund type
SELECT 
    'Fund Type Distribution' AS CHECK_NAME,
    FUND_TYPE,
    COUNT(*) AS COUNT,
    ROUND(COUNT(*) * 100.0 / 1000, 2) AS PERCENTAGE
FROM FCA_SECURITIES
GROUP BY FUND_TYPE
ORDER BY COUNT DESC;

-- Check 7: Sample of unique managers
SELECT 
    'Top 10 Fund Managers' AS CHECK_NAME,
    MANAGER_NAME,
    COUNT(*) AS FUND_COUNT
FROM FCA_SECURITIES
GROUP BY MANAGER_NAME
ORDER BY FUND_COUNT DESC
LIMIT 10;

-- Check 8: ISIN uniqueness (should be 1000 unique)
SELECT 'ISIN Uniqueness' AS CHECK_NAME,
       COUNT(DISTINCT ISIN) AS UNIQUE_ISINS,
       CASE WHEN COUNT(DISTINCT ISIN) = 1000 THEN 'PASS' ELSE 'FAIL' END AS STATUS
FROM FCA_SECURITIES;

-- Check 9: Sample data preview
SELECT 
    FCA_REF_NUMBER,
    FUND_NAME,
    ISIN,
    SEDOL,
    FUND_TYPE,
    MANAGER_NAME
FROM FCA_SECURITIES
ORDER BY FCA_REF_NUMBER
LIMIT 20;


-- =============================================================================
-- FCA DATA GENERATION COMPLETE
-- =============================================================================
-- Phase 2C FCA Data Generation: ✅ COMPLETE
--
-- Summary:
-- ✅ Procedure Created: GENERATE_FCA_SECURITIES
-- ✅ Records Generated: 1000 FCA securities
-- ✅ UK Identifiers: ISINs (GB prefix), SEDOLs, FCA Reference Numbers
-- ✅ Data Quality: Verified with 9 quality checks
-- ✅ Variations: Regulatory naming conventions (formal fund names)
-- ✅ FCA Format: All reference numbers start with FRN
--
-- Key Differences from Bloomberg and Refinitiv:
-- • Field names: FCA_REF_NUMBER, FUND_NAME, FUND_TYPE, MANAGER_NAME
-- • Regulatory focus: OEIC, Unit Trust, Investment Trust classifications
-- • Formal naming: "The [Company] Investment Fund", "[Company] Asset Management"
-- • Manager tracking: Separate manager name field for regulatory oversight
--
-- Next Steps:
-- → Verify all 3 data sources (Bloomberg, Refinitiv, FCA)
-- → Total records should be 3000 (1000 each)
-- =============================================================================
