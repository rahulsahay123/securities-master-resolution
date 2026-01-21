/*==============================================================================
  Project: Securities Master Resolution Platform for UK Asset Management
  Phase: 3.0 - Data Profiling & Analysis (PRE-HARMONIZATION)
  File: 3.0_Data_Profiling_Analysis.sql
  Author: Rahul Sahay
  GitHub: https://github.com/rahulsahay123/securities-master-resolution
  Date: January 2026
  
  Description:
  This FIXED script profiles and analyzes the raw data from all three sources
  to identify data quality issues. This version has corrected queries that
  will actually return results!
  
  RUN THIS BEFORE 3.1_Data_Harmonization.sql to understand WHY we need cleaning!
  
==============================================================================*/

USE WAREHOUSE SECURITIES_WH;
USE DATABASE SECURITIES_MASTER;
USE SCHEMA ENTITY_RESOLUTION;

-- =============================================================================
-- SECTION 1: BASIC DATA OVERVIEW
-- =============================================================================

SELECT '========================================' AS INFO
UNION ALL SELECT '📊 DATA PROFILING ANALYSIS'
UNION ALL SELECT '========================================';

-- Record counts
SELECT '1️⃣ RECORD COUNTS' AS SECTION, '' AS DETAILS;

SELECT 'Bloomberg' AS SOURCE, COUNT(*) AS RECORDS FROM BLOOMBERG_SECURITIES
UNION ALL
SELECT 'Refinitiv' AS SOURCE, COUNT(*) AS RECORDS FROM REFINITIV_SECURITIES
UNION ALL
SELECT 'FCA' AS SOURCE, COUNT(*) AS RECORDS FROM FCA_SECURITIES;


-- =============================================================================
-- SECTION 2: ACTUAL SAMPLE DATA (RAW)
-- =============================================================================
-- Let's see what the data ACTUALLY looks like

SELECT '2️⃣ RAW DATA SAMPLES' AS SECTION, 
       'See the actual data before cleaning' AS DETAILS;

-- Bloomberg samples
SELECT 'BLOOMBERG SAMPLES' AS SOURCE_TYPE,
       SECURITY_ID,
       SECURITY_NAME,
       ISSUER_NAME,
       ASSET_CLASS
FROM BLOOMBERG_SECURITIES
LIMIT 10;

-- Refinitiv samples  
SELECT 'REFINITIV SAMPLES' AS SOURCE_TYPE,
       RIC_CODE AS SECURITY_ID,
       INSTRUMENT_NAME AS SECURITY_NAME,
       ISSUER AS ISSUER_NAME,
       INSTRUMENT_TYPE AS ASSET_CLASS
FROM REFINITIV_SECURITIES
LIMIT 10;

-- FCA samples
SELECT 'FCA SAMPLES' AS SOURCE_TYPE,
       FCA_REF_NUMBER AS SECURITY_ID,
       FUND_NAME AS SECURITY_NAME,
       MANAGER_NAME AS ISSUER_NAME,
       FUND_TYPE AS ASSET_CLASS
FROM FCA_SECURITIES
LIMIT 10;


-- =============================================================================
-- SECTION 3: FIELD NAME MAPPING PROBLEM
-- =============================================================================

SELECT '3️⃣ FIELD NAME INCONSISTENCIES' AS SECTION,
       'Each source uses different column names!' AS DETAILS;

SELECT 
    'Security ID' AS DATA_ELEMENT,
    'SECURITY_ID' AS BLOOMBERG,
    'RIC_CODE' AS REFINITIV,
    'FCA_REF_NUMBER' AS FCA,
    '❌ DIFFERENT' AS STATUS
UNION ALL
SELECT 
    'Security Name' AS DATA_ELEMENT,
    'SECURITY_NAME' AS BLOOMBERG,
    'INSTRUMENT_NAME' AS REFINITIV,
    'FUND_NAME' AS FCA,
    '❌ DIFFERENT' AS STATUS
UNION ALL
SELECT 
    'ISIN' AS DATA_ELEMENT,
    'ISIN' AS BLOOMBERG,
    'ISIN_CODE' AS REFINITIV,
    'ISIN' AS FCA,
    '⚠️ PARTIAL' AS STATUS
UNION ALL
SELECT 
    'Asset Type' AS DATA_ELEMENT,
    'ASSET_CLASS' AS BLOOMBERG,
    'INSTRUMENT_TYPE' AS REFINITIV,
    'FUND_TYPE' AS FCA,
    '❌ DIFFERENT' AS STATUS;


-- =============================================================================
-- SECTION 4: CASE SENSITIVITY ANALYSIS
-- =============================================================================

SELECT '4️⃣ CASE SENSITIVITY ISSUES' AS SECTION,
       'Mixed case will prevent matching!' AS DETAILS;

-- Check case variations in Bloomberg
SELECT 
    'Bloomberg' AS SOURCE,
    COUNT(*) AS TOTAL_RECORDS,
    COUNT(CASE WHEN SECURITY_NAME = UPPER(SECURITY_NAME) THEN 1 END) AS ALL_UPPERCASE,
    COUNT(CASE WHEN SECURITY_NAME = LOWER(SECURITY_NAME) THEN 1 END) AS ALL_LOWERCASE,
    COUNT(CASE WHEN SECURITY_NAME != UPPER(SECURITY_NAME) 
               AND SECURITY_NAME != LOWER(SECURITY_NAME) THEN 1 END) AS MIXED_CASE
FROM BLOOMBERG_SECURITIES

UNION ALL

SELECT 
    'Refinitiv' AS SOURCE,
    COUNT(*) AS TOTAL_RECORDS,
    COUNT(CASE WHEN INSTRUMENT_NAME = UPPER(INSTRUMENT_NAME) THEN 1 END) AS ALL_UPPERCASE,
    COUNT(CASE WHEN INSTRUMENT_NAME = LOWER(INSTRUMENT_NAME) THEN 1 END) AS ALL_LOWERCASE,
    COUNT(CASE WHEN INSTRUMENT_NAME != UPPER(INSTRUMENT_NAME) 
               AND INSTRUMENT_NAME != LOWER(INSTRUMENT_NAME) THEN 1 END) AS MIXED_CASE
FROM REFINITIV_SECURITIES

UNION ALL

SELECT 
    'FCA' AS SOURCE,
    COUNT(*) AS TOTAL_RECORDS,
    COUNT(CASE WHEN FUND_NAME = UPPER(FUND_NAME) THEN 1 END) AS ALL_UPPERCASE,
    COUNT(CASE WHEN FUND_NAME = LOWER(FUND_NAME) THEN 1 END) AS ALL_LOWERCASE,
    COUNT(CASE WHEN FUND_NAME != UPPER(FUND_NAME) 
               AND FUND_NAME != LOWER(FUND_NAME) THEN 1 END) AS MIXED_CASE
FROM FCA_SECURITIES;

-- Show actual examples of mixed case
SELECT '📋 Examples of Case Issues' AS EXAMPLE_TYPE,
       'Original' AS BEFORE,
       'What it should be' AS AFTER;

SELECT 
    'Bloomberg Example' AS SOURCE,
    SECURITY_NAME AS BEFORE,
    UPPER(SECURITY_NAME) AS AFTER
FROM BLOOMBERG_SECURITIES
WHERE SECURITY_NAME != UPPER(SECURITY_NAME)
LIMIT 5;

SELECT 
    'Refinitiv Example' AS SOURCE,
    INSTRUMENT_NAME AS BEFORE,
    UPPER(INSTRUMENT_NAME) AS AFTER
FROM REFINITIV_SECURITIES
WHERE INSTRUMENT_NAME != UPPER(INSTRUMENT_NAME)
LIMIT 5;

SELECT 
    'FCA Example' AS SOURCE,
    FUND_NAME AS BEFORE,
    UPPER(FUND_NAME) AS AFTER
FROM FCA_SECURITIES
WHERE FUND_NAME != UPPER(FUND_NAME)
LIMIT 5;


-- =============================================================================
-- SECTION 5: NAMING CONVENTION VARIATIONS
-- =============================================================================

SELECT '5️⃣ NAMING CONVENTION VARIATIONS' AS SECTION,
       'Same company named differently!' AS DETAILS;

-- Find records with "plc", "PLC", "Ltd", "Limited" variations
SELECT 'Bloomberg PLC/plc variations' AS VARIATION_TYPE,
       SECURITY_NAME,
       ISSUER_NAME
FROM BLOOMBERG_SECURITIES
WHERE SECURITY_NAME LIKE '%plc%' 
   OR SECURITY_NAME LIKE '%PLC%'
LIMIT 5;

SELECT 'Refinitiv Ltd/Limited variations' AS VARIATION_TYPE,
       INSTRUMENT_NAME AS SECURITY_NAME,
       ISSUER AS ISSUER_NAME
FROM REFINITIV_SECURITIES
WHERE INSTRUMENT_NAME LIKE '%Ltd%' 
   OR INSTRUMENT_NAME LIKE '%Limited%'
LIMIT 5;

SELECT 'FCA Fund variations' AS VARIATION_TYPE,
       FUND_NAME AS SECURITY_NAME,
       MANAGER_NAME AS ISSUER_NAME
FROM FCA_SECURITIES
WHERE FUND_NAME LIKE '%The %' 
   OR FUND_NAME LIKE '%Fund%'
LIMIT 5;


-- =============================================================================
-- SECTION 6: CHARACTER LENGTH ANALYSIS
-- =============================================================================

SELECT '6️⃣ NAME LENGTH VARIATIONS' AS SECTION,
       'Different name lengths indicate formatting differences' AS DETAILS;

SELECT 
    'Bloomberg' AS SOURCE,
    MIN(LENGTH(SECURITY_NAME)) AS MIN_LENGTH,
    MAX(LENGTH(SECURITY_NAME)) AS MAX_LENGTH,
    ROUND(AVG(LENGTH(SECURITY_NAME)), 2) AS AVG_LENGTH,
    'Longer = more descriptive/verbose' AS NOTE
FROM BLOOMBERG_SECURITIES

UNION ALL

SELECT 
    'Refinitiv' AS SOURCE,
    MIN(LENGTH(INSTRUMENT_NAME)) AS MIN_LENGTH,
    MAX(LENGTH(INSTRUMENT_NAME)) AS MAX_LENGTH,
    ROUND(AVG(LENGTH(INSTRUMENT_NAME)), 2) AS AVG_LENGTH,
    'Longer = more descriptive/verbose' AS NOTE
FROM REFINITIV_SECURITIES

UNION ALL

SELECT 
    'FCA' AS SOURCE,
    MIN(LENGTH(FUND_NAME)) AS MIN_LENGTH,
    MAX(LENGTH(FUND_NAME)) AS MAX_LENGTH,
    ROUND(AVG(LENGTH(FUND_NAME)), 2) AS AVG_LENGTH,
    'Longer = more descriptive/verbose' AS NOTE
FROM FCA_SECURITIES;


-- =============================================================================
-- SECTION 7: SPACING ISSUES
-- =============================================================================

SELECT '7️⃣ SPACING ISSUES' AS SECTION,
       'Extra spaces will break exact matching!' AS DETAILS;

-- Check for leading/trailing spaces
SELECT 
    'Bloomberg' AS SOURCE,
    COUNT(CASE WHEN SECURITY_NAME != TRIM(SECURITY_NAME) THEN 1 END) AS HAS_EXTRA_SPACES,
    COUNT(CASE WHEN SECURITY_NAME LIKE '% %' THEN 1 END) AS HAS_ANY_SPACES,
    'Extra spaces break matching' AS ISSUE
FROM BLOOMBERG_SECURITIES

UNION ALL

SELECT 
    'Refinitiv' AS SOURCE,
    COUNT(CASE WHEN INSTRUMENT_NAME != TRIM(INSTRUMENT_NAME) THEN 1 END) AS HAS_EXTRA_SPACES,
    COUNT(CASE WHEN INSTRUMENT_NAME LIKE '% %' THEN 1 END) AS HAS_ANY_SPACES,
    'Extra spaces break matching' AS ISSUE
FROM REFINITIV_SECURITIES

UNION ALL

SELECT 
    'FCA' AS SOURCE,
    COUNT(CASE WHEN FUND_NAME != TRIM(FUND_NAME) THEN 1 END) AS HAS_EXTRA_SPACES,
    COUNT(CASE WHEN FUND_NAME LIKE '% %' THEN 1 END) AS HAS_ANY_SPACES,
    'Extra spaces break matching' AS ISSUE
FROM FCA_SECURITIES;


-- =============================================================================
-- SECTION 8: CROSS-SOURCE COMPARISON
-- =============================================================================

SELECT '8️⃣ CROSS-SOURCE COMPARISON' AS SECTION,
       'How the same entities look different across sources' AS DETAILS;

-- Compare how companies with similar names appear
(SELECT 
    'HSBC Comparison' AS COMPANY,
    'Bloomberg' AS SOURCE,
    SECURITY_NAME AS HOW_IT_APPEARS,
    ISSUER_NAME AS ISSUER_FORMAT
FROM BLOOMBERG_SECURITIES
WHERE ISSUER_NAME LIKE '%HSBC%'
LIMIT 3)

UNION ALL

(SELECT 
    'HSBC Comparison' AS COMPANY,
    'Refinitiv' AS SOURCE,
    INSTRUMENT_NAME AS HOW_IT_APPEARS,
    ISSUER AS ISSUER_FORMAT
FROM REFINITIV_SECURITIES
WHERE ISSUER LIKE '%HSBC%'
LIMIT 3)

UNION ALL

(SELECT 
    'HSBC Comparison' AS COMPANY,
    'FCA' AS SOURCE,
    FUND_NAME AS HOW_IT_APPEARS,
    MANAGER_NAME AS ISSUER_FORMAT
FROM FCA_SECURITIES
WHERE MANAGER_NAME LIKE '%HSBC%'
LIMIT 3);


-- Same for BP
(SELECT 
    'BP Comparison' AS COMPANY,
    'Bloomberg' AS SOURCE,
    SECURITY_NAME AS HOW_IT_APPEARS,
    ISSUER_NAME AS ISSUER_FORMAT
FROM BLOOMBERG_SECURITIES
WHERE ISSUER_NAME LIKE '%BP%'
LIMIT 3)

UNION ALL

(SELECT 
    'BP Comparison' AS COMPANY,
    'Refinitiv' AS SOURCE,
    INSTRUMENT_NAME AS HOW_IT_APPEARS,
    ISSUER AS ISSUER_FORMAT
FROM REFINITIV_SECURITIES
WHERE ISSUER LIKE '%BP%'
LIMIT 3)

UNION ALL

(SELECT 
    'BP Comparison' AS COMPANY,
    'FCA' AS SOURCE,
    FUND_NAME AS HOW_IT_APPEARS,
    MANAGER_NAME AS ISSUER_FORMAT
FROM FCA_SECURITIES
WHERE MANAGER_NAME LIKE '%BP%'
LIMIT 3);


-- =============================================================================
-- SECTION 9: EXACT MATCH TEST (WILL FAIL)
-- =============================================================================

SELECT '9️⃣ WHY EXACT MATCHING FAILS' AS SECTION,
       'Trying to match without harmonization...' AS DETAILS;

-- Try to match Bloomberg and Refinitiv by ISIN
-- This shows how many would match on identifiers
SELECT 
    'ISIN Match Attempt' AS MATCH_TYPE,
    COUNT(*) AS POTENTIAL_MATCHES,
    'These have same ISIN but different names' AS EXPLANATION
FROM BLOOMBERG_SECURITIES B
INNER JOIN REFINITIV_SECURITIES R
    ON B.ISIN = R.ISIN_CODE;

-- But try to match by security name (will fail)
SELECT 
    'Name Match Attempt' AS MATCH_TYPE,
    COUNT(*) AS MATCHES_FOUND,
    'Almost ZERO - names too different!' AS EXPLANATION
FROM BLOOMBERG_SECURITIES B
INNER JOIN REFINITIV_SECURITIES R
    ON B.SECURITY_NAME = R.INSTRUMENT_NAME;


-- =============================================================================
-- SECTION 10: SUMMARY OF PROBLEMS
-- =============================================================================

SELECT '========================================' AS DIVIDER
UNION ALL SELECT '📋 SUMMARY OF DATA QUALITY ISSUES'
UNION ALL SELECT '========================================';

SELECT 
    '❌ Problem #1' AS ISSUE,
    'Different field names' AS DESCRIPTION,
    'SECURITY_NAME vs INSTRUMENT_NAME vs FUND_NAME' AS EXAMPLE,
    'Need to map to standard names' AS SOLUTION

UNION ALL SELECT 
    '❌ Problem #2' AS ISSUE,
    'Case sensitivity' AS DESCRIPTION,
    'HSBC vs Hsbc vs hsbc' AS EXAMPLE,
    'Convert everything to UPPERCASE' AS SOLUTION

UNION ALL SELECT 
    '❌ Problem #3' AS ISSUE,
    'Naming conventions' AS DESCRIPTION,
    'Holdings vs Hldgs, PLC vs plc, Ltd vs Limited' AS EXAMPLE,
    'Standardize abbreviations' AS SOLUTION

UNION ALL SELECT 
    '❌ Problem #4' AS ISSUE,
    'Extra spaces' AS DESCRIPTION,
    'Leading/trailing spaces, multiple spaces' AS EXAMPLE,
    'TRIM and normalize spacing' AS SOLUTION

UNION ALL SELECT 
    '❌ Problem #5' AS ISSUE,
    'Different descriptors' AS DESCRIPTION,
    'Same company but different fund/instrument descriptions' AS EXAMPLE,
    'Use AI embeddings for semantic matching' AS SOLUTION

UNION ALL SELECT 
    '' AS ISSUE,
    '' AS DESCRIPTION,
    '' AS EXAMPLE,
    '' AS SOLUTION

UNION ALL SELECT 
    '✅ NEXT STEP' AS ISSUE,
    'Run 3.1_Data_Harmonization.sql' AS DESCRIPTION,
    'This will fix ALL the issues above' AS EXAMPLE,
    'Then we can match using AI embeddings' AS SOLUTION;


-- =============================================================================
-- KEY INSIGHT: WHY WE NEED HARMONIZATION
-- =============================================================================

SELECT '🎯 KEY INSIGHT' AS INFO,
       'Without harmonization, matching is IMPOSSIBLE!' AS INSIGHT;

-- Show side-by-side comparison of what needs to match
SELECT 
    B.SECURITY_NAME AS BLOOMBERG_NAME,
    R.INSTRUMENT_NAME AS REFINITIV_NAME,
    F.FUND_NAME AS FCA_NAME,
    B.ISIN AS BLOOMBERG_ISIN,
    R.ISIN_CODE AS REFINITIV_ISIN,
    F.ISIN AS FCA_ISIN,
    CASE 
        WHEN B.ISIN = R.ISIN_CODE AND R.ISIN_CODE = F.ISIN 
        THEN '✅ Same ISIN'
        ELSE '❌ Different ISINs'
    END AS IDENTIFIER_MATCH,
    CASE 
        WHEN B.SECURITY_NAME = R.INSTRUMENT_NAME 
        THEN '✅ Names Match'
        ELSE '❌ Names DON''T Match'
    END AS NAME_MATCH
FROM BLOOMBERG_SECURITIES B
CROSS JOIN REFINITIV_SECURITIES R
CROSS JOIN FCA_SECURITIES F
WHERE B.ISSUER_NAME LIKE '%Shell%'
  AND R.ISSUER LIKE '%Shell%'
  AND F.MANAGER_NAME LIKE '%Shell%'
LIMIT 10;


-- =============================================================================
-- DATA PROFILING COMPLETE
-- =============================================================================
SELECT '========================================' AS DIVIDER
UNION ALL SELECT '✅ PROFILING COMPLETE!'
UNION ALL SELECT '========================================'
UNION ALL SELECT 'You now understand WHY we need harmonization'
UNION ALL SELECT 'Next: Run 3.1_Data_Harmonization.sql';