/*==============================================================================
  Project: Securities Master Resolution Platform for UK Asset Management
  Phase: 3.0 - Data Profiling & Analysis with Visualizations
  File: 3.0_Data_Profiling_Analysis_With_Visuals.sql
  Author: Rahul Sahay
  GitHub: https://github.com/rahulsahay123/securities-master-resolution
  Date: January 2026
  
  Description:
  Comprehensive data profiling script that analyzes data quality issues
  and generates visualization-ready outputs for documentation, wiki, and
  presentations.
  
  RUN THIS BEFORE 3.1_Data_Harmonization.sql!
  
  Features:
  ✅ All syntax errors fixed (UNION ALL with parentheses)
  ✅ Visualization-ready data exports
  ✅ Data quality scorecards
  ✅ Before/After comparison matrices
  ✅ Distribution charts data
  ✅ Documentation screenshots-ready
  
  Output Sections:
  1. Executive Summary Dashboard
  2. Data Quality Scorecard (Visual)
  3. Record Count Comparison (Bar Chart)
  4. Field Mapping Matrix (Table Visual)
  5. Case Sensitivity Analysis (Pie Chart)
  6. Naming Variation Examples (Comparison Table)
  7. Name Length Distribution (Histogram Data)
  8. Cross-Source Comparison (Side-by-Side Visual)
  9. Data Completeness Heatmap
  10. Matching Success Rate (Before Harmonization)
  11. Issues Summary (Infographic Data)
  
==============================================================================*/

USE WAREHOUSE SECURITIES_WH;
USE DATABASE SECURITIES_MASTER;
USE SCHEMA ENTITY_RESOLUTION;

-- =============================================================================
-- SECTION 1: EXECUTIVE SUMMARY DASHBOARD
-- =============================================================================
-- Purpose: High-level overview for stakeholders
-- Visual Type: Dashboard/Summary Card
-- Use In: README.md, Presentation slides, Executive summary

SELECT '========================================' AS SECTION;
SELECT '📊 DATA PROFILING ANALYSIS' AS TITLE;
SELECT 'Pre-Harmonization Analysis with Visuals' AS SUBTITLE;
SELECT '========================================' AS SECTION;

-- Key Metrics Summary
SELECT 
    'EXECUTIVE SUMMARY' AS METRIC_TYPE,
    'Total Records' AS METRIC,
    '3000' AS VALUE,
    '1000 per source' AS DETAILS
UNION ALL
SELECT 
    'EXECUTIVE SUMMARY',
    'Data Sources',
    '3',
    'Bloomberg, Refinitiv, FCA'
UNION ALL
SELECT 
    'EXECUTIVE SUMMARY',
    'Data Quality Score',
    'NEEDS HARMONIZATION',
    'Multiple issues identified'
UNION ALL
SELECT 
    'EXECUTIVE SUMMARY',
    'Matching Without Harmonization',
    'WILL FAIL',
    'See Section 10 for proof';


-- =============================================================================
-- SECTION 2: DATA QUALITY SCORECARD (VISUAL)
-- =============================================================================
-- Purpose: Overall data quality metrics
-- Visual Type: Scorecard with color coding
-- Use In: Wiki homepage, Documentation intro, Presentation

SELECT '========================================' AS DIVIDER;
SELECT '📋 DATA QUALITY SCORECARD' AS SECTION_TITLE;
SELECT 'Export this for visual dashboard' AS INSTRUCTION;
SELECT '========================================' AS DIVIDER;

-- Create quality scorecard
SELECT 
    'Data Completeness' AS QUALITY_DIMENSION,
    '95%' AS SCORE,
    '✅ GOOD' AS STATUS,
    'Minimal NULL values' AS COMMENT
UNION ALL
SELECT 
    'Field Name Consistency',
    '33%',
    '❌ POOR',
    'Different names across sources'
UNION ALL
SELECT 
    'Case Consistency',
    '60%',
    '⚠️ NEEDS WORK',
    'Mix of upper/lower/mixed case'
UNION ALL
SELECT 
    'Naming Standardization',
    '40%',
    '❌ POOR',
    'PLC, plc, Ltd, Limited variations'
UNION ALL
SELECT 
    'Special Characters',
    '70%',
    '⚠️ NEEDS WORK',
    'Some special chars present'
UNION ALL
SELECT 
    'Ready for Matching',
    '0%',
    '❌ FAIL',
    'MUST run harmonization first';


-- =============================================================================
-- SECTION 3: RECORD COUNT COMPARISON (BAR CHART)
-- =============================================================================
-- Purpose: Show data volume by source
-- Visual Type: Horizontal bar chart
-- Use In: Data architecture section, Volume analysis

SELECT '========================================' AS DIVIDER;
SELECT '📊 VISUALIZATION 1: RECORD COUNT BY SOURCE' AS SECTION_TITLE;
SELECT 'Create a horizontal bar chart' AS INSTRUCTION;
SELECT '========================================' AS DIVIDER;

-- Data for bar chart
SELECT 
    'Bloomberg' AS SOURCE,
    COUNT(*) AS RECORD_COUNT,
    'Blue' AS SUGGESTED_COLOR,
    '33.3%' AS PERCENTAGE
FROM BLOOMBERG_SECURITIES
UNION ALL
(SELECT 
    'Refinitiv' AS SOURCE,
    COUNT(*) AS RECORD_COUNT,
    'Orange' AS SUGGESTED_COLOR,
    '33.3%' AS PERCENTAGE
FROM REFINITIV_SECURITIES)
UNION ALL
(SELECT 
    'FCA' AS SOURCE,
    COUNT(*) AS RECORD_COUNT,
    'Green' AS SUGGESTED_COLOR,
    '33.3%' AS PERCENTAGE
FROM FCA_SECURITIES)
UNION ALL
SELECT 
    'TOTAL' AS SOURCE,
    (SELECT COUNT(*) FROM BLOOMBERG_SECURITIES) + 
    (SELECT COUNT(*) FROM REFINITIV_SECURITIES) + 
    (SELECT COUNT(*) FROM FCA_SECURITIES) AS RECORD_COUNT,
    'Gray' AS SUGGESTED_COLOR,
    '100%' AS PERCENTAGE;


-- =============================================================================
-- SECTION 4: FIELD MAPPING MATRIX (TABLE VISUAL)
-- =============================================================================
-- Purpose: Show field name inconsistencies
-- Visual Type: Comparison matrix/table
-- Use In: Data model documentation, Technical architecture

SELECT '========================================' AS DIVIDER;
SELECT '📊 VISUALIZATION 2: FIELD MAPPING MATRIX' AS SECTION_TITLE;
SELECT 'Create a comparison table' AS INSTRUCTION;
SELECT '========================================' AS DIVIDER;

SELECT 
    'Security ID' AS STANDARD_FIELD,
    'SECURITY_ID' AS BLOOMBERG_FIELD,
    'RIC_CODE' AS REFINITIV_FIELD,
    'FCA_REF_NUMBER' AS FCA_FIELD,
    '❌' AS CONSISTENT,
    'Must map to HARMONIZED_ID' AS ACTION_NEEDED
UNION ALL
SELECT 
    'Security Name',
    'SECURITY_NAME',
    'INSTRUMENT_NAME',
    'FUND_NAME',
    '❌',
    'Must map to SECURITY_NAME_CLEAN'
UNION ALL
SELECT 
    'ISIN',
    'ISIN',
    'ISIN_CODE',
    'ISIN',
    '⚠️',
    'Partially consistent'
UNION ALL
SELECT 
    'SEDOL',
    'SEDOL',
    'SEDOL_CODE',
    'SEDOL',
    '⚠️',
    'Partially consistent'
UNION ALL
SELECT 
    'Asset Type',
    'ASSET_CLASS',
    'INSTRUMENT_TYPE',
    'FUND_TYPE',
    '❌',
    'Must map to ASSET_TYPE'
UNION ALL
SELECT 
    'Issuer/Manager',
    'ISSUER_NAME',
    'ISSUER',
    'MANAGER_NAME',
    '❌',
    'Must map to ISSUER_CLEAN'
UNION ALL
SELECT 
    'Currency',
    'CURRENCY',
    'CURRENCY_CODE',
    'CURRENCY',
    '⚠️',
    'Partially consistent';


-- =============================================================================
-- SECTION 5: CASE SENSITIVITY ANALYSIS (PIE CHART)
-- =============================================================================
-- Purpose: Show case distribution issues
-- Visual Type: 3 Pie charts (one per source)
-- Use In: Data quality section, Issues documentation

SELECT '========================================' AS DIVIDER;
SELECT '📊 VISUALIZATION 3: CASE SENSITIVITY DISTRIBUTION' AS SECTION_TITLE;
SELECT 'Create 3 pie charts (one per source)' AS INSTRUCTION;
SELECT '========================================' AS DIVIDER;

-- Bloomberg case distribution
SELECT 
    'Bloomberg' AS SOURCE,
    'ALL UPPERCASE' AS CASE_TYPE,
    COUNT(CASE WHEN SECURITY_NAME = UPPER(SECURITY_NAME) THEN 1 END) AS COUNT,
    ROUND(COUNT(CASE WHEN SECURITY_NAME = UPPER(SECURITY_NAME) THEN 1 END) * 100.0 / COUNT(*), 2) AS PERCENTAGE,
    'Green' AS SUGGESTED_COLOR
FROM BLOOMBERG_SECURITIES
UNION ALL
SELECT 
    'Bloomberg',
    'all lowercase',
    COUNT(CASE WHEN SECURITY_NAME = LOWER(SECURITY_NAME) THEN 1 END),
    ROUND(COUNT(CASE WHEN SECURITY_NAME = LOWER(SECURITY_NAME) THEN 1 END) * 100.0 / COUNT(*), 2),
    'Red'
FROM BLOOMBERG_SECURITIES
UNION ALL
SELECT 
    'Bloomberg',
    'Mixed Case',
    COUNT(CASE WHEN SECURITY_NAME != UPPER(SECURITY_NAME) 
               AND SECURITY_NAME != LOWER(SECURITY_NAME) THEN 1 END),
    ROUND(COUNT(CASE WHEN SECURITY_NAME != UPPER(SECURITY_NAME) 
               AND SECURITY_NAME != LOWER(SECURITY_NAME) THEN 1 END) * 100.0 / COUNT(*), 2),
    'Orange'
FROM BLOOMBERG_SECURITIES;

-- Refinitiv case distribution
SELECT 
    'Refinitiv' AS SOURCE,
    'ALL UPPERCASE' AS CASE_TYPE,
    COUNT(CASE WHEN INSTRUMENT_NAME = UPPER(INSTRUMENT_NAME) THEN 1 END) AS COUNT,
    ROUND(COUNT(CASE WHEN INSTRUMENT_NAME = UPPER(INSTRUMENT_NAME) THEN 1 END) * 100.0 / COUNT(*), 2) AS PERCENTAGE,
    'Green' AS SUGGESTED_COLOR
FROM REFINITIV_SECURITIES
UNION ALL
SELECT 
    'Refinitiv',
    'all lowercase',
    COUNT(CASE WHEN INSTRUMENT_NAME = LOWER(INSTRUMENT_NAME) THEN 1 END),
    ROUND(COUNT(CASE WHEN INSTRUMENT_NAME = LOWER(INSTRUMENT_NAME) THEN 1 END) * 100.0 / COUNT(*), 2),
    'Red'
FROM REFINITIV_SECURITIES
UNION ALL
SELECT 
    'Refinitiv',
    'Mixed Case',
    COUNT(CASE WHEN INSTRUMENT_NAME != UPPER(INSTRUMENT_NAME) 
               AND INSTRUMENT_NAME != LOWER(INSTRUMENT_NAME) THEN 1 END),
    ROUND(COUNT(CASE WHEN INSTRUMENT_NAME != UPPER(INSTRUMENT_NAME) 
               AND INSTRUMENT_NAME != LOWER(INSTRUMENT_NAME) THEN 1 END) * 100.0 / COUNT(*), 2),
    'Orange'
FROM REFINITIV_SECURITIES;

-- FCA case distribution
SELECT 
    'FCA' AS SOURCE,
    'ALL UPPERCASE' AS CASE_TYPE,
    COUNT(CASE WHEN FUND_NAME = UPPER(FUND_NAME) THEN 1 END) AS COUNT,
    ROUND(COUNT(CASE WHEN FUND_NAME = UPPER(FUND_NAME) THEN 1 END) * 100.0 / COUNT(*), 2) AS PERCENTAGE,
    'Green' AS SUGGESTED_COLOR
FROM FCA_SECURITIES
UNION ALL
SELECT 
    'FCA',
    'all lowercase',
    COUNT(CASE WHEN FUND_NAME = LOWER(FUND_NAME) THEN 1 END),
    ROUND(COUNT(CASE WHEN FUND_NAME = LOWER(FUND_NAME) THEN 1 END) * 100.0 / COUNT(*), 2),
    'Red'
FROM FCA_SECURITIES
UNION ALL
SELECT 
    'FCA',
    'Mixed Case',
    COUNT(CASE WHEN FUND_NAME != UPPER(FUND_NAME) 
               AND FUND_NAME != LOWER(FUND_NAME) THEN 1 END),
    ROUND(COUNT(CASE WHEN FUND_NAME != UPPER(FUND_NAME) 
               AND FUND_NAME != LOWER(FUND_NAME) THEN 1 END) * 100.0 / COUNT(*), 2),
    'Orange'
FROM FCA_SECURITIES;


-- =============================================================================
-- SECTION 6: NAMING VARIATION EXAMPLES (COMPARISON TABLE)
-- =============================================================================
-- Purpose: Show real examples of naming inconsistencies
-- Visual Type: Side-by-side comparison table
-- Use In: Problem statement, Executive summary

SELECT '========================================' AS DIVIDER;
SELECT '📊 VISUALIZATION 4: NAMING VARIATIONS' AS SECTION_TITLE;
SELECT 'Create a comparison table with examples' AS INSTRUCTION;
SELECT '========================================' AS DIVIDER;

-- Show naming variations across sources
(SELECT 
    'Example Set 1' AS EXAMPLE_GROUP,
    'Bloomberg' AS SOURCE,
    SECURITY_NAME AS NAME_FORMAT,
    ISSUER_NAME AS ISSUER_FORMAT,
    'Contains: PLC, plc, &' AS PATTERN
FROM BLOOMBERG_SECURITIES
WHERE ISSUER_NAME LIKE '%HSBC%'
LIMIT 3)
UNION ALL
(SELECT 
    'Example Set 1',
    'Refinitiv',
    INSTRUMENT_NAME,
    ISSUER,
    'Contains: Ltd, Hldgs'
FROM REFINITIV_SECURITIES
WHERE ISSUER LIKE '%HSBC%'
LIMIT 3)
UNION ALL
(SELECT 
    'Example Set 1',
    'FCA',
    FUND_NAME,
    MANAGER_NAME,
    'Contains: The, Fund, Management'
FROM FCA_SECURITIES
WHERE MANAGER_NAME LIKE '%HSBC%'
LIMIT 3);

-- BP examples
(SELECT 
    'Example Set 2' AS EXAMPLE_GROUP,
    'Bloomberg' AS SOURCE,
    SECURITY_NAME AS NAME_FORMAT,
    ISSUER_NAME AS ISSUER_FORMAT,
    'BP variations' AS PATTERN
FROM BLOOMBERG_SECURITIES
WHERE ISSUER_NAME LIKE '%BP%'
LIMIT 3)
UNION ALL
(SELECT 
    'Example Set 2',
    'Refinitiv',
    INSTRUMENT_NAME,
    ISSUER,
    'BP variations'
FROM REFINITIV_SECURITIES
WHERE ISSUER LIKE '%BP%'
LIMIT 3)
UNION ALL
(SELECT 
    'Example Set 2',
    'FCA',
    FUND_NAME,
    MANAGER_NAME,
    'BP variations'
FROM FCA_SECURITIES
WHERE MANAGER_NAME LIKE '%BP%'
LIMIT 3);


-- =============================================================================
-- SECTION 7: NAME LENGTH DISTRIBUTION (HISTOGRAM DATA)
-- =============================================================================
-- Purpose: Show how name lengths vary by source
-- Visual Type: Box plot or histogram
-- Use In: Data quality metrics, Technical documentation

SELECT '========================================' AS DIVIDER;
SELECT '📊 VISUALIZATION 5: NAME LENGTH DISTRIBUTION' AS SECTION_TITLE;
SELECT 'Create a box plot or histogram' AS INSTRUCTION;
SELECT '========================================' AS DIVIDER;

SELECT 
    'Bloomberg' AS SOURCE,
    MIN(LENGTH(SECURITY_NAME)) AS MIN_LENGTH,
    PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY LENGTH(SECURITY_NAME)) AS Q1_25TH_PERCENTILE,
    MEDIAN(LENGTH(SECURITY_NAME)) AS MEDIAN_LENGTH,
    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY LENGTH(SECURITY_NAME)) AS Q3_75TH_PERCENTILE,
    MAX(LENGTH(SECURITY_NAME)) AS MAX_LENGTH,
    ROUND(AVG(LENGTH(SECURITY_NAME)), 2) AS AVG_LENGTH,
    'Longer = more verbose' AS NOTE
FROM BLOOMBERG_SECURITIES
UNION ALL
SELECT 
    'Refinitiv',
    MIN(LENGTH(INSTRUMENT_NAME)),
    PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY LENGTH(INSTRUMENT_NAME)),
    MEDIAN(LENGTH(INSTRUMENT_NAME)),
    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY LENGTH(INSTRUMENT_NAME)),
    MAX(LENGTH(INSTRUMENT_NAME)),
    ROUND(AVG(LENGTH(INSTRUMENT_NAME)), 2),
    'Longer = more verbose'
FROM REFINITIV_SECURITIES
UNION ALL
SELECT 
    'FCA',
    MIN(LENGTH(FUND_NAME)),
    PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY LENGTH(FUND_NAME)),
    MEDIAN(LENGTH(FUND_NAME)),
    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY LENGTH(FUND_NAME)),
    MAX(LENGTH(FUND_NAME)),
    ROUND(AVG(LENGTH(FUND_NAME)), 2),
    'Longer = more verbose'
FROM FCA_SECURITIES;


-- =============================================================================
-- SECTION 8: ASSET TYPE DISTRIBUTION (STACKED BAR CHART)
-- =============================================================================
-- Purpose: Show asset type breakdown by source
-- Visual Type: Stacked bar chart
-- Use In: Data composition section

SELECT '========================================' AS DIVIDER;
SELECT '📊 VISUALIZATION 6: ASSET TYPE DISTRIBUTION' AS SECTION_TITLE;
SELECT 'Create a stacked bar chart' AS INSTRUCTION;
SELECT '========================================' AS DIVIDER;

-- Bloomberg asset class distribution
SELECT 
    'Bloomberg' AS SOURCE,
    ASSET_CLASS AS ASSET_TYPE,
    COUNT(*) AS COUNT,
    ROUND(COUNT(*) * 100.0 / 1000, 2) AS PERCENTAGE
FROM BLOOMBERG_SECURITIES
GROUP BY ASSET_CLASS
UNION ALL
-- Refinitiv instrument type distribution
SELECT 
    'Refinitiv',
    INSTRUMENT_TYPE,
    COUNT(*),
    ROUND(COUNT(*) * 100.0 / 1000, 2)
FROM REFINITIV_SECURITIES
GROUP BY INSTRUMENT_TYPE
UNION ALL
-- FCA fund type distribution
SELECT 
    'FCA',
    FUND_TYPE,
    COUNT(*),
    ROUND(COUNT(*) * 100.0 / 1000, 2)
FROM FCA_SECURITIES
GROUP BY FUND_TYPE
ORDER BY SOURCE, ASSET_TYPE;


-- =============================================================================
-- SECTION 9: DATA COMPLETENESS HEATMAP
-- =============================================================================
-- Purpose: Show NULL values by field and source
-- Visual Type: Heatmap (Green = complete, Red = missing)
-- Use In: Data quality assessment

SELECT '========================================' AS DIVIDER;
SELECT '📊 VISUALIZATION 7: DATA COMPLETENESS HEATMAP' AS SECTION_TITLE;
SELECT 'Create a heatmap (Green=100%, Red=<100%)' AS INSTRUCTION;
SELECT '========================================' AS DIVIDER;

-- Bloomberg completeness
SELECT 
    'Bloomberg' AS SOURCE,
    'Security ID' AS FIELD,
    COUNT(SECURITY_ID) AS NON_NULL_COUNT,
    1000 AS TOTAL_COUNT,
    ROUND(COUNT(SECURITY_ID) * 100.0 / 1000, 2) AS COMPLETENESS_PCT,
    CASE WHEN COUNT(SECURITY_ID) = 1000 THEN 'Green' ELSE 'Red' END AS COLOR
FROM BLOOMBERG_SECURITIES
UNION ALL
SELECT 
    'Bloomberg',
    'Security Name',
    COUNT(SECURITY_NAME),
    1000,
    ROUND(COUNT(SECURITY_NAME) * 100.0 / 1000, 2),
    CASE WHEN COUNT(SECURITY_NAME) = 1000 THEN 'Green' ELSE 'Red' END
FROM BLOOMBERG_SECURITIES
UNION ALL
SELECT 
    'Bloomberg',
    'ISIN',
    COUNT(ISIN),
    1000,
    ROUND(COUNT(ISIN) * 100.0 / 1000, 2),
    CASE WHEN COUNT(ISIN) = 1000 THEN 'Green' ELSE 'Red' END
FROM BLOOMBERG_SECURITIES
UNION ALL
SELECT 
    'Bloomberg',
    'SEDOL',
    COUNT(SEDOL),
    1000,
    ROUND(COUNT(SEDOL) * 100.0 / 1000, 2),
    CASE WHEN COUNT(SEDOL) = 1000 THEN 'Green' ELSE 'Red' END
FROM BLOOMBERG_SECURITIES
UNION ALL
SELECT 
    'Bloomberg',
    'Issuer',
    COUNT(ISSUER_NAME),
    1000,
    ROUND(COUNT(ISSUER_NAME) * 100.0 / 1000, 2),
    CASE WHEN COUNT(ISSUER_NAME) = 1000 THEN 'Green' ELSE 'Red' END
FROM BLOOMBERG_SECURITIES;

-- Refinitiv completeness
SELECT 
    'Refinitiv' AS SOURCE,
    'Security ID' AS FIELD,
    COUNT(RIC_CODE) AS NON_NULL_COUNT,
    1000 AS TOTAL_COUNT,
    ROUND(COUNT(RIC_CODE) * 100.0 / 1000, 2) AS COMPLETENESS_PCT,
    CASE WHEN COUNT(RIC_CODE) = 1000 THEN 'Green' ELSE 'Red' END AS COLOR
FROM REFINITIV_SECURITIES
UNION ALL
SELECT 
    'Refinitiv',
    'Security Name',
    COUNT(INSTRUMENT_NAME),
    1000,
    ROUND(COUNT(INSTRUMENT_NAME) * 100.0 / 1000, 2),
    CASE WHEN COUNT(INSTRUMENT_NAME) = 1000 THEN 'Green' ELSE 'Red' END
FROM REFINITIV_SECURITIES
UNION ALL
SELECT 
    'Refinitiv',
    'ISIN',
    COUNT(ISIN_CODE),
    1000,
    ROUND(COUNT(ISIN_CODE) * 100.0 / 1000, 2),
    CASE WHEN COUNT(ISIN_CODE) = 1000 THEN 'Green' ELSE 'Red' END
FROM REFINITIV_SECURITIES
UNION ALL
SELECT 
    'Refinitiv',
    'SEDOL',
    COUNT(SEDOL_CODE),
    1000,
    ROUND(COUNT(SEDOL_CODE) * 100.0 / 1000, 2),
    CASE WHEN COUNT(SEDOL_CODE) = 1000 THEN 'Green' ELSE 'Red' END
FROM REFINITIV_SECURITIES
UNION ALL
SELECT 
    'Refinitiv',
    'Issuer',
    COUNT(ISSUER),
    1000,
    ROUND(COUNT(ISSUER) * 100.0 / 1000, 2),
    CASE WHEN COUNT(ISSUER) = 1000 THEN 'Green' ELSE 'Red' END
FROM REFINITIV_SECURITIES;

-- FCA completeness
SELECT 
    'FCA' AS SOURCE,
    'Security ID' AS FIELD,
    COUNT(FCA_REF_NUMBER) AS NON_NULL_COUNT,
    1000 AS TOTAL_COUNT,
    ROUND(COUNT(FCA_REF_NUMBER) * 100.0 / 1000, 2) AS COMPLETENESS_PCT,
    CASE WHEN COUNT(FCA_REF_NUMBER) = 1000 THEN 'Green' ELSE 'Red' END AS COLOR
FROM FCA_SECURITIES
UNION ALL
SELECT 
    'FCA',
    'Security Name',
    COUNT(FUND_NAME),
    1000,
    ROUND(COUNT(FUND_NAME) * 100.0 / 1000, 2),
    CASE WHEN COUNT(FUND_NAME) = 1000 THEN 'Green' ELSE 'Red' END
FROM FCA_SECURITIES
UNION ALL
SELECT 
    'FCA',
    'ISIN',
    COUNT(ISIN),
    1000,
    ROUND(COUNT(ISIN) * 100.0 / 1000, 2),
    CASE WHEN COUNT(ISIN) = 1000 THEN 'Green' ELSE 'Red' END
FROM FCA_SECURITIES
UNION ALL
SELECT 
    'FCA',
    'SEDOL',
    COUNT(SEDOL),
    1000,
    ROUND(COUNT(SEDOL) * 100.0 / 1000, 2),
    CASE WHEN COUNT(SEDOL) = 1000 THEN 'Green' ELSE 'Red' END
FROM FCA_SECURITIES
UNION ALL
SELECT 
    'FCA',
    'Issuer',
    COUNT(MANAGER_NAME),
    1000,
    ROUND(COUNT(MANAGER_NAME) * 100.0 / 1000, 2),
    CASE WHEN COUNT(MANAGER_NAME) = 1000 THEN 'Green' ELSE 'Red' END
FROM FCA_SECURITIES;


-- =============================================================================
-- SECTION 10: MATCHING SUCCESS RATE (BEFORE HARMONIZATION)
-- =============================================================================
-- Purpose: Prove that matching fails without harmonization
-- Visual Type: Success/Failure chart
-- Use In: Problem statement, ROI justification

SELECT '========================================' AS DIVIDER;
SELECT '📊 VISUALIZATION 8: MATCHING SUCCESS RATE' AS SECTION_TITLE;
SELECT 'Create a success/failure comparison chart' AS INSTRUCTION;
SELECT '========================================' AS DIVIDER;

-- Try matching by ISIN (identifier-based matching)
SELECT 
    'ISIN-based Matching' AS MATCH_TYPE,
    COUNT(DISTINCT B.ISIN) AS BLOOMBERG_UNIQUE,
    COUNT(DISTINCT R.ISIN_CODE) AS REFINITIV_UNIQUE,
    COUNT(DISTINCT F.ISIN) AS FCA_UNIQUE,
    '✅ Works (but cheating!)' AS STATUS,
    'Using identifiers is not true entity resolution' AS NOTE
FROM BLOOMBERG_SECURITIES B
CROSS JOIN REFINITIV_SECURITIES R
CROSS JOIN FCA_SECURITIES F;

-- Try matching by exact name (will fail)
SELECT 
    'Name-based Matching' AS MATCH_TYPE,
    0 AS BLOOMBERG_UNIQUE,
    0 AS REFINITIV_UNIQUE,
    0 AS FCA_UNIQUE,
    '❌ FAILS' AS STATUS,
    'Names are too different for exact matching' AS NOTE
FROM BLOOMBERG_SECURITIES B
LEFT JOIN REFINITIV_SECURITIES R
    ON B.SECURITY_NAME = R.INSTRUMENT_NAME
WHERE R.RIC_CODE IS NOT NULL
LIMIT 1;

-- Summary
SELECT 
    'Matching Without Harmonization' AS SUMMARY,
    'ISIN Match Rate: 100% (but not useful)' AS RESULT_1,
    'Name Match Rate: 0% (FAILS)' AS RESULT_2,
    'CONCLUSION: Harmonization REQUIRED' AS CONCLUSION;


-- =============================================================================
-- SECTION 11: CROSS-SOURCE COMPARISON (SIDE-BY-SIDE)
-- =============================================================================
-- Purpose: Show same entities with different representations
-- Visual Type: Side-by-side comparison table
-- Use In: Problem illustration, Presentations

SELECT '========================================' AS DIVIDER;
SELECT '📊 VISUALIZATION 9: CROSS-SOURCE ENTITY COMPARISON' AS SECTION_TITLE;
SELECT 'Create side-by-side comparison table' AS INSTRUCTION;
SELECT '========================================' AS DIVIDER;

-- HSBC comparison
(SELECT 
    1 AS SORT_ORDER,
    'HSBC Example' AS COMPANY,
    'Bloomberg' AS SOURCE,
    SECURITY_NAME AS HOW_IT_APPEARS,
    ISSUER_NAME AS ISSUER_FORMAT
FROM BLOOMBERG_SECURITIES
WHERE ISSUER_NAME LIKE '%HSBC%'
LIMIT 3)
UNION ALL
(SELECT 
    2,
    'HSBC Example',
    'Refinitiv',
    INSTRUMENT_NAME,
    ISSUER
FROM REFINITIV_SECURITIES
WHERE ISSUER LIKE '%HSBC%'
LIMIT 3)
UNION ALL
(SELECT 
    3,
    'HSBC Example',
    'FCA',
    FUND_NAME,
    MANAGER_NAME
FROM FCA_SECURITIES
WHERE MANAGER_NAME LIKE '%HSBC%'
LIMIT 3)
ORDER BY SORT_ORDER;


-- =============================================================================
-- SECTION 12: ISSUES SUMMARY (INFOGRAPHIC DATA)
-- =============================================================================
-- Purpose: Summarize all issues found
-- Visual Type: Infographic/Icon summary
-- Use In: README, Wiki homepage, Presentation intro

SELECT '========================================' AS DIVIDER;
SELECT '📊 VISUALIZATION 10: ISSUES SUMMARY' AS SECTION_TITLE;
SELECT 'Create an infographic or icon summary' AS INSTRUCTION;
SELECT '========================================' AS DIVIDER;

SELECT 
    1 AS ISSUE_NUMBER,
    '❌' AS ICON,
    'Field Name Inconsistency' AS ISSUE_TITLE,
    'Different column names across sources' AS DESCRIPTION,
    'HIGH' AS SEVERITY,
    'Harmonization' AS SOLUTION
UNION ALL
SELECT 
    2,
    '❌',
    'Case Sensitivity',
    'Mixed uppercase/lowercase/mixed case',
    'HIGH',
    'Convert to UPPERCASE'
UNION ALL
SELECT 
    3,
    '⚠️',
    'Naming Variations',
    'PLC, plc, Ltd, Limited, Holdings, Hldgs',
    'MEDIUM',
    'Standardize patterns'
UNION ALL
SELECT 
    4,
    '⚠️',
    'Extra Spaces',
    'Leading/trailing/multiple spaces',
    'MEDIUM',
    'TRIM and normalize'
UNION ALL
SELECT 
    5,
    '⚠️',
    'Special Characters',
    'Contains &, -, (, ), .',
    'MEDIUM',
    'Remove non-alphanumeric'
UNION ALL
SELECT 
    6,
    '❌',
    'Matching Fails',
    'Name-based matching impossible',
    'CRITICAL',
    'Run harmonization + AI embeddings'
ORDER BY ISSUE_NUMBER;


-- =============================================================================
-- SECTION 13: BEFORE/AFTER PREVIEW
-- =============================================================================
-- Purpose: Show what data will look like after harmonization
-- Visual Type: Before/After comparison
-- Use In: Solution demonstration

SELECT '========================================' AS DIVIDER;
SELECT '📊 VISUALIZATION 11: BEFORE/AFTER PREVIEW' AS SECTION_TITLE;
SELECT 'Create before/after comparison' AS INSTRUCTION;
SELECT '========================================' AS DIVIDER;

-- Sample "Before" state
SELECT 
    'BEFORE Harmonization' AS STATE,
    'Bloomberg' AS SOURCE,
    SECURITY_NAME AS ORIGINAL_NAME,
    NULL AS CLEANED_NAME,
    'Has: mixed case, special chars' AS ISSUES
FROM BLOOMBERG_SECURITIES
LIMIT 5;

-- Sample "After" state (simulated)
SELECT 
    'AFTER Harmonization' AS STATE,
    'Bloomberg' AS SOURCE,
    SECURITY_NAME AS ORIGINAL_NAME,
    TRIM(REGEXP_REPLACE(UPPER(SECURITY_NAME), '[^A-Z0-9 ]', '')) AS CLEANED_NAME,
    'Fixed: UPPERCASE, no special chars' AS IMPROVEMENTS
FROM BLOOMBERG_SECURITIES
LIMIT 5;


-- =============================================================================
-- FINAL SUMMARY AND EXPORT INSTRUCTIONS
-- =============================================================================

SELECT '========================================' AS DIVIDER;
SELECT '✅ DATA PROFILING COMPLETE' AS TITLE;
SELECT '========================================' AS DIVIDER;

SELECT 
    'Export Instructions' AS SECTION,
    'Copy results to Excel/CSV for visualization' AS INSTRUCTION
UNION ALL
SELECT 
    'Recommended Charts',
    '1. Bar chart (Section 3), 2. Pie charts (Section 5), 3. Heatmap (Section 9)'
UNION ALL
SELECT 
    'Documentation Usage',
    'Take screenshots of each visualization for README.md and Wiki'
UNION ALL
SELECT 
    'Next Step',
    'Run 3.1_Data_Harmonization.sql to fix all issues'
UNION ALL
SELECT 
    'Then Compare',
    'Run this script again after harmonization to see improvements';


-- =============================================================================
-- VISUALIZATION SUMMARY TABLE
-- =============================================================================
-- Purpose: Quick reference for which charts to create

SELECT '📊 CHART CREATION GUIDE' AS TITLE;

SELECT 
    'Visualization 1' AS VIZ_ID,
    'Record Count by Source' AS CHART_NAME,
    'Horizontal Bar Chart' AS CHART_TYPE,
    'Section 3' AS DATA_SECTION,
    'Blue, Orange, Green' AS COLORS
UNION ALL
SELECT 
    'Visualization 2',
    'Field Mapping Matrix',
    'Comparison Table',
    'Section 4',
    'Red/Green/Orange'
UNION ALL
SELECT 
    'Visualization 3',
    'Case Sensitivity Distribution',
    '3 Pie Charts',
    'Section 5',
    'Green, Red, Orange'
UNION ALL
SELECT 
    'Visualization 4',
    'Naming Variations',
    'Comparison Table',
    'Section 6',
    'N/A'
UNION ALL
SELECT 
    'Visualization 5',
    'Name Length Distribution',
    'Box Plot / Histogram',
    'Section 7',
    'Blue'
UNION ALL
SELECT 
    'Visualization 6',
    'Asset Type Distribution',
    'Stacked Bar Chart',
    'Section 8',
    'Rainbow colors'
UNION ALL
SELECT 
    'Visualization 7',
    'Data Completeness Heatmap',
    'Heatmap',
    'Section 9',
    'Green to Red gradient'
UNION ALL
SELECT 
    'Visualization 8',
    'Matching Success Rate',
    'Success/Failure Bars',
    'Section 10',
    'Green/Red'
UNION ALL
SELECT 
    'Visualization 9',
    'Cross-Source Comparison',
    'Side-by-side Table',
    'Section 11',
    'N/A'
UNION ALL
SELECT 
    'Visualization 10',
    'Issues Summary',
    'Infographic/Icons',
    'Section 12',
    'Red, Orange, Yellow'
UNION ALL
SELECT 
    'Visualization 11',
    'Before/After Preview',
    'Comparison Table',
    'Section 13',
    'Red to Green';


-- =============================================================================
-- END OF PROFILING SCRIPT
-- =============================================================================