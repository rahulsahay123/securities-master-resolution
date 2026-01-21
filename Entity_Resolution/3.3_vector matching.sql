/*==============================================================================
  Project: Securities Master Resolution Platform for UK Asset Management
  Phase: 3.3 - Vector Similarity Matching (FIXED - No Reserved Keywords)
  File: 3.3_Vector_Matching.sql
  Author: Rahul Sahay
  GitHub: https://github.com/rahulsahay123/securities-master-resolution
  Date: January 2026
  
  Description:
  This script performs vector similarity matching across all securities using
  the embeddings generated in Phase 3.2. It calculates cosine similarity scores
  and identifies high-confidence matches (>80% similarity) between securities
  from different data sources.
    
  Prerequisites:
  - Phase 3.2 (3.2_Embedding_Generation.sql) completed successfully
  - SECURITY_EMBEDDINGS table populated with 3000+ embeddings
  - All embeddings are 768-dimensional vectors
  
  Matching Strategy:
  - Compare embeddings across different sources (Bloomberg vs Refinitiv vs FCA)
  - Calculate cosine similarity (0.0 to 1.0 scale)
  - Threshold: ≥0.80 = High confidence match
  - Threshold: 0.60-0.79 = Medium confidence (for AI validation)
  - Below 0.60 = No match
  
  Usage:
  Run all sections sequentially in Snowflake Snowsight interface
  
  Sections:
  1. Environment Setup & Verification
  2. Understanding Vector Similarity
  3. Cross-Source Similarity Calculation
  4. Generate High-Confidence Matches (≥80%)
  5. Match Quality Verification
  6. Match Statistics & Analysis
  7. Visualization Data Export
==============================================================================*/

-- =============================================================================
-- SECTION 1: ENVIRONMENT SETUP & VERIFICATION
-- =============================================================================

USE WAREHOUSE SECURITIES_WH;
USE DATABASE SECURITIES_MASTER;
USE SCHEMA ENTITY_RESOLUTION;

-- Verify we're in the correct context
SELECT CURRENT_WAREHOUSE() AS WAREHOUSE,
       CURRENT_DATABASE() AS DATABASE_NAME,
       CURRENT_SCHEMA() AS SCHEMA_NAME;

-- Check that embeddings exist
SELECT 
    'Embeddings Check' AS VALIDATION,
    COUNT(*) AS EMBEDDING_COUNT,
    CASE 
        WHEN COUNT(*) >= 3000 THEN '✅ READY FOR MATCHING' 
        ELSE '❌ Run 3.2 First!' 
    END AS STATUS_VALUE
FROM SECURITY_EMBEDDINGS;

-- Check embeddings by source
SELECT 
    'Embeddings by Source' AS REPORT_TYPE,
    H.ORIGINAL_SOURCE AS SOURCE_NAME,
    COUNT(*) AS RECORD_COUNT
FROM SECURITY_EMBEDDINGS E
JOIN HARMONIZED_SECURITIES H ON E.HARMONIZED_ID = H.HARMONIZED_ID
GROUP BY H.ORIGINAL_SOURCE
ORDER BY H.ORIGINAL_SOURCE;

SELECT '========================================' AS DIVIDER;
SELECT '🔍 VECTOR SIMILARITY MATCHING STARTING' AS TITLE;
SELECT '========================================' AS DIVIDER;


-- =============================================================================
-- SECTION 2: UNDERSTANDING VECTOR SIMILARITY
-- =============================================================================
-- Purpose: Demonstrate how cosine similarity works with examples

SELECT '========================================' AS DIVIDER;
SELECT '📚 UNDERSTANDING COSINE SIMILARITY' AS SECTION_TITLE;
SELECT 'Learn how matching works' AS INSTRUCTION;
SELECT '========================================' AS DIVIDER;

-- Example 1: Same source comparison (should be lower similarity)
SELECT 
    'Example 1: Within Same Source' AS EXAMPLE_TYPE,
    E1.SECURITY_DESCRIPTION AS SECURITY_1,
    E2.SECURITY_DESCRIPTION AS SECURITY_2,
    ROUND(VECTOR_COSINE_SIMILARITY(E1.EMBEDDING_VECTOR, E2.EMBEDDING_VECTOR), 4) AS SIMILARITY_SCORE,
    'Lower similarity expected (different companies)' AS EXPECTATION
FROM SECURITY_EMBEDDINGS E1
JOIN HARMONIZED_SECURITIES H1 ON E1.HARMONIZED_ID = H1.HARMONIZED_ID
CROSS JOIN SECURITY_EMBEDDINGS E2
JOIN HARMONIZED_SECURITIES H2 ON E2.HARMONIZED_ID = H2.HARMONIZED_ID
WHERE H1.ORIGINAL_SOURCE = 'BLOOMBERG'
  AND H2.ORIGINAL_SOURCE = 'BLOOMBERG'
  AND E1.EMBEDDING_ID != E2.EMBEDDING_ID
  AND H1.ISSUER_CLEAN LIKE '%HSBC%'
  AND H2.ISSUER_CLEAN LIKE '%BP%'
LIMIT 3;

-- Example 2: Cross-source comparison (should be higher similarity)
SELECT 
    'Example 2: Across Sources (Same Company)' AS EXAMPLE_TYPE,
    E1.SECURITY_DESCRIPTION AS BLOOMBERG_SECURITY,
    E2.SECURITY_DESCRIPTION AS REFINITIV_SECURITY,
    ROUND(VECTOR_COSINE_SIMILARITY(E1.EMBEDDING_VECTOR, E2.EMBEDDING_VECTOR), 4) AS SIMILARITY_SCORE,
    'Higher similarity expected (same company, different source)' AS EXPECTATION
FROM SECURITY_EMBEDDINGS E1
JOIN HARMONIZED_SECURITIES H1 ON E1.HARMONIZED_ID = H1.HARMONIZED_ID
CROSS JOIN SECURITY_EMBEDDINGS E2
JOIN HARMONIZED_SECURITIES H2 ON E2.HARMONIZED_ID = H2.HARMONIZED_ID
WHERE H1.ORIGINAL_SOURCE = 'BLOOMBERG'
  AND H2.ORIGINAL_SOURCE = 'REFINITIV'
  AND H1.ISSUER_CLEAN LIKE '%HSBC%'
  AND H2.ISSUER_CLEAN LIKE '%HSBC%'
LIMIT 3;

-- Example 3: Show similarity score distribution
SELECT 
    'Example 3: Similarity Score Ranges' AS EXAMPLE_TYPE,
    CASE 
        WHEN ROUND(VECTOR_COSINE_SIMILARITY(E1.EMBEDDING_VECTOR, E2.EMBEDDING_VECTOR), 2) >= 0.90 
        THEN '0.90-1.00 (Excellent Match)'
        WHEN ROUND(VECTOR_COSINE_SIMILARITY(E1.EMBEDDING_VECTOR, E2.EMBEDDING_VECTOR), 2) >= 0.80 
        THEN '0.80-0.89 (High Confidence)'
        WHEN ROUND(VECTOR_COSINE_SIMILARITY(E1.EMBEDDING_VECTOR, E2.EMBEDDING_VECTOR), 2) >= 0.70 
        THEN '0.70-0.79 (Medium Confidence)'
        WHEN ROUND(VECTOR_COSINE_SIMILARITY(E1.EMBEDDING_VECTOR, E2.EMBEDDING_VECTOR), 2) >= 0.60 
        THEN '0.60-0.69 (Low Confidence)'
        ELSE '< 0.60 (No Match)'
    END AS SCORE_RANGE,
    COUNT(*) AS PAIR_COUNT,
    'Distribution of similarity scores' AS DESC_TEXT
FROM SECURITY_EMBEDDINGS E1
CROSS JOIN SECURITY_EMBEDDINGS E2
WHERE E1.EMBEDDING_ID < E2.EMBEDDING_ID  -- Avoid duplicates
GROUP BY SCORE_RANGE
ORDER BY SCORE_RANGE DESC LIMIT 10000;


-- =============================================================================
-- SECTION 3: CROSS-SOURCE SIMILARITY CALCULATION
-- =============================================================================
-- Purpose: Calculate similarities between ALL cross-source pairs
-- Note: This creates a Cartesian product - will take 2-3 minutes

SELECT '========================================' AS DIVIDER;
SELECT '⚙️ CALCULATING CROSS-SOURCE SIMILARITIES' AS SECTION_TITLE;
SELECT 'This may take 2-3 minutes...' AS INSTRUCTION;
SELECT '========================================' AS DIVIDER;

-- Create a temporary table with all cross-source similarities
-- This avoids recalculating during subsequent queries
CREATE OR REPLACE TEMPORARY TABLE TEMP_SIMILARITIES AS
SELECT 
    -- Source 1 info
    H1.HARMONIZED_ID AS HARMONIZED_ID_1,
    H1.ORIGINAL_SOURCE AS SOURCE_NAME_1,
    H1.ORIGINAL_ID AS ORIGINAL_ID_1,
    E1.SECURITY_DESCRIPTION AS DESCRIPTION_1,
    H1.SECURITY_NAME_CLEAN AS NAME_1,
    H1.ISSUER_CLEAN AS ISSUER_1,
    H1.ASSET_TYPE AS ASSET_TYPE_1,
    
    -- Source 2 info
    H2.HARMONIZED_ID AS HARMONIZED_ID_2,
    H2.ORIGINAL_SOURCE AS SOURCE_NAME_2,
    H2.ORIGINAL_ID AS ORIGINAL_ID_2,
    E2.SECURITY_DESCRIPTION AS DESCRIPTION_2,
    H2.SECURITY_NAME_CLEAN AS NAME_2,
    H2.ISSUER_CLEAN AS ISSUER_2,
    H2.ASSET_TYPE AS ASSET_TYPE_2,
    
    -- Similarity score
    ROUND(VECTOR_COSINE_SIMILARITY(E1.EMBEDDING_VECTOR, E2.EMBEDDING_VECTOR), 4) AS SIMILARITY_SCORE
    
FROM SECURITY_EMBEDDINGS E1
JOIN HARMONIZED_SECURITIES H1 ON E1.HARMONIZED_ID = H1.HARMONIZED_ID
CROSS JOIN SECURITY_EMBEDDINGS E2
JOIN HARMONIZED_SECURITIES H2 ON E2.HARMONIZED_ID = H2.HARMONIZED_ID

WHERE 
    -- Only cross-source comparisons (not same source)
    H1.ORIGINAL_SOURCE < H2.ORIGINAL_SOURCE  -- Ensures no duplicates and different sources
    
    -- Optional: Filter by asset type match (securities of same type)
    AND H1.ASSET_TYPE = H2.ASSET_TYPE;

-- Verify similarity calculation completed
SELECT 
    'Similarity Calculation Complete' AS STATUS_MSG,
    COUNT(*) AS TOTAL_COMPARISONS,
    '✅ Ready for matching' AS RESULT_MSG
FROM TEMP_SIMILARITIES;

-- Show sample of calculated similarities
SELECT 
    'Sample Similarities' AS REPORT_TYPE,
    SOURCE_NAME_1,
    SOURCE_NAME_2,
    DESCRIPTION_1,
    DESCRIPTION_2,
    SIMILARITY_SCORE
FROM TEMP_SIMILARITIES
ORDER BY SIMILARITY_SCORE DESC
LIMIT 20;


-- =============================================================================
-- SECTION 4: GENERATE HIGH-CONFIDENCE MATCHES (≥80%)
-- =============================================================================
-- Purpose: Create matches for securities with ≥80% similarity
-- This populates the MATCHED_SECURITIES table

SELECT '========================================' AS DIVIDER;
SELECT '🎯 GENERATING HIGH-CONFIDENCE MATCHES' AS SECTION_TITLE;
SELECT 'Threshold: ≥80% similarity' AS INSTRUCTION;
SELECT '========================================' AS DIVIDER;

-- Clear any existing matches (fresh start)
TRUNCATE TABLE IF EXISTS MATCHED_SECURITIES;

-- Insert high-confidence matches (≥0.80 similarity)
INSERT INTO MATCHED_SECURITIES (
    MATCH_ID,
    SOURCE_1,
    ID_1,
    SOURCE_2,
    ID_2,
    SIMILARITY_SCORE,
    MATCH_METHOD,
    MATCH_STATUS,
    CREATED_AT
)
SELECT 
    -- Create unique match ID
    'MATCH_' || ROW_NUMBER() OVER (ORDER BY SIMILARITY_SCORE DESC) AS MATCH_ID,
    
    -- Source 1 (alphabetically first)
    SOURCE_NAME_1 AS SOURCE_1,
    ORIGINAL_ID_1 AS ID_1,
    
    -- Source 2 (alphabetically second)
    SOURCE_NAME_2 AS SOURCE_2,
    ORIGINAL_ID_2 AS ID_2,
    
    -- Similarity score
    SIMILARITY_SCORE,
    
    -- Match method
    'VECTOR' AS MATCH_METHOD,
    
    -- Status (high confidence = auto-approved)
    CASE 
        WHEN SIMILARITY_SCORE >= 0.90 THEN 'APPROVED'
        WHEN SIMILARITY_SCORE >= 0.80 THEN 'PENDING'  -- Will be validated by AI in Phase 3.4
        ELSE 'REJECTED'
    END AS MATCH_STATUS,
    
    -- Timestamp
    CURRENT_TIMESTAMP() AS CREATED_AT
    
FROM TEMP_SIMILARITIES
WHERE SIMILARITY_SCORE >= 0.80  -- High confidence threshold
ORDER BY SIMILARITY_SCORE DESC;

-- Verify matches were created
SELECT 
    'Match Generation Complete' AS STATUS_MSG,
    COUNT(*) AS MATCHES_CREATED,
    CASE 
        WHEN COUNT(*) > 0 THEN '✅ SUCCESS' 
        ELSE '⚠️ No matches found (threshold may be too high)' 
    END AS RESULT_MSG
FROM MATCHED_SECURITIES;


-- =============================================================================
-- SECTION 5: MATCH QUALITY VERIFICATION
-- =============================================================================
-- Purpose: Comprehensive checks that matches are valid and useful

SELECT '========================================' AS DIVIDER;
SELECT '✅ MATCH QUALITY VERIFICATION' AS SECTION_TITLE;
SELECT '========================================' AS DIVIDER;

-- Check 1: Total match count
SELECT 
    'Check 1: Total Matches' AS CHECK_NAME,
    COUNT(*) AS MATCH_COUNT,
    CASE 
        WHEN COUNT(*) > 0 THEN '✅ PASS' 
        ELSE '❌ FAIL - No matches found' 
    END AS STATUS_VALUE
FROM MATCHED_SECURITIES;

-- Check 2: Matches by source pair
SELECT 
    'Check 2: Matches by Source Pair' AS CHECK_NAME,
    SOURCE_1 AS SOURCE_NAME_1,
    SOURCE_2 AS SOURCE_NAME_2,
    COUNT(*) AS MATCH_COUNT,
    ROUND(AVG(SIMILARITY_SCORE), 4) AS AVG_SIMILARITY,
    MIN(SIMILARITY_SCORE) AS MIN_SIMILARITY,
    MAX(SIMILARITY_SCORE) AS MAX_SIMILARITY
FROM MATCHED_SECURITIES
GROUP BY SOURCE_1, SOURCE_2
ORDER BY SOURCE_1, SOURCE_2;

-- Check 3: Match status distribution
SELECT 
    'Check 3: Match Status Distribution' AS CHECK_NAME,
    MATCH_STATUS AS STATUS_VALUE,
    COUNT(*) AS RECORD_COUNT,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS PERCENTAGE
FROM MATCHED_SECURITIES
GROUP BY MATCH_STATUS
ORDER BY RECORD_COUNT DESC;

-- Check 4: Similarity score distribution
SELECT 
    'Check 4: Similarity Score Distribution' AS CHECK_NAME,
    CASE 
        WHEN SIMILARITY_SCORE >= 0.95 THEN '0.95-1.00 (Excellent)'
        WHEN SIMILARITY_SCORE >= 0.90 THEN '0.90-0.94 (Very High)'
        WHEN SIMILARITY_SCORE >= 0.85 THEN '0.85-0.89 (High)'
        WHEN SIMILARITY_SCORE >= 0.80 THEN '0.80-0.84 (Good)'
        ELSE '< 0.80 (Should not exist!)'
    END AS SCORE_RANGE,
    COUNT(*) AS RECORD_COUNT,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS PERCENTAGE
FROM MATCHED_SECURITIES
GROUP BY SCORE_RANGE
ORDER BY SCORE_RANGE DESC;

-- Check 5: No self-matches (same source)
SELECT 
    'Check 5: Self-Match Check' AS CHECK_NAME,
    COUNT(*) AS SELF_MATCHES,
    CASE 
        WHEN COUNT(*) = 0 THEN '✅ PASS - No self-matches' 
        ELSE '❌ FAIL - Self-matches found' 
    END AS STATUS_VALUE
FROM MATCHED_SECURITIES
WHERE SOURCE_1 = SOURCE_2;

-- Check 6: No duplicate matches
SELECT 
    'Check 6: Duplicate Match Check' AS CHECK_NAME,
    COUNT(*) AS DUPLICATE_COUNT,
    CASE 
        WHEN COUNT(*) = 0 THEN '✅ PASS - No duplicates' 
        ELSE '⚠️ WARNING - Duplicates found' 
    END AS STATUS_VALUE
FROM (
    SELECT SOURCE_1, ID_1, SOURCE_2, ID_2, COUNT(*) AS cnt
    FROM MATCHED_SECURITIES
    GROUP BY SOURCE_1, ID_1, SOURCE_2, ID_2
    HAVING COUNT(*) > 1
);

-- Check 7: Sample of best matches
SELECT 
    'Check 7: Top 10 Best Matches' AS CHECK_NAME,
    MATCH_ID,
    SOURCE_1 || ': ' || ID_1 AS SECURITY_1,
    SOURCE_2 || ': ' || ID_2 AS SECURITY_2,
    SIMILARITY_SCORE,
    MATCH_STATUS AS STATUS_VALUE
FROM MATCHED_SECURITIES
ORDER BY SIMILARITY_SCORE DESC
LIMIT 10;


-- =============================================================================
-- SECTION 6: MATCH STATISTICS & ANALYSIS
-- =============================================================================
-- Purpose: Detailed analysis of matching results

SELECT '========================================' AS DIVIDER;
SELECT '📊 MATCH STATISTICS & ANALYSIS' AS SECTION_TITLE;
SELECT '========================================' AS DIVIDER;

-- Analysis 1: Match coverage by source
SELECT 
    'Analysis 1: Match Coverage' AS ANALYSIS_TYPE,
    H.ORIGINAL_SOURCE AS SOURCE_NAME,
    COUNT(DISTINCT H.HARMONIZED_ID) AS TOTAL_SECURITIES,
    COUNT(DISTINCT CASE 
        WHEN M.MATCH_ID IS NOT NULL THEN H.HARMONIZED_ID 
    END) AS MATCHED_SECURITIES,
    ROUND(COUNT(DISTINCT CASE 
        WHEN M.MATCH_ID IS NOT NULL THEN H.HARMONIZED_ID 
    END) * 100.0 / COUNT(DISTINCT H.HARMONIZED_ID), 2) AS MATCH_RATE_PCT
FROM HARMONIZED_SECURITIES H
LEFT JOIN (
    SELECT SOURCE_1 AS SOURCE_NAME, ID_1 AS ID_VALUE, MATCH_ID FROM MATCHED_SECURITIES
    UNION ALL
    SELECT SOURCE_2 AS SOURCE_NAME, ID_2 AS ID_VALUE, MATCH_ID FROM MATCHED_SECURITIES
) M ON H.ORIGINAL_SOURCE = M.SOURCE_NAME AND H.ORIGINAL_ID = M.ID_VALUE
GROUP BY H.ORIGINAL_SOURCE
ORDER BY H.ORIGINAL_SOURCE;

-- Analysis 2: Multi-source matches (securities matched across multiple sources)
SELECT 
    'Analysis 2: Multi-Source Matches' AS ANALYSIS_TYPE,
    match_counts.MATCH_COUNT AS SOURCES_MATCHED,
    COUNT(*) AS SECURITY_COUNT,
    'Securities matched with this many sources' AS DESC_TEXT
FROM (
    SELECT 
        H.HARMONIZED_ID,
        COUNT(DISTINCT M.SOURCE_NAME) AS MATCH_COUNT
    FROM HARMONIZED_SECURITIES H
    LEFT JOIN (
        SELECT SOURCE_1 AS SOURCE_NAME, ID_1 AS ID_VALUE FROM MATCHED_SECURITIES
        UNION ALL
        SELECT SOURCE_2 AS SOURCE_NAME, ID_2 AS ID_VALUE FROM MATCHED_SECURITIES
    ) M ON H.ORIGINAL_SOURCE = M.SOURCE_NAME AND H.ORIGINAL_ID = M.ID_VALUE
    GROUP BY H.HARMONIZED_ID
) match_counts
GROUP BY match_counts.MATCH_COUNT
ORDER BY match_counts.MATCH_COUNT DESC;

-- Analysis 3: Asset type match distribution
SELECT 
    'Analysis 3: Asset Type Match Distribution' AS ANALYSIS_TYPE,
    H1.ASSET_TYPE AS ASSET_TYPE_NAME,
    COUNT(*) AS MATCH_COUNT,
    ROUND(AVG(M.SIMILARITY_SCORE), 4) AS AVG_SIMILARITY
FROM MATCHED_SECURITIES M
JOIN HARMONIZED_SECURITIES H1 
    ON M.SOURCE_1 = H1.ORIGINAL_SOURCE 
    AND M.ID_1 = H1.ORIGINAL_ID
GROUP BY H1.ASSET_TYPE
ORDER BY MATCH_COUNT DESC;

-- Analysis 4: Matches with full details (for review)
SELECT 
    'Analysis 4: Sample Match Details' AS ANALYSIS_TYPE,
    M.MATCH_ID,
    M.SOURCE_1 AS SOURCE_NAME_1,
    H1.SECURITY_NAME_CLEAN AS NAME_1,
    H1.ISSUER_CLEAN AS ISSUER_1,
    M.SOURCE_2 AS SOURCE_NAME_2,
    H2.SECURITY_NAME_CLEAN AS NAME_2,
    H2.ISSUER_CLEAN AS ISSUER_2,
    M.SIMILARITY_SCORE,
    M.MATCH_STATUS AS STATUS_VALUE
FROM MATCHED_SECURITIES M
JOIN HARMONIZED_SECURITIES H1 
    ON M.SOURCE_1 = H1.ORIGINAL_SOURCE 
    AND M.ID_1 = H1.ORIGINAL_ID
JOIN HARMONIZED_SECURITIES H2 
    ON M.SOURCE_2 = H2.ORIGINAL_SOURCE 
    AND M.ID_2 = H2.ORIGINAL_ID
ORDER BY M.SIMILARITY_SCORE DESC
LIMIT 20;


-- =============================================================================
-- SECTION 7: VISUALIZATION DATA EXPORT
-- =============================================================================
-- Purpose: Generate data for charts and documentation

SELECT '========================================' AS DIVIDER;
SELECT '📊 VISUALIZATION DATA EXPORT' AS SECTION_TITLE;
SELECT 'Export these for documentation charts' AS INSTRUCTION;
SELECT '========================================' AS DIVIDER;

-- Viz 1: Match counts by source pair (Grouped bar chart)
SELECT 
    'Viz 1: Matches by Source Pair' AS VIZ_NAME,
    SOURCE_1 || ' → ' || SOURCE_2 AS SOURCE_PAIR,
    COUNT(*) AS MATCH_COUNT,
    ROUND(AVG(SIMILARITY_SCORE), 4) AS AVG_SIMILARITY,
    'Create grouped bar chart' AS CHART_TYPE
FROM MATCHED_SECURITIES
GROUP BY SOURCE_1, SOURCE_2
ORDER BY MATCH_COUNT DESC;

-- Viz 2: Similarity score distribution (Histogram)
SELECT 
    'Viz 2: Similarity Distribution' AS VIZ_NAME,
    FLOOR(SIMILARITY_SCORE * 20) / 20 AS SCORE_BIN,  -- Bins of 0.05
    COUNT(*) AS RECORD_COUNT,
    'Create histogram' AS CHART_TYPE
FROM MATCHED_SECURITIES
GROUP BY SCORE_BIN
ORDER BY SCORE_BIN;

-- Viz 3: Match rate by source (Bar chart)
SELECT 
    'Viz 3: Match Rate by Source' AS VIZ_NAME,
    H.ORIGINAL_SOURCE AS SOURCE_NAME,
    COUNT(DISTINCT H.HARMONIZED_ID) AS TOTAL_SECURITIES,
    COUNT(DISTINCT CASE 
        WHEN M.MATCH_ID IS NOT NULL THEN H.HARMONIZED_ID 
    END) AS MATCHED_SECURITIES,
    ROUND(COUNT(DISTINCT CASE 
        WHEN M.MATCH_ID IS NOT NULL THEN H.HARMONIZED_ID 
    END) * 100.0 / COUNT(DISTINCT H.HARMONIZED_ID), 2) AS MATCH_RATE_PCT,
    'Create bar chart with percentage' AS CHART_TYPE
FROM HARMONIZED_SECURITIES H
LEFT JOIN (
    SELECT SOURCE_1 AS SOURCE_NAME, ID_1 AS ID_VALUE, MATCH_ID FROM MATCHED_SECURITIES
    UNION ALL
    SELECT SOURCE_2 AS SOURCE_NAME, ID_2 AS ID_VALUE, MATCH_ID FROM MATCHED_SECURITIES
) M ON H.ORIGINAL_SOURCE = M.SOURCE_NAME AND H.ORIGINAL_ID = M.ID_VALUE
GROUP BY H.ORIGINAL_SOURCE
ORDER BY MATCH_RATE_PCT DESC;

-- Viz 4: Match status pie chart
SELECT 
    'Viz 4: Match Status Distribution' AS VIZ_NAME,
    MATCH_STATUS AS STATUS_VALUE,
    COUNT(*) AS RECORD_COUNT,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS PERCENTAGE,
    'Create pie chart' AS CHART_TYPE
FROM MATCHED_SECURITIES
GROUP BY MATCH_STATUS;

-- Viz 5: Match quality matrix (Heatmap)
SELECT 
    'Viz 5: Cross-Source Match Matrix' AS VIZ_NAME,
    SOURCE_1 AS SOURCE_NAME_1,
    SOURCE_2 AS SOURCE_NAME_2,
    COUNT(*) AS MATCH_COUNT,
    ROUND(AVG(SIMILARITY_SCORE), 4) AS AVG_SIMILARITY,
    'Create heatmap' AS CHART_TYPE
FROM MATCHED_SECURITIES
GROUP BY SOURCE_1, SOURCE_2;


-- =============================================================================
-- SECTION 8: MATCH EXPORT FOR REVIEW
-- =============================================================================
-- Purpose: Create a comprehensive match report for manual review

SELECT '========================================' AS DIVIDER;
SELECT '📄 MATCH REPORT FOR REVIEW' AS SECTION_TITLE;
SELECT '========================================' AS DIVIDER;

-- Create detailed match report with all information
CREATE OR REPLACE VIEW MATCH_REVIEW_REPORT AS
SELECT 
    -- Match Info
    M.MATCH_ID,
    M.MATCH_STATUS AS STATUS_VALUE,
    M.SIMILARITY_SCORE,
    M.MATCH_METHOD,
    
    -- Source 1 Details
    M.SOURCE_1 AS SOURCE_NAME_1,
    M.ID_1 AS ID_VALUE_1,
    H1.SECURITY_NAME_CLEAN AS SECURITY_NAME_1,
    H1.ISSUER_CLEAN AS ISSUER_1,
    H1.ASSET_TYPE AS ASSET_TYPE_1,
    H1.ISIN AS ISIN_1,
    H1.SEDOL AS SEDOL_1,
    
    -- Source 2 Details
    M.SOURCE_2 AS SOURCE_NAME_2,
    M.ID_2 AS ID_VALUE_2,
    H2.SECURITY_NAME_CLEAN AS SECURITY_NAME_2,
    H2.ISSUER_CLEAN AS ISSUER_2,
    H2.ASSET_TYPE AS ASSET_TYPE_2,
    H2.ISIN AS ISIN_2,
    H2.SEDOL AS SEDOL_2,
    
    -- Match Quality Indicators
    CASE 
        WHEN H1.ISSUER_CLEAN = H2.ISSUER_CLEAN THEN 'Yes'
        ELSE 'No'
    END AS ISSUER_MATCH,
    
    CASE 
        WHEN H1.ASSET_TYPE = H2.ASSET_TYPE THEN 'Yes'
        ELSE 'No'
    END AS ASSET_TYPE_MATCH,
    
    M.CREATED_AT
    
FROM MATCHED_SECURITIES M
JOIN HARMONIZED_SECURITIES H1 
    ON M.SOURCE_1 = H1.ORIGINAL_SOURCE 
    AND M.ID_1 = H1.ORIGINAL_ID
JOIN HARMONIZED_SECURITIES H2 
    ON M.SOURCE_2 = H2.ORIGINAL_SOURCE 
    AND M.ID_2 = H2.ORIGINAL_ID
ORDER BY M.SIMILARITY_SCORE DESC;

-- Show sample of match report
SELECT * FROM MATCH_REVIEW_REPORT LIMIT 50;


-- =============================================================================
-- VECTOR MATCHING COMPLETE
-- =============================================================================

SELECT '========================================' AS DIVIDER;
SELECT '✅ PHASE 3.3 COMPLETE: VECTOR MATCHING DONE' AS TITLE;
SELECT '========================================' AS DIVIDER;

-- Final summary
SELECT 'Summary' AS SECTION_NAME, 'Metric' AS METRIC_NAME, 'Value' AS METRIC_VALUE
UNION ALL
SELECT 'Summary', 'Total Matches Found', CAST(COUNT(*) AS VARCHAR)
FROM MATCHED_SECURITIES
UNION ALL
SELECT 'Summary', 'Average Similarity Score', 
       CAST(ROUND(AVG(SIMILARITY_SCORE), 4) AS VARCHAR)
FROM MATCHED_SECURITIES
UNION ALL
SELECT 'Summary', 'Approved Matches', 
       CAST(COUNT(CASE WHEN MATCH_STATUS = 'APPROVED' THEN 1 END) AS VARCHAR)
FROM MATCHED_SECURITIES
UNION ALL
SELECT 'Summary', 'Pending Validation', 
       CAST(COUNT(CASE WHEN MATCH_STATUS = 'PENDING' THEN 1 END) AS VARCHAR)
FROM MATCHED_SECURITIES
UNION ALL
SELECT 'Summary', 'Match Method', 'VECTOR (Cosine Similarity)'
UNION ALL
SELECT 'Summary', 'Similarity Threshold', '≥0.80 (High Confidence)';

SELECT '========================================' AS DIVIDER;
SELECT '🎯 NEXT STEPS' AS SECTION_NAME;
SELECT '========================================' AS DIVIDER;

SELECT 
    'Next Phase' AS ITEM_NAME, 'Details' AS ITEM_INFO
UNION ALL
SELECT '→ Phase 3.4', 'AI Validation (LLM validates pending matches)'
UNION ALL
SELECT '→ File', '3.4_AI_Validation.sql'
UNION ALL
SELECT '→ What it does', 'Use Cortex LLM to validate PENDING matches'
UNION ALL
SELECT '→ Output', 'All matches approved or rejected';

-- Cleanup temporary tables
DROP TABLE IF EXISTS TEMP_SIMILARITIES;

-- =============================================================================
-- END OF VECTOR MATCHING SCRIPT
-- =============================================================================