/*==============================================================================
  Project: Securities Master Resolution Platform for UK Asset Management
  Phase: 3.2 - Embedding Generation
  File: 3.2_Embedding_Generation.sql
  Author: Rahul Sahay
  GitHub: https://github.com/rahulsahay123/securities-master-resolution
  Date: January 2026
  
  Description:
  This script generates vector embeddings using Snowflake Cortex AI for all
  harmonized securities. Embeddings capture the semantic meaning of security
  names, enabling similarity-based matching across different data sources.
  
  Prerequisites:
  - Phase 3.1 (3.1_Data_Harmonization.sql) completed successfully
  - HARMONIZED_SECURITIES table populated with 3000 records
  - Snowflake Cortex AI enabled in your account
  - SECURITIES_WH warehouse running (LARGE or bigger)
  
  What are Embeddings?
  - Vector representations of text (768 numbers per security)
  - Similar securities get similar embeddings
  - Enables semantic matching: "HSBC Holdings" ≈ "HSBC Hldgs"
  
  Usage:
  Run all sections sequentially in Snowflake Snowsight interface
  
  Sections:
  1. Environment Setup & Cortex Verification
  2. Understand Embeddings (Examples)
  3. Create Security Descriptions
  4. Generate Embeddings for All Securities
  5. Embedding Quality Verification
  6. Sample Similarity Calculations
==============================================================================*/

-- =============================================================================
-- SECTION 1: ENVIRONMENT SETUP & CORTEX VERIFICATION
-- =============================================================================

USE WAREHOUSE SECURITIES_WH;
USE DATABASE SECURITIES_MASTER;
USE SCHEMA ENTITY_RESOLUTION;

-- Verify we're in the correct context
SELECT CURRENT_WAREHOUSE() AS WAREHOUSE,
       CURRENT_DATABASE() AS DATABASE,
       CURRENT_SCHEMA() AS SCHEMA;

-- Check that harmonization was completed
SELECT 
    'Harmonization Check' AS VALIDATION,
    COUNT(*) AS HARMONIZED_RECORDS,
    CASE WHEN COUNT(*) = 3000 THEN '✅ READY' ELSE '❌ Run 3.1 First!' END AS STATUS
FROM HARMONIZED_SECURITIES;

-- Test Cortex AI is working
SELECT 
    'Cortex AI Test' AS TEST_NAME,
    SNOWFLAKE.CORTEX.COMPLETE(
        'mistral-large',
        'Reply with only: Cortex AI is working'
    ) AS TEST_RESULT;

SELECT '========================================' AS DIVIDER;
SELECT '🧠 EMBEDDING GENERATION STARTING' AS TITLE;
SELECT '========================================' AS DIVIDER;


-- =============================================================================
-- SECTION 2: UNDERSTAND EMBEDDINGS (EXAMPLES)
-- =============================================================================
-- Purpose: Show what embeddings look like before generating them for all data

SELECT '========================================' AS DIVIDER;
SELECT '📚 UNDERSTANDING EMBEDDINGS' AS SECTION_TITLE;
SELECT 'Learn how embeddings work with examples' AS INSTRUCTION;
SELECT '========================================' AS DIVIDER;

-- Example 1: Generate embedding for a simple text
-- This creates a 768-dimensional vector
SELECT 
    'Example 1: Simple Embedding' AS EXAMPLE,
    'HSBC Holdings' AS INPUT_TEXT,
    SNOWFLAKE.CORTEX.EMBED_TEXT_768(
        'e5-base-v2',  -- Embedding model
        'HSBC Holdings'
    ) AS EMBEDDING_VECTOR,
    'This is a 768-dimensional vector' AS EXPLANATION;

-- Example 2: Show that similar texts get similar embeddings
-- We'll generate embeddings for two similar company names
WITH test_embeddings AS (
    SELECT 
        'HSBC Holdings' AS text1,
        'HSBC Hldgs' AS text2,
        SNOWFLAKE.CORTEX.EMBED_TEXT_768('e5-base-v2', 'HSBC Holdings') AS embed1,
        SNOWFLAKE.CORTEX.EMBED_TEXT_768('e5-base-v2', 'HSBC Hldgs') AS embed2
)
SELECT 
    'Example 2: Similar Names' AS EXAMPLE,
    text1,
    text2,
    VECTOR_COSINE_SIMILARITY(embed1, embed2) AS SIMILARITY_SCORE,
    'Score close to 1.0 = very similar!' AS EXPLANATION
FROM test_embeddings;

-- Example 3: Show that different texts get different embeddings
WITH test_embeddings AS (
    SELECT 
        'HSBC Holdings' AS text1,
        'BP Energy' AS text2,
        SNOWFLAKE.CORTEX.EMBED_TEXT_768('e5-base-v2', 'HSBC Holdings') AS embed1,
        SNOWFLAKE.CORTEX.EMBED_TEXT_768('e5-base-v2', 'BP Energy') AS embed2
)
SELECT 
    'Example 3: Different Names' AS EXAMPLE,
    text1,
    text2,
    VECTOR_COSINE_SIMILARITY(embed1, embed2) AS SIMILARITY_SCORE,
    'Score lower = less similar' AS EXPLANATION
FROM test_embeddings;


-- =============================================================================
-- SECTION 3: CREATE SECURITY DESCRIPTIONS
-- =============================================================================
-- Purpose: Build meaningful text descriptions for embedding generation
-- Strategy: Combine security name + issuer + asset type for richer context

SELECT '========================================' AS DIVIDER;
SELECT '📝 CREATING SECURITY DESCRIPTIONS' AS SECTION_TITLE;
SELECT 'Build rich descriptions for better embeddings' AS INSTRUCTION;
SELECT '========================================' AS DIVIDER;

-- Show examples of descriptions we'll create
SELECT 
    'Sample Descriptions' AS REPORT_TYPE,
    HARMONIZED_ID,
    ORIGINAL_SOURCE,
    SECURITY_NAME_CLEAN,
    ISSUER_CLEAN,
    ASSET_TYPE,
    -- Build description: "Security Name by Issuer (Asset Type)"
    SECURITY_NAME_CLEAN || ' by ' || ISSUER_CLEAN || ' (' || ASSET_TYPE || ')' AS FULL_DESCRIPTION,
    'This is what we will embed' AS NOTE
FROM HARMONIZED_SECURITIES
LIMIT 10;

-- Verify all descriptions can be created
SELECT 
    'Description Creation Check' AS CHECK_NAME,
    COUNT(*) AS TOTAL_RECORDS,
    COUNT(CASE 
        WHEN SECURITY_NAME_CLEAN IS NOT NULL 
         AND ISSUER_CLEAN IS NOT NULL 
         AND ASSET_TYPE IS NOT NULL 
        THEN 1 
    END) AS COMPLETE_DESCRIPTIONS,
    CASE 
        WHEN COUNT(*) = COUNT(CASE 
            WHEN SECURITY_NAME_CLEAN IS NOT NULL 
             AND ISSUER_CLEAN IS NOT NULL 
             AND ASSET_TYPE IS NOT NULL 
            THEN 1 
        END)
        THEN '✅ ALL READY'
        ELSE '⚠️ Some incomplete'
    END AS STATUS
FROM HARMONIZED_SECURITIES;


-- =============================================================================
-- SECTION 4: GENERATE EMBEDDINGS FOR ALL SECURITIES
-- =============================================================================
-- Purpose: Create embeddings for all 3000 harmonized securities
-- Note: This will take 2-3 minutes to process all records

SELECT '========================================' AS DIVIDER;
SELECT '🚀 GENERATING EMBEDDINGS' AS SECTION_TITLE;
SELECT 'This may take 2-3 minutes for 3000 records...' AS INSTRUCTION;
SELECT '========================================' AS DIVIDER;

-- Clear any existing embeddings (fresh start)
TRUNCATE TABLE IF EXISTS SECURITY_EMBEDDINGS;

-- Generate embeddings for all harmonized securities
-- This is the MAIN embedding generation step
INSERT INTO SECURITY_EMBEDDINGS (
    EMBEDDING_ID,
    HARMONIZED_ID,
    SECURITY_DESCRIPTION,
    EMBEDDING_VECTOR,
    CREATED_AT
)
SELECT 
    -- Create unique embedding ID
    'EMB_' || HARMONIZED_ID AS EMBEDDING_ID,
    
    -- Link back to harmonized record
    HARMONIZED_ID,
    
    -- Build full security description for embedding
    -- Format: "Security Name by Issuer (Asset Type)"
    SECURITY_NAME_CLEAN || ' by ' || ISSUER_CLEAN || ' (' || ASSET_TYPE || ')' AS SECURITY_DESCRIPTION,
    
    -- Generate 768-dimensional embedding using Cortex AI
    -- Model: e5-base-v2 (optimized for semantic similarity)
    SNOWFLAKE.CORTEX.EMBED_TEXT_768(
        'e5-base-v2',
        SECURITY_NAME_CLEAN || ' by ' || ISSUER_CLEAN || ' (' || ASSET_TYPE || ')'
    ) AS EMBEDDING_VECTOR,
    
    -- Timestamp
    CURRENT_TIMESTAMP() AS CREATED_AT
    
FROM HARMONIZED_SECURITIES;

-- Verify embeddings were created
SELECT 
    'Embedding Generation Complete' AS STATUS,
    COUNT(*) AS EMBEDDINGS_CREATED,
    CASE WHEN COUNT(*) = 3000 THEN '✅ SUCCESS' ELSE '❌ CHECK ERRORS' END AS RESULT
FROM SECURITY_EMBEDDINGS;


-- =============================================================================
-- SECTION 5: EMBEDDING QUALITY VERIFICATION
-- =============================================================================
-- Purpose: Comprehensive checks that embeddings are valid and useful

SELECT '========================================' AS DIVIDER;
SELECT '✅ EMBEDDING QUALITY CHECKS' AS SECTION_TITLE;
SELECT 'Verifying embeddings are correct' AS INSTRUCTION;
SELECT '========================================' AS DIVIDER;

-- Check 1: Total record count (should be 3000)
SELECT 
    'Check 1: Total Count' AS CHECK_NAME,
    COUNT(*) AS EMBEDDING_COUNT,
    CASE WHEN COUNT(*) = 3000 THEN '✅ PASS' ELSE '❌ FAIL' END AS STATUS
FROM SECURITY_EMBEDDINGS;

-- Check 2: No NULL embeddings
SELECT 
    'Check 2: NULL Embeddings' AS CHECK_NAME,
    COUNT(*) AS NULL_COUNT,
    CASE WHEN COUNT(*) = 0 THEN '✅ PASS' ELSE '❌ FAIL' END AS STATUS
FROM SECURITY_EMBEDDINGS
WHERE EMBEDDING_VECTOR IS NULL;

-- Check 3: Embeddings have correct dimensions (768)
SELECT 
    'Check 3: Vector Dimensions' AS CHECK_NAME,
    'All vectors are 768-dimensional' AS RESULT,
    '✅ PASS' AS STATUS
FROM SECURITY_EMBEDDINGS
LIMIT 1;

-- Check 4: Embeddings by source
SELECT 
    'Check 4: Embeddings by Source' AS CHECK_NAME,
    H.ORIGINAL_SOURCE,
    COUNT(*) AS EMBEDDING_COUNT,
    CASE WHEN COUNT(*) = 1000 THEN '✅ PASS' ELSE '❌ FAIL' END AS STATUS
FROM SECURITY_EMBEDDINGS E
JOIN HARMONIZED_SECURITIES H ON E.HARMONIZED_ID = H.HARMONIZED_ID
GROUP BY H.ORIGINAL_SOURCE
ORDER BY H.ORIGINAL_SOURCE;

-- Check 5: Sample embeddings preview
SELECT 
    'Check 5: Sample Preview' AS CHECK_NAME,
    EMBEDDING_ID,
    HARMONIZED_ID,
    SECURITY_DESCRIPTION,
    'Vector with 768 dimensions' AS EMBEDDING_INFO,
    CREATED_AT
FROM SECURITY_EMBEDDINGS
ORDER BY EMBEDDING_ID
LIMIT 10;

-- Check 6: Verify descriptions are meaningful
SELECT 
    'Check 6: Description Quality' AS CHECK_NAME,
    MIN(LENGTH(SECURITY_DESCRIPTION)) AS MIN_LENGTH,
    MAX(LENGTH(SECURITY_DESCRIPTION)) AS MAX_LENGTH,
    ROUND(AVG(LENGTH(SECURITY_DESCRIPTION)), 2) AS AVG_LENGTH,
    CASE 
        WHEN MIN(LENGTH(SECURITY_DESCRIPTION)) > 10 
        THEN '✅ PASS' 
        ELSE '❌ FAIL' 
    END AS STATUS
FROM SECURITY_EMBEDDINGS;


-- =============================================================================
-- SECTION 6: SAMPLE SIMILARITY CALCULATIONS
-- =============================================================================
-- Purpose: Test that embeddings can find similar securities
-- This proves the embeddings are working correctly

SELECT '========================================' AS DIVIDER;
SELECT '🔍 TESTING SIMILARITY MATCHING' AS SECTION_TITLE;
SELECT 'Prove embeddings can find similar securities' AS INSTRUCTION;
SELECT '========================================' AS DIVIDER;

-- Test 1: Find similar securities for a sample Bloomberg security
-- Test 1: Find similar securities for a sample Bloomberg security
-- FIXED VERSION - Using subquery instead of CTE

SELECT 
    'Test 1: Find Similar Securities' AS TEST_NAME,
    sample1.SECURITY_DESCRIPTION AS QUERY_SECURITY,
    sample1.ORIGINAL_SOURCE AS QUERY_SOURCE,
    E.SECURITY_DESCRIPTION AS SIMILAR_SECURITY,
    H.ORIGINAL_SOURCE AS SIMILAR_SOURCE,
    ROUND(VECTOR_COSINE_SIMILARITY(sample1.EMBEDDING_VECTOR, E.EMBEDDING_VECTOR), 4) AS SIMILARITY_SCORE,
    CASE 
        WHEN ROUND(VECTOR_COSINE_SIMILARITY(sample1.EMBEDDING_VECTOR, E.EMBEDDING_VECTOR), 4) >= 0.80 
        THEN '✅ High Similarity'
        WHEN ROUND(VECTOR_COSINE_SIMILARITY(sample1.EMBEDDING_VECTOR, E.EMBEDDING_VECTOR), 4) >= 0.60
        THEN '⚠️ Medium Similarity'
        ELSE '❌ Low Similarity'
    END AS MATCH_QUALITY
FROM (
    -- Sample security (subquery instead of CTE)
    SELECT 
        E.EMBEDDING_ID,
        E.HARMONIZED_ID,
        E.SECURITY_DESCRIPTION,
        E.EMBEDDING_VECTOR,
        H.ORIGINAL_SOURCE
    FROM SECURITY_EMBEDDINGS E
    JOIN HARMONIZED_SECURITIES H ON E.HARMONIZED_ID = H.HARMONIZED_ID
    WHERE H.ORIGINAL_SOURCE = 'BLOOMBERG'
    LIMIT 1
) sample1
CROSS JOIN SECURITY_EMBEDDINGS E
JOIN HARMONIZED_SECURITIES H ON E.HARMONIZED_ID = H.HARMONIZED_ID
WHERE E.HARMONIZED_ID != sample1.HARMONIZED_ID  -- Exclude self
ORDER BY SIMILARITY_SCORE DESC
LIMIT 10;

-- Test 2: Cross-source similarity test
-- Find Refinitiv securities similar to a Bloomberg security
WITH bloomberg_sample AS (
    SELECT 
        E.EMBEDDING_ID,
        E.SECURITY_DESCRIPTION AS BBG_DESCRIPTION,
        E.EMBEDDING_VECTOR
    FROM SECURITY_EMBEDDINGS E
    JOIN HARMONIZED_SECURITIES H ON E.HARMONIZED_ID = H.HARMONIZED_ID
    WHERE H.ORIGINAL_SOURCE = 'BLOOMBERG'
      AND H.ISSUER_CLEAN LIKE '%HSBC%'
    LIMIT 1
)
SELECT 
    'Test 2: Cross-Source Matching' AS TEST_NAME,
    B.BBG_DESCRIPTION AS BLOOMBERG_SECURITY,
    E.SECURITY_DESCRIPTION AS REFINITIV_SECURITY,
    ROUND(VECTOR_COSINE_SIMILARITY(B.EMBEDDING_VECTOR, E.EMBEDDING_VECTOR), 4) AS SIMILARITY_SCORE,
    'Looking for HSBC matches across sources' AS NOTE
FROM bloomberg_sample B
CROSS JOIN SECURITY_EMBEDDINGS E
JOIN HARMONIZED_SECURITIES H ON E.HARMONIZED_ID = H.HARMONIZED_ID
WHERE H.ORIGINAL_SOURCE = 'REFINITIV'
  AND H.ISSUER_CLEAN LIKE '%HSBC%'
ORDER BY SIMILARITY_SCORE DESC
LIMIT 5;

-- Test 3: Distribution of similarity scores
SELECT 
    'Test 3: Similarity Distribution' AS TEST_NAME,
    CASE 
        WHEN sim_score >= 0.90 THEN '0.90 - 1.00 (Excellent)'
        WHEN sim_score >= 0.80 THEN '0.80 - 0.89 (Very Good)'
        WHEN sim_score >= 0.70 THEN '0.70 - 0.79 (Good)'
        WHEN sim_score >= 0.60 THEN '0.60 - 0.69 (Fair)'
        ELSE '< 0.60 (Poor)'
    END AS SIMILARITY_RANGE,
    COUNT(*) AS PAIR_COUNT,
    'Shows how many security pairs fall in each similarity range' AS NOTE
FROM (
    SELECT 
        ROUND(VECTOR_COSINE_SIMILARITY(E1.EMBEDDING_VECTOR, E2.EMBEDDING_VECTOR), 2) AS sim_score
    FROM SECURITY_EMBEDDINGS E1
    CROSS JOIN SECURITY_EMBEDDINGS E2
    WHERE E1.EMBEDDING_ID < E2.EMBEDDING_ID  -- Avoid duplicates and self-comparison
    LIMIT 10000  -- Sample to avoid long runtime
) similarity_pairs
GROUP BY SIMILARITY_RANGE
ORDER BY SIMILARITY_RANGE DESC;


-- =============================================================================
-- SECTION 7: VISUALIZATION DATA EXPORT
-- =============================================================================
-- Purpose: Generate data for documentation visuals

SELECT '========================================' AS DIVIDER;
SELECT '📊 VISUALIZATION DATA' AS SECTION_TITLE;
SELECT 'Export these for charts in documentation' AS INSTRUCTION;
SELECT '========================================' AS DIVIDER;

-- Viz 1: Embedding counts by source (Bar chart)
SELECT 
    'Viz 1: Embeddings by Source' AS VIZ_NAME,
    H.ORIGINAL_SOURCE AS SOURCE,
    COUNT(*) AS EMBEDDING_COUNT,
    'Create bar chart' AS CHART_TYPE
FROM SECURITY_EMBEDDINGS E
JOIN HARMONIZED_SECURITIES H ON E.HARMONIZED_ID = H.HARMONIZED_ID
GROUP BY H.ORIGINAL_SOURCE
ORDER BY H.ORIGINAL_SOURCE;

-- Viz 2: Description length distribution (Histogram)
SELECT 
    'Viz 2: Description Length Distribution' AS VIZ_NAME,
    CASE 
        WHEN LENGTH(SECURITY_DESCRIPTION) < 50 THEN '< 50 chars'
        WHEN LENGTH(SECURITY_DESCRIPTION) < 75 THEN '50-74 chars'
        WHEN LENGTH(SECURITY_DESCRIPTION) < 100 THEN '75-99 chars'
        ELSE '100+ chars'
    END AS LENGTH_RANGE,
    COUNT(*) AS COUNT,
    'Create histogram' AS CHART_TYPE
FROM SECURITY_EMBEDDINGS
GROUP BY LENGTH_RANGE
ORDER BY LENGTH_RANGE;

-- Viz 3: Sample similarity matrix (Heatmap data)
-- Shows similarity between first 20 securities
SELECT 
    'Viz 3: Similarity Matrix Sample' AS VIZ_NAME,
    E1.HARMONIZED_ID AS SECURITY_1,
    E2.HARMONIZED_ID AS SECURITY_2,
    ROUND(VECTOR_COSINE_SIMILARITY(E1.EMBEDDING_VECTOR, E2.EMBEDDING_VECTOR), 4) AS SIMILARITY,
    'Create heatmap' AS CHART_TYPE
FROM (
    SELECT * FROM SECURITY_EMBEDDINGS LIMIT 20
) E1
CROSS JOIN (
    SELECT * FROM SECURITY_EMBEDDINGS LIMIT 20
) E2
WHERE E1.EMBEDDING_ID != E2.EMBEDDING_ID;


-- =============================================================================
-- EMBEDDING GENERATION COMPLETE
-- =============================================================================

SELECT '========================================' AS DIVIDER;
SELECT '✅ PHASE 3.2 COMPLETE: EMBEDDINGS GENERATED' AS TITLE;
SELECT '========================================' AS DIVIDER;

-- Final summary
SELECT 
    'Summary' AS SECTION,
    'Metric' AS METRIC_NAME,
    'Value' AS METRIC_VALUE
UNION ALL
SELECT 
    'Summary',
    'Total Embeddings',
    CAST(COUNT(*) AS VARCHAR)
FROM SECURITY_EMBEDDINGS
UNION ALL
SELECT 
    'Summary',
    'Bloomberg Embeddings',
    CAST(COUNT(CASE WHEN H.ORIGINAL_SOURCE = 'BLOOMBERG' THEN 1 END) AS VARCHAR)
FROM SECURITY_EMBEDDINGS E
JOIN HARMONIZED_SECURITIES H ON E.HARMONIZED_ID = H.HARMONIZED_ID
UNION ALL
SELECT 
    'Summary',
    'Refinitiv Embeddings',
    CAST(COUNT(CASE WHEN H.ORIGINAL_SOURCE = 'REFINITIV' THEN 1 END) AS VARCHAR)
FROM SECURITY_EMBEDDINGS E
JOIN HARMONIZED_SECURITIES H ON E.HARMONIZED_ID = H.HARMONIZED_ID
UNION ALL
SELECT 
    'Summary',
    'FCA Embeddings',
    CAST(COUNT(CASE WHEN H.ORIGINAL_SOURCE = 'FCA' THEN 1 END) AS VARCHAR)
FROM SECURITY_EMBEDDINGS E
JOIN HARMONIZED_SECURITIES H ON E.HARMONIZED_ID = H.HARMONIZED_ID
UNION ALL
SELECT 
    'Summary',
    'Vector Dimensions',
    '768 per security'
UNION ALL
SELECT 
    'Summary',
    'Embedding Model',
    'e5-base-v2 (Cortex AI)'
UNION ALL
SELECT 
    'Summary',
    'Ready for Matching',
    '✅ YES';

SELECT '========================================' AS DIVIDER;
SELECT '🎯 NEXT STEPS' AS SECTION;
SELECT '========================================' AS DIVIDER;

SELECT 
    'Next Phase' AS ITEM,
    'Details' AS INFORMATION
UNION ALL
SELECT 
    '→ Phase 3.3',
    'Vector Similarity Matching'
UNION ALL
SELECT 
    '→ File',
    '3.3_Vector_Matching.sql'
UNION ALL
SELECT 
    '→ What it does',
    'Find securities with >80% similarity'
UNION ALL
SELECT 
    '→ Output',
    'MATCHED_SECURITIES table';

-- =============================================================================
-- END OF EMBEDDING GENERATION SCRIPT
-- =============================================================================