/*==============================================================================
  Project: Securities Master Resolution Platform for UK Asset Management
  Phase: 3.4 - AI Validation (LLM-Based Match Validation)
  File: 3.4_AI_Validation.sql
  Author: Rahul Sahay
  GitHub: https://github.com/rahulsahay123/securities-master-resolution
  Date: January 2026
  
  Description:
  This script uses Snowflake Cortex AI (LLM) to validate pending matches
  from Phase 3.3. The LLM analyzes security pairs and determines if they
  represent the same entity, providing reasoning for each decision.
  
  Prerequisites:
  - Phase 3.3 (3.3_Vector_Matching.sql) completed successfully
  - MATCHED_SECURITIES table populated with matches
  - Some matches with status = 'PENDING' (similarity 0.80-0.89)
  - Snowflake Cortex AI enabled
  
  AI Validation Strategy:
  - Take matches with status = 'PENDING' (0.80-0.89 similarity)
  - Use Cortex LLM to analyze each match
  - LLM provides: APPROVED or REJECTED with reasoning
  - Update match status based on AI decision
  - High confidence matches (≥0.90) already auto-approved
  
  Usage:
  Run all sections sequentially in Snowflake Snowsight interface
  
  Sections:
  1. Environment Setup & Validation Check
  2. Understanding AI Validation
  3. AI Validation Test (Single Match)
  4. Batch AI Validation (All Pending Matches)
  5. Update Match Status
  6. Final Match Statistics
  7. Match Quality Report
==============================================================================*/

-- =============================================================================
-- SECTION 1: ENVIRONMENT SETUP & VALIDATION CHECK
-- =============================================================================

USE WAREHOUSE SECURITIES_WH;
USE DATABASE SECURITIES_MASTER;
USE SCHEMA ENTITY_RESOLUTION;

-- Verify we're in the correct context
SELECT CURRENT_WAREHOUSE() AS WAREHOUSE_NAME,
       CURRENT_DATABASE() AS DATABASE_NAME,
       CURRENT_SCHEMA() AS SCHEMA_NAME;

-- Check that matches exist
SELECT 
    'Matches Check' AS VALIDATION_TYPE,
    COUNT(*) AS TOTAL_MATCHES,
    CASE 
        WHEN COUNT(*) > 0 THEN '✅ READY FOR VALIDATION' 
        ELSE '❌ Run 3.3 First!' 
    END AS STATUS_MSG
FROM MATCHED_SECURITIES;

-- Check pending matches (need AI validation)
SELECT 
    'Pending Matches Check' AS VALIDATION_TYPE,
    COUNT(*) AS PENDING_COUNT,
    CASE 
        WHEN COUNT(*) > 0 THEN '✅ Found pending matches for validation' 
        WHEN COUNT(*) = 0 THEN '⚠️ No pending matches (all auto-approved or threshold too high)'
        ELSE '❌ Error'
    END AS STATUS_MSG
FROM MATCHED_SECURITIES
WHERE MATCH_STATUS = 'PENDING';

-- Show match status distribution
SELECT 
    'Current Match Status' AS REPORT_TYPE,
    MATCH_STATUS AS STATUS_VALUE,
    COUNT(*) AS MATCH_COUNT,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS PERCENTAGE
FROM MATCHED_SECURITIES
GROUP BY MATCH_STATUS
ORDER BY MATCH_COUNT DESC;

SELECT '========================================' AS DIVIDER;
SELECT '🤖 AI VALIDATION STARTING' AS TITLE;
SELECT '========================================' AS DIVIDER;


-- =============================================================================
-- SECTION 2: UNDERSTANDING AI VALIDATION
-- =============================================================================
-- Purpose: Show how LLM validates matches

SELECT '========================================' AS DIVIDER;
SELECT '📚 UNDERSTANDING AI VALIDATION' AS SECTION_TITLE;
SELECT 'Learn how LLM validates matches' AS INSTRUCTION;
SELECT '========================================' AS DIVIDER;

-- Example: Show what data the LLM will see
SELECT 
    'Example Input to LLM' AS EXAMPLE_TYPE,
    M.MATCH_ID,
    M.SIMILARITY_SCORE,
    H1.SECURITY_NAME_CLEAN AS SECURITY_1,
    H1.ISSUER_CLEAN AS ISSUER_1,
    H1.ASSET_TYPE AS ASSET_TYPE_1,
    H2.SECURITY_NAME_CLEAN AS SECURITY_2,
    H2.ISSUER_CLEAN AS ISSUER_2,
    H2.ASSET_TYPE AS ASSET_TYPE_2,
    'This is what AI will analyze' AS NOTE
FROM MATCHED_SECURITIES M
JOIN HARMONIZED_SECURITIES H1 
    ON M.SOURCE_1 = H1.ORIGINAL_SOURCE 
    AND M.ID_1 = H1.ORIGINAL_ID
JOIN HARMONIZED_SECURITIES H2 
    ON M.SOURCE_2 = H2.ORIGINAL_SOURCE 
    AND M.ID_2 = H2.ORIGINAL_ID
WHERE M.MATCH_STATUS = 'PENDING'
LIMIT 3;


-- =============================================================================
-- SECTION 3: AI VALIDATION TEST (SINGLE MATCH)
-- =============================================================================
-- Purpose: Test AI validation on one match before batch processing

SELECT '========================================' AS DIVIDER;
SELECT '🧪 TESTING AI VALIDATION (SINGLE MATCH)' AS SECTION_TITLE;
SELECT 'Test LLM on one pending match' AS INSTRUCTION;
SELECT '========================================' AS DIVIDER;

-- Get one pending match for testing
CREATE OR REPLACE TEMPORARY TABLE TEST_MATCH AS
SELECT 
    M.MATCH_ID,
    M.SIMILARITY_SCORE,
    H1.SECURITY_NAME_CLEAN AS SECURITY_1,
    H1.ISSUER_CLEAN AS ISSUER_1,
    H1.ASSET_TYPE AS ASSET_TYPE_1,
    H2.SECURITY_NAME_CLEAN AS SECURITY_2,
    H2.ISSUER_CLEAN AS ISSUER_2,
    H2.ASSET_TYPE AS ASSET_TYPE_2
FROM MATCHED_SECURITIES M
JOIN HARMONIZED_SECURITIES H1 
    ON M.SOURCE_1 = H1.ORIGINAL_SOURCE 
    AND M.ID_1 = H1.ORIGINAL_ID
JOIN HARMONIZED_SECURITIES H2 
    ON M.SOURCE_2 = H2.ORIGINAL_SOURCE 
    AND M.ID_2 = H2.ORIGINAL_ID
WHERE M.MATCH_STATUS = 'PENDING'
LIMIT 1;

-- Show the test match
SELECT 'Test Match Details' AS TEST_STEP,
       MATCH_ID,
       SECURITY_1,
       ISSUER_1,
       SECURITY_2,
       ISSUER_2,
       SIMILARITY_SCORE
FROM TEST_MATCH;

-- Test AI validation on this match
SELECT 
    'AI Validation Test Result' AS TEST_STEP,
    MATCH_ID,
    SECURITY_1,
    SECURITY_2,
    SIMILARITY_SCORE,
    SNOWFLAKE.CORTEX.COMPLETE(
        'mistral-large',
        'You are an expert in financial entity resolution. Analyze these two securities and determine if they represent the SAME entity.

Security 1: ' || SECURITY_1 || '
Issuer 1: ' || ISSUER_1 || '
Asset Type 1: ' || ASSET_TYPE_1 || '

Security 2: ' || SECURITY_2 || '
Issuer 2: ' || ISSUER_2 || '
Asset Type 2: ' || ASSET_TYPE_2 || '

Vector Similarity Score: ' || CAST(SIMILARITY_SCORE AS VARCHAR) || '

Respond ONLY with one of these exact formats:
APPROVED - [reason in 1-2 sentences]
REJECTED - [reason in 1-2 sentences]

Consider:
- Company name variations (Holdings, Hldgs, PLC, Ltd, Limited)
- Asset type match
- Issuer similarity
- Overall context

Your response:'
    ) AS AI_DECISION
FROM TEST_MATCH;

-- Cleanup test table
DROP TABLE IF EXISTS TEST_MATCH;


-- =============================================================================
-- SECTION 4: BATCH AI VALIDATION (ALL PENDING MATCHES)
-- =============================================================================
-- Purpose: Validate ALL pending matches using Cortex LLM
-- Note: This may take 2-3 minutes for large datasets

SELECT '========================================' AS DIVIDER;
SELECT '🚀 BATCH AI VALIDATION' AS SECTION_TITLE;
SELECT 'Validating all pending matches...' AS INSTRUCTION;
SELECT 'This may take 2-3 minutes...' AS WARNING;
SELECT '========================================' AS DIVIDER;

-- Create temporary table with AI validation results
CREATE OR REPLACE TEMPORARY TABLE AI_VALIDATION_RESULTS AS
SELECT 
    M.MATCH_ID,
    M.SOURCE_1 AS SOURCE_NAME_1,
    M.ID_1 AS ID_VALUE_1,
    M.SOURCE_2 AS SOURCE_NAME_2,
    M.ID_2 AS ID_VALUE_2,
    M.SIMILARITY_SCORE,
    H1.SECURITY_NAME_CLEAN AS SECURITY_1,
    H1.ISSUER_CLEAN AS ISSUER_1,
    H1.ASSET_TYPE AS ASSET_TYPE_1,
    H2.SECURITY_NAME_CLEAN AS SECURITY_2,
    H2.ISSUER_CLEAN AS ISSUER_2,
    H2.ASSET_TYPE AS ASSET_TYPE_2,
    
    -- AI Validation using Cortex LLM
    SNOWFLAKE.CORTEX.COMPLETE(
        'mistral-large',
        'You are an expert in financial entity resolution. Analyze these two securities and determine if they represent the SAME entity.

Security 1: ' || H1.SECURITY_NAME_CLEAN || '
Issuer 1: ' || H1.ISSUER_CLEAN || '
Asset Type 1: ' || H1.ASSET_TYPE || '

Security 2: ' || H2.SECURITY_NAME_CLEAN || '
Issuer 2: ' || H2.ISSUER_CLEAN || '
Asset Type 2: ' || H2.ASSET_TYPE || '

Vector Similarity Score: ' || CAST(M.SIMILARITY_SCORE AS VARCHAR) || '

Respond ONLY with one of these exact formats:
APPROVED - [reason in 1-2 sentences]
REJECTED - [reason in 1-2 sentences]

Consider:
- Company name variations (Holdings, Hldgs, PLC, Ltd, Limited)
- Asset type match
- Issuer similarity
- Overall context

Your response:'
    ) AS AI_RESPONSE,
    
    CURRENT_TIMESTAMP() AS VALIDATION_TIMESTAMP
    
FROM MATCHED_SECURITIES M
JOIN HARMONIZED_SECURITIES H1 
    ON M.SOURCE_1 = H1.ORIGINAL_SOURCE 
    AND M.ID_1 = H1.ORIGINAL_ID
JOIN HARMONIZED_SECURITIES H2 
    ON M.SOURCE_2 = H2.ORIGINAL_SOURCE 
    AND M.ID_2 = H2.ORIGINAL_ID
WHERE M.MATCH_STATUS = 'PENDING';

-- Verify AI validation completed
SELECT 
    'AI Validation Complete' AS STATUS_MSG,
    COUNT(*) AS VALIDATIONS_PERFORMED,
    '✅ Ready to parse results' AS RESULT_MSG
FROM AI_VALIDATION_RESULTS;

-- Show sample of AI responses
SELECT 
    'Sample AI Responses' AS REPORT_TYPE,
    MATCH_ID,
    SECURITY_1,
    SECURITY_2,
    SIMILARITY_SCORE,
    SUBSTRING(AI_RESPONSE, 1, 100) AS AI_RESPONSE_PREVIEW
FROM AI_VALIDATION_RESULTS
LIMIT 10;


-- =============================================================================
-- SECTION 5: PARSE AI RESPONSES & UPDATE MATCH STATUS
-- =============================================================================
-- Purpose: Extract AI decision and update MATCHED_SECURITIES table

SELECT '========================================' AS DIVIDER;
SELECT '📊 PARSING AI RESPONSES' AS SECTION_TITLE;
SELECT '========================================' AS DIVIDER;

-- Parse AI responses to extract decision (APPROVED or REJECTED)
CREATE OR REPLACE TEMPORARY TABLE PARSED_AI_DECISIONS AS
SELECT 
    MATCH_ID,
    AI_RESPONSE,
    CASE 
        WHEN UPPER(AI_RESPONSE) LIKE '%APPROVED%' THEN 'APPROVED'
        WHEN UPPER(AI_RESPONSE) LIKE '%REJECTED%' THEN 'REJECTED'
        ELSE 'PENDING'  -- If AI response is unclear, keep as pending
    END AS AI_DECISION,
    
    -- Extract reasoning (text after the decision)
    CASE 
        WHEN UPPER(AI_RESPONSE) LIKE '%APPROVED - %' 
        THEN TRIM(SUBSTRING(AI_RESPONSE, POSITION('APPROVED - ' IN UPPER(AI_RESPONSE)) + 11))
        WHEN UPPER(AI_RESPONSE) LIKE '%REJECTED - %' 
        THEN TRIM(SUBSTRING(AI_RESPONSE, POSITION('REJECTED - ' IN UPPER(AI_RESPONSE)) + 11))
        ELSE AI_RESPONSE
    END AS AI_REASONING,
    
    VALIDATION_TIMESTAMP
FROM AI_VALIDATION_RESULTS;

-- Show parsed decisions
SELECT 
    'Parsed AI Decisions' AS REPORT_TYPE,
    AI_DECISION,
    COUNT(*) AS DECISION_COUNT,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS PERCENTAGE
FROM PARSED_AI_DECISIONS
GROUP BY AI_DECISION
ORDER BY DECISION_COUNT DESC;

-- Show sample parsed results
SELECT 
    'Sample Parsed Decisions' AS REPORT_TYPE,
    P.MATCH_ID,
    V.SECURITY_1,
    V.SECURITY_2,
    V.SIMILARITY_SCORE,
    P.AI_DECISION,
    SUBSTRING(P.AI_REASONING, 1, 100) AS REASONING_PREVIEW
FROM PARSED_AI_DECISIONS P
JOIN AI_VALIDATION_RESULTS V ON P.MATCH_ID = V.MATCH_ID
LIMIT 10;

-- Update MATCHED_SECURITIES table with AI decisions
UPDATE MATCHED_SECURITIES M
SET 
    MATCH_STATUS = P.AI_DECISION,
    MATCH_METHOD = 'AI_VALIDATED'
FROM PARSED_AI_DECISIONS P
WHERE M.MATCH_ID = P.MATCH_ID
  AND M.MATCH_STATUS = 'PENDING';

-- Verify updates
SELECT 
    'Match Status Update Complete' AS STATUS_MSG,
    COUNT(*) AS RECORDS_UPDATED,
    '✅ Matches validated by AI' AS RESULT_MSG
FROM MATCHED_SECURITIES
WHERE MATCH_METHOD = 'AI_VALIDATED';


-- =============================================================================
-- SECTION 6: FINAL MATCH STATISTICS
-- =============================================================================
-- Purpose: Comprehensive statistics after AI validation

SELECT '========================================' AS DIVIDER;
SELECT '📊 FINAL MATCH STATISTICS' AS SECTION_TITLE;
SELECT '========================================' AS DIVIDER;

-- Final status distribution
SELECT 
    'Final Match Status Distribution' AS REPORT_TYPE,
    MATCH_STATUS AS STATUS_VALUE,
    MATCH_METHOD AS METHOD_TYPE,
    COUNT(*) AS MATCH_COUNT,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS PERCENTAGE
FROM MATCHED_SECURITIES
GROUP BY MATCH_STATUS, MATCH_METHOD
ORDER BY MATCH_COUNT DESC;

-- Overall statistics
SELECT 
    'Overall Match Statistics' AS STAT_TYPE,
    COUNT(*) AS TOTAL_MATCHES,
    COUNT(CASE WHEN MATCH_STATUS = 'APPROVED' THEN 1 END) AS APPROVED_MATCHES,
    COUNT(CASE WHEN MATCH_STATUS = 'REJECTED' THEN 1 END) AS REJECTED_MATCHES,
    COUNT(CASE WHEN MATCH_STATUS = 'PENDING' THEN 1 END) AS STILL_PENDING,
    ROUND(COUNT(CASE WHEN MATCH_STATUS = 'APPROVED' THEN 1 END) * 100.0 / COUNT(*), 2) AS APPROVAL_RATE_PCT
FROM MATCHED_SECURITIES;

-- AI validation performance
SELECT 
    'AI Validation Performance' AS REPORT_TYPE,
    COUNT(*) AS AI_VALIDATED_MATCHES,
    COUNT(CASE WHEN MATCH_STATUS = 'APPROVED' THEN 1 END) AS AI_APPROVED,
    COUNT(CASE WHEN MATCH_STATUS = 'REJECTED' THEN 1 END) AS AI_REJECTED,
    ROUND(COUNT(CASE WHEN MATCH_STATUS = 'APPROVED' THEN 1 END) * 100.0 / COUNT(*), 2) AS AI_APPROVAL_RATE
FROM MATCHED_SECURITIES
WHERE MATCH_METHOD = 'AI_VALIDATED';

-- Match quality by similarity range (after AI validation)
SELECT 
    'Match Quality by Similarity Range' AS REPORT_TYPE,
    CASE 
        WHEN SIMILARITY_SCORE >= 0.95 THEN '0.95-1.00'
        WHEN SIMILARITY_SCORE >= 0.90 THEN '0.90-0.94'
        WHEN SIMILARITY_SCORE >= 0.85 THEN '0.85-0.89'
        WHEN SIMILARITY_SCORE >= 0.80 THEN '0.80-0.84'
        ELSE '< 0.80'
    END AS SIMILARITY_RANGE,
    COUNT(*) AS TOTAL_IN_RANGE,
    COUNT(CASE WHEN MATCH_STATUS = 'APPROVED' THEN 1 END) AS APPROVED_IN_RANGE,
    ROUND(COUNT(CASE WHEN MATCH_STATUS = 'APPROVED' THEN 1 END) * 100.0 / COUNT(*), 2) AS APPROVAL_RATE_PCT
FROM MATCHED_SECURITIES
GROUP BY SIMILARITY_RANGE
ORDER BY SIMILARITY_RANGE DESC;


-- =============================================================================
-- SECTION 7: FINAL MATCH QUALITY REPORT
-- =============================================================================
-- Purpose: Create final comprehensive report with AI validation results

SELECT '========================================' AS DIVIDER;
SELECT '📄 FINAL MATCH QUALITY REPORT' AS SECTION_TITLE;
SELECT '========================================' AS DIVIDER;

-- Update the match review report view to include AI validation results
CREATE OR REPLACE VIEW MATCH_REVIEW_REPORT_FINAL AS
SELECT 
    -- Match Info
    M.MATCH_ID,
    M.MATCH_STATUS AS STATUS_VALUE,
    M.SIMILARITY_SCORE,
    M.MATCH_METHOD AS METHOD_TYPE,
    
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
    
    -- AI Validation Results (if available)
    P.AI_REASONING,
    
    M.CREATED_AT
    
FROM MATCHED_SECURITIES M
JOIN HARMONIZED_SECURITIES H1 
    ON M.SOURCE_1 = H1.ORIGINAL_SOURCE 
    AND M.ID_1 = H1.ORIGINAL_ID
JOIN HARMONIZED_SECURITIES H2 
    ON M.SOURCE_2 = H2.ORIGINAL_SOURCE 
    AND M.ID_2 = H2.ORIGINAL_ID
LEFT JOIN PARSED_AI_DECISIONS P
    ON M.MATCH_ID = P.MATCH_ID
ORDER BY M.SIMILARITY_SCORE DESC;

-- Show sample of final report
SELECT 
    'Final Match Report Sample' AS REPORT_TYPE,
    MATCH_ID,
    SOURCE_NAME_1,
    SECURITY_NAME_1,
    SOURCE_NAME_2,
    SECURITY_NAME_2,
    SIMILARITY_SCORE,
    STATUS_VALUE,
    METHOD_TYPE,
    SUBSTRING(AI_REASONING, 1, 50) AS AI_REASONING_PREVIEW
FROM MATCH_REVIEW_REPORT_FINAL
WHERE STATUS_VALUE = 'APPROVED'
ORDER BY SIMILARITY_SCORE DESC
LIMIT 20;

-- Export-ready approved matches
SELECT 
    'Export Ready: Approved Matches' AS EXPORT_TYPE,
    COUNT(*) AS TOTAL_APPROVED_MATCHES,
    'Use SELECT * FROM MATCH_REVIEW_REPORT_FINAL WHERE STATUS_VALUE = ''APPROVED''' AS EXPORT_QUERY
FROM MATCH_REVIEW_REPORT_FINAL
WHERE STATUS_VALUE = 'APPROVED';


-- =============================================================================
-- SECTION 8: VISUALIZATION DATA FOR DOCUMENTATION
-- =============================================================================
-- Purpose: Generate data for charts showing AI validation impact

SELECT '========================================' AS DIVIDER;
SELECT '📊 VISUALIZATION DATA EXPORT' AS SECTION_TITLE;
SELECT '========================================' AS DIVIDER;

-- Viz 1: Before/After AI Validation (Stacked Bar)
SELECT 
    'Viz 1: AI Validation Impact' AS VIZ_NAME,
    MATCH_METHOD AS METHOD_TYPE,
    MATCH_STATUS AS STATUS_VALUE,
    COUNT(*) AS MATCH_COUNT,
    'Create stacked bar chart' AS CHART_TYPE
FROM MATCHED_SECURITIES
GROUP BY MATCH_METHOD, MATCH_STATUS
ORDER BY MATCH_METHOD, MATCH_STATUS;

-- Viz 2: AI Decision Distribution (Pie Chart)
SELECT 
    'Viz 2: AI Decision Distribution' AS VIZ_NAME,
    AI_DECISION AS DECISION_TYPE,
    COUNT(*) AS DECISION_COUNT,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS PERCENTAGE,
    'Create pie chart' AS CHART_TYPE
FROM PARSED_AI_DECISIONS
GROUP BY AI_DECISION;

-- Viz 3: Match Success Rate by Source Pair (After AI)
SELECT 
    'Viz 3: Match Rate by Source Pair (Final)' AS VIZ_NAME,
    SOURCE_1 || ' ↔ ' || SOURCE_2 AS SOURCE_PAIR,
    COUNT(*) AS TOTAL_MATCHES,
    COUNT(CASE WHEN MATCH_STATUS = 'APPROVED' THEN 1 END) AS APPROVED_MATCHES,
    ROUND(COUNT(CASE WHEN MATCH_STATUS = 'APPROVED' THEN 1 END) * 100.0 / COUNT(*), 2) AS APPROVAL_RATE_PCT,
    'Create grouped bar chart' AS CHART_TYPE
FROM MATCHED_SECURITIES
GROUP BY SOURCE_1, SOURCE_2
ORDER BY APPROVAL_RATE_PCT DESC;


-- =============================================================================
-- AI VALIDATION COMPLETE
-- =============================================================================

SELECT '========================================' AS DIVIDER;
SELECT '✅ PHASE 3.4 COMPLETE: AI VALIDATION DONE' AS TITLE;
SELECT '========================================' AS DIVIDER;

-- Final summary
SELECT 'Final Summary' AS SECTION_NAME, 'Metric' AS METRIC_NAME, 'Value' AS METRIC_VALUE
UNION ALL
SELECT 'Final Summary', 'Total Matches', CAST(COUNT(*) AS VARCHAR)
FROM MATCHED_SECURITIES
UNION ALL
SELECT 'Final Summary', 'Approved Matches', 
       CAST(COUNT(CASE WHEN MATCH_STATUS = 'APPROVED' THEN 1 END) AS VARCHAR)
FROM MATCHED_SECURITIES
UNION ALL
SELECT 'Final Summary', 'AI Validated Matches', 
       CAST(COUNT(CASE WHEN MATCH_METHOD = 'AI_VALIDATED' THEN 1 END) AS VARCHAR)
FROM MATCHED_SECURITIES
UNION ALL
SELECT 'Final Summary', 'Overall Approval Rate', 
       CAST(ROUND(COUNT(CASE WHEN MATCH_STATUS = 'APPROVED' THEN 1 END) * 100.0 / COUNT(*), 2) AS VARCHAR) || '%'
FROM MATCHED_SECURITIES
UNION ALL
SELECT 'Final Summary', 'Average Similarity (Approved)', 
       CAST(ROUND(AVG(CASE WHEN MATCH_STATUS = 'APPROVED' THEN SIMILARITY_SCORE END), 4) AS VARCHAR)
FROM MATCHED_SECURITIES;

SELECT '========================================' AS DIVIDER;
SELECT '🎯 ENTITY RESOLUTION PIPELINE COMPLETE!' AS SECTION_NAME;
SELECT '========================================' AS DIVIDER;

SELECT 
    'Achievement Unlocked' AS ACHIEVEMENT_TYPE,
    'Entity Resolution Pipeline' AS ACCOMPLISHMENT,
    'You built an AI-powered matching system!' AS DESCRIPTION
UNION ALL
SELECT 
    'Next Steps',
    'Phase 4: Streamlit Application',
    'Build interactive dashboard'
UNION ALL
SELECT 
    'Next Steps',
    'Phase 5: Documentation',
    'Create README, Wiki, Playbook'
UNION ALL
SELECT 
    'Next Steps',
    'Phase 6: Medium Article',
    'Write technical walkthrough';

-- Cleanup temporary tables
DROP TABLE IF EXISTS AI_VALIDATION_RESULTS;
DROP TABLE IF EXISTS PARSED_AI_DECISIONS;

-- =============================================================================
-- END OF AI VALIDATION SCRIPT
-- =============================================================================