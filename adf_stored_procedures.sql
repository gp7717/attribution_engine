-- =====================================================
-- ADF PIPELINE STORED PROCEDURES
-- =====================================================
-- Stored procedures for Azure Data Factory pipeline execution
-- =====================================================

-- =====================================================
-- 1. PIPELINE LOGGING PROCEDURES
-- =====================================================

-- Procedure: Log pipeline start
CREATE OR REPLACE FUNCTION log_pipeline_start(
    p_pipeline_name VARCHAR(255),
    p_pipeline_run_id VARCHAR(255),
    p_batch_id VARCHAR(100),
    p_start_date DATE,
    p_end_date DATE
) RETURNS TABLE(batch_id VARCHAR(100)) AS $$
BEGIN
    -- Insert pipeline start log
    INSERT INTO attribution_pipeline_log (
        pipeline_name, pipeline_run_id, batch_id, execution_start_time,
        execution_status, analysis_date_start, analysis_date_end
    ) VALUES (
        p_pipeline_name, p_pipeline_run_id, p_batch_id, CURRENT_TIMESTAMP,
        'RUNNING', p_start_date, p_end_date
    );
    
    -- Return batch_id for use in subsequent activities
    RETURN QUERY SELECT p_batch_id;
END;
$$ LANGUAGE plpgsql;

-- Procedure: Log pipeline success
CREATE OR REPLACE FUNCTION log_pipeline_success(
    p_batch_id VARCHAR(100),
    p_total_orders INTEGER,
    p_total_ads INTEGER,
    p_attributed_orders INTEGER,
    p_total_spend DECIMAL(15,2),
    p_total_sales DECIMAL(15,2),
    p_quality_score DECIMAL(5,2)
) RETURNS TABLE(
    execution_end_time TIMESTAMP WITH TIME ZONE,
    execution_duration_seconds INTEGER
) AS $$
DECLARE
    v_start_time TIMESTAMP WITH TIME ZONE;
    v_end_time TIMESTAMP WITH TIME ZONE;
    v_duration INTEGER;
BEGIN
    -- Get start time
    SELECT execution_start_time INTO v_start_time
    FROM attribution_pipeline_log
    WHERE batch_id = p_batch_id;
    
    -- Set end time
    v_end_time := CURRENT_TIMESTAMP;
    
    -- Calculate duration
    v_duration := EXTRACT(EPOCH FROM (v_end_time - v_start_time))::INTEGER;
    
    -- Update pipeline log
    UPDATE attribution_pipeline_log 
    SET execution_end_time = v_end_time,
        execution_status = 'SUCCESS',
        total_orders_processed = p_total_orders,
        total_ads_processed = p_total_ads,
        attributed_orders_count = p_attributed_orders,
        unattributed_orders_count = p_total_orders - p_attributed_orders,
        total_spend_processed = p_total_spend,
        total_sales_processed = p_total_sales,
        data_quality_score = p_quality_score,
        updated_at = CURRENT_TIMESTAMP
    WHERE batch_id = p_batch_id;
    
    -- Return execution details
    RETURN QUERY SELECT v_end_time, v_duration;
END;
$$ LANGUAGE plpgsql;

-- Procedure: Log pipeline failure
CREATE OR REPLACE FUNCTION log_pipeline_failure(
    p_batch_id VARCHAR(100),
    p_error_message TEXT
) RETURNS TABLE(
    execution_end_time TIMESTAMP WITH TIME ZONE,
    execution_duration_seconds INTEGER
) AS $$
DECLARE
    v_start_time TIMESTAMP WITH TIME ZONE;
    v_end_time TIMESTAMP WITH TIME ZONE;
    v_duration INTEGER;
BEGIN
    -- Get start time
    SELECT execution_start_time INTO v_start_time
    FROM attribution_pipeline_log
    WHERE batch_id = p_batch_id;
    
    -- Set end time
    v_end_time := CURRENT_TIMESTAMP;
    
    -- Calculate duration
    v_duration := EXTRACT(EPOCH FROM (v_end_time - v_start_time))::INTEGER;
    
    -- Update pipeline log
    UPDATE attribution_pipeline_log 
    SET execution_end_time = v_end_time,
        execution_status = 'FAILED',
        error_message = p_error_message,
        updated_at = CURRENT_TIMESTAMP
    WHERE batch_id = p_batch_id;
    
    -- Return execution details
    RETURN QUERY SELECT v_end_time, v_duration;
END;
$$ LANGUAGE plpgsql;

-- =====================================================
-- 2. DATA QUALITY VALIDATION PROCEDURES
-- =====================================================

-- Procedure: Validate attribution data quality
CREATE OR REPLACE FUNCTION validate_attribution_data_quality(
    p_batch_id VARCHAR(100)
) RETURNS TABLE(
    quality_score DECIMAL(5,2),
    total_records INTEGER,
    completeness_score DECIMAL(5,2),
    consistency_score DECIMAL(5,2),
    accuracy_score DECIMAL(5,2),
    validation_details JSONB
) AS $$
DECLARE
    v_total_records INTEGER;
    v_completeness_score DECIMAL(5,2);
    v_consistency_score DECIMAL(5,2);
    v_accuracy_score DECIMAL(5,2);
    v_quality_score DECIMAL(5,2);
    v_validation_details JSONB;
BEGIN
    -- Get total records for this batch
    SELECT COUNT(*) INTO v_total_records
    FROM attribution_raw_data
    WHERE batch_id = p_batch_id;
    
    IF v_total_records = 0 THEN
        RETURN QUERY SELECT 0.0, 0, 0.0, 0.0, 0.0, '{"error": "No records found for batch"}'::JSONB;
        RETURN;
    END IF;
    
    -- Calculate completeness score (non-null required fields)
    SELECT ROUND(
        (COUNT(*)::DECIMAL / v_total_records) * 100, 2
    ) INTO v_completeness_score
    FROM attribution_raw_data
    WHERE batch_id = p_batch_id
    AND order_id IS NOT NULL
    AND order_date IS NOT NULL
    AND order_value IS NOT NULL
    AND order_value > 0;
    
    -- Calculate consistency score (attribution logic consistency)
    SELECT ROUND(
        (COUNT(*)::DECIMAL / v_total_records) * 100, 2
    ) INTO v_consistency_score
    FROM attribution_raw_data
    WHERE batch_id = p_batch_id
    AND (
        (is_attributed = TRUE AND attribution_source IS NOT NULL) OR
        (is_attributed = FALSE AND attribution_source IS NULL)
    );
    
    -- Calculate accuracy score (financial data consistency)
    SELECT ROUND(
        (COUNT(*)::DECIMAL / v_total_records) * 100, 2
    ) INTO v_accuracy_score
    FROM attribution_raw_data
    WHERE batch_id = p_batch_id
    AND order_value >= total_cogs
    AND total_cogs >= 0;
    
    -- Calculate overall quality score (weighted average)
    v_quality_score := (v_completeness_score * 0.4) + (v_consistency_score * 0.3) + (v_accuracy_score * 0.3);
    
    -- Build validation details
    v_validation_details := jsonb_build_object(
        'total_records', v_total_records,
        'completeness_issues', v_total_records - (v_completeness_score * v_total_records / 100),
        'consistency_issues', v_total_records - (v_consistency_score * v_total_records / 100),
        'accuracy_issues', v_total_records - (v_accuracy_score * v_total_records / 100),
        'validation_timestamp', CURRENT_TIMESTAMP
    );
    
    -- Return quality metrics
    RETURN QUERY SELECT 
        v_quality_score,
        v_total_records,
        v_completeness_score,
        v_consistency_score,
        v_accuracy_score,
        v_validation_details;
END;
$$ LANGUAGE plpgsql;

-- =====================================================
-- 3. ANALYTICAL VIEWS REFRESH PROCEDURES
-- =====================================================

-- Procedure: Refresh analytical views
CREATE OR REPLACE FUNCTION refresh_analytical_views(
    p_batch_id VARCHAR(100)
) RETURNS TABLE(
    views_refreshed INTEGER,
    refresh_timestamp TIMESTAMP WITH TIME ZONE
) AS $$
DECLARE
    v_views_refreshed INTEGER := 0;
    v_refresh_timestamp TIMESTAMP WITH TIME ZONE;
BEGIN
    v_refresh_timestamp := CURRENT_TIMESTAMP;
    
    -- Refresh daily attribution summary view
    -- Note: Views are automatically updated, but we can force refresh by querying them
    PERFORM COUNT(*) FROM v_attribution_daily_summary;
    v_views_refreshed := v_views_refreshed + 1;
    
    -- Refresh campaign performance summary view
    PERFORM COUNT(*) FROM v_campaign_performance_summary;
    v_views_refreshed := v_views_refreshed + 1;
    
    -- Refresh ad performance summary view
    PERFORM COUNT(*) FROM v_ad_performance_summary;
    v_views_refreshed := v_views_refreshed + 1;
    
    -- Refresh channel performance summary view
    PERFORM COUNT(*) FROM v_channel_performance_summary;
    v_views_refreshed := v_views_refreshed + 1;
    
    -- Update table statistics for better query performance
    ANALYZE attribution_raw_data;
    ANALYZE attribution_ad_level_analytics;
    
    -- Return refresh details
    RETURN QUERY SELECT v_views_refreshed, v_refresh_timestamp;
END;
$$ LANGUAGE plpgsql;

-- =====================================================
-- 4. DATA CLEANUP PROCEDURES
-- =====================================================

-- Procedure: Clean up old data
CREATE OR REPLACE FUNCTION cleanup_old_attribution_data(
    p_retention_days INTEGER DEFAULT 90
) RETURNS TABLE(
    raw_data_deleted INTEGER,
    ad_level_data_deleted INTEGER,
    pipeline_logs_deleted INTEGER,
    cleanup_timestamp TIMESTAMP WITH TIME ZONE
) AS $$
DECLARE
    v_raw_data_deleted INTEGER;
    v_ad_level_data_deleted INTEGER;
    v_pipeline_logs_deleted INTEGER;
    v_cleanup_timestamp TIMESTAMP WITH TIME ZONE;
    v_cutoff_date DATE;
BEGIN
    v_cleanup_timestamp := CURRENT_TIMESTAMP;
    v_cutoff_date := CURRENT_DATE - INTERVAL '1 day' * p_retention_days;
    
    -- Delete old raw data
    DELETE FROM attribution_raw_data 
    WHERE created_at < v_cutoff_date;
    GET DIAGNOSTICS v_raw_data_deleted = ROW_COUNT;
    
    -- Delete old ad-level data
    DELETE FROM attribution_ad_level_analytics 
    WHERE created_at < v_cutoff_date;
    GET DIAGNOSTICS v_ad_level_data_deleted = ROW_COUNT;
    
    -- Delete old pipeline logs (keep more logs than data)
    DELETE FROM attribution_pipeline_log 
    WHERE created_at < (v_cutoff_date - INTERVAL '30 days');
    GET DIAGNOSTICS v_pipeline_logs_deleted = ROW_COUNT;
    
    -- Vacuum tables to reclaim space
    VACUUM ANALYZE attribution_raw_data;
    VACUUM ANALYZE attribution_ad_level_analytics;
    VACUUM ANALYZE attribution_pipeline_log;
    
    -- Return cleanup details
    RETURN QUERY SELECT 
        v_raw_data_deleted,
        v_ad_level_data_deleted,
        v_pipeline_logs_deleted,
        v_cleanup_timestamp;
END;
$$ LANGUAGE plpgsql;

-- =====================================================
-- 5. MONITORING AND ALERTING PROCEDURES
-- =====================================================

-- Procedure: Get pipeline health status
CREATE OR REPLACE FUNCTION get_pipeline_health_status(
    p_hours_back INTEGER DEFAULT 24
) RETURNS TABLE(
    pipeline_name VARCHAR(255),
    last_successful_run TIMESTAMP WITH TIME ZONE,
    last_failed_run TIMESTAMP WITH TIME ZONE,
    success_rate DECIMAL(5,2),
    avg_execution_time_minutes DECIMAL(10,2),
    total_runs INTEGER,
    successful_runs INTEGER,
    failed_runs INTEGER,
    health_status VARCHAR(20)
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        pl.pipeline_name,
        MAX(CASE WHEN pl.execution_status = 'SUCCESS' THEN pl.execution_end_time END) as last_successful_run,
        MAX(CASE WHEN pl.execution_status = 'FAILED' THEN pl.execution_end_time END) as last_failed_run,
        ROUND(
            (COUNT(CASE WHEN pl.execution_status = 'SUCCESS' THEN 1 END)::DECIMAL / COUNT(*)) * 100, 2
        ) as success_rate,
        ROUND(
            AVG(EXTRACT(EPOCH FROM (pl.execution_end_time - pl.execution_start_time)) / 60), 2
        ) as avg_execution_time_minutes,
        COUNT(*) as total_runs,
        COUNT(CASE WHEN pl.execution_status = 'SUCCESS' THEN 1 END) as successful_runs,
        COUNT(CASE WHEN pl.execution_status = 'FAILED' THEN 1 END) as failed_runs,
        CASE 
            WHEN MAX(CASE WHEN pl.execution_status = 'SUCCESS' THEN pl.execution_end_time END) > 
                 CURRENT_TIMESTAMP - INTERVAL '1 day' * p_hours_back / 24 THEN 'HEALTHY'
            WHEN MAX(CASE WHEN pl.execution_status = 'FAILED' THEN pl.execution_end_time END) > 
                 CURRENT_TIMESTAMP - INTERVAL '1 day' * p_hours_back / 24 THEN 'UNHEALTHY'
            ELSE 'NO_RECENT_RUNS'
        END as health_status
    FROM attribution_pipeline_log pl
    WHERE pl.execution_start_time >= CURRENT_TIMESTAMP - INTERVAL '1 day' * p_hours_back / 24
    GROUP BY pl.pipeline_name
    ORDER BY pl.pipeline_name;
END;
$$ LANGUAGE plpgsql;

-- Procedure: Get data quality alerts
CREATE OR REPLACE FUNCTION get_data_quality_alerts(
    p_hours_back INTEGER DEFAULT 24
) RETURNS TABLE(
    batch_id VARCHAR(100),
    pipeline_name VARCHAR(255),
    execution_time TIMESTAMP WITH TIME ZONE,
    data_quality_score DECIMAL(5,2),
    total_records INTEGER,
    alert_level VARCHAR(20),
    alert_message TEXT
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        pl.batch_id,
        pl.pipeline_name,
        pl.execution_start_time,
        pl.data_quality_score,
        pl.total_orders_processed,
        CASE 
            WHEN pl.data_quality_score < 80 THEN 'CRITICAL'
            WHEN pl.data_quality_score < 90 THEN 'WARNING'
            ELSE 'INFO'
        END as alert_level,
        CASE 
            WHEN pl.data_quality_score < 80 THEN 'Data quality score is critically low'
            WHEN pl.data_quality_score < 90 THEN 'Data quality score is below recommended threshold'
            ELSE 'Data quality is within acceptable range'
        END as alert_message
    FROM attribution_pipeline_log pl
    WHERE pl.execution_start_time >= CURRENT_TIMESTAMP - INTERVAL '1 day' * p_hours_back / 24
    AND pl.execution_status = 'SUCCESS'
    AND pl.data_quality_score < 95
    ORDER BY pl.data_quality_score ASC, pl.execution_start_time DESC;
END;
$$ LANGUAGE plpgsql;

-- =====================================================
-- 6. UTILITY PROCEDURES
-- =====================================================

-- Procedure: Get pipeline execution summary
CREATE OR REPLACE FUNCTION get_pipeline_execution_summary(
    p_batch_id VARCHAR(100)
) RETURNS TABLE(
    pipeline_name VARCHAR(255),
    batch_id VARCHAR(100),
    execution_status VARCHAR(50),
    execution_start_time TIMESTAMP WITH TIME ZONE,
    execution_end_time TIMESTAMP WITH TIME ZONE,
    execution_duration_minutes DECIMAL(10,2),
    total_orders_processed INTEGER,
    total_ads_processed INTEGER,
    attributed_orders_count INTEGER,
    unattributed_orders_count INTEGER,
    total_spend_processed DECIMAL(15,2),
    total_sales_processed DECIMAL(15,2),
    data_quality_score DECIMAL(5,2),
    error_message TEXT
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        pl.pipeline_name,
        pl.batch_id,
        pl.execution_status,
        pl.execution_start_time,
        pl.execution_end_time,
        ROUND(
            EXTRACT(EPOCH FROM (pl.execution_end_time - pl.execution_start_time)) / 60, 2
        ) as execution_duration_minutes,
        pl.total_orders_processed,
        pl.total_ads_processed,
        pl.attributed_orders_count,
        pl.unattributed_orders_count,
        pl.total_spend_processed,
        pl.total_sales_processed,
        pl.data_quality_score,
        pl.error_message
    FROM attribution_pipeline_log pl
    WHERE pl.batch_id = p_batch_id;
END;
$$ LANGUAGE plpgsql;

-- Procedure: Get attribution performance metrics
CREATE OR REPLACE FUNCTION get_attribution_performance_metrics(
    p_start_date DATE,
    p_end_date DATE
) RETURNS TABLE(
    channel VARCHAR(100),
    total_orders INTEGER,
    attributed_orders INTEGER,
    attribution_rate DECIMAL(5,2),
    total_sales DECIMAL(15,2),
    total_spend DECIMAL(15,2),
    roas DECIMAL(10,4),
    profit_margin DECIMAL(8,4)
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        ar.channel,
        COUNT(*) as total_orders,
        SUM(CASE WHEN ar.is_attributed THEN 1 ELSE 0 END) as attributed_orders,
        ROUND(
            (SUM(CASE WHEN ar.is_attributed THEN 1 ELSE 0 END)::DECIMAL / COUNT(*)) * 100, 2
        ) as attribution_rate,
        SUM(ar.order_value) as total_sales,
        COALESCE(SUM(aa.spend), 0) as total_spend,
        CASE 
            WHEN SUM(aa.spend) > 0 THEN ROUND((SUM(ar.order_value) / SUM(aa.spend))::DECIMAL, 4)
            ELSE 0 
        END as roas,
        CASE 
            WHEN SUM(ar.order_value) > 0 THEN ROUND(((SUM(ar.order_value) - SUM(ar.total_cogs) - COALESCE(SUM(aa.spend), 0)) / SUM(ar.order_value)) * 100, 2)
            ELSE 0 
        END as profit_margin
    FROM attribution_raw_data ar
    LEFT JOIN attribution_ad_level_analytics aa ON ar.ad_id = aa.ad_id 
        AND ar.campaign_id = aa.campaign_id
        AND DATE(ar.order_date) BETWEEN aa.analysis_date_start AND aa.analysis_date_end
    WHERE DATE(ar.order_date) BETWEEN p_start_date AND p_end_date
    GROUP BY ar.channel
    ORDER BY total_sales DESC;
END;
$$ LANGUAGE plpgsql;

-- =====================================================
-- 7. SAMPLE USAGE QUERIES
-- =====================================================

/*
-- Example 1: Log pipeline start
SELECT * FROM log_pipeline_start(
    'AttributionAnalysisPipeline',
    'run-12345',
    'batch-67890',
    '2025-01-01',
    '2025-01-02'
);

-- Example 2: Validate data quality
SELECT * FROM validate_attribution_data_quality('batch-67890');

-- Example 3: Get pipeline health status
SELECT * FROM get_pipeline_health_status(24);

-- Example 4: Get data quality alerts
SELECT * FROM get_data_quality_alerts(24);

-- Example 5: Get attribution performance metrics
SELECT * FROM get_attribution_performance_metrics('2025-01-01', '2025-01-02');

-- Example 6: Clean up old data
SELECT * FROM cleanup_old_attribution_data(90);
*/

-- =====================================================
-- STORED PROCEDURES CREATION COMPLETE
-- =====================================================
-- These procedures provide:
-- 1. Pipeline execution logging
-- 2. Data quality validation
-- 3. Analytical views refresh
-- 4. Data cleanup and maintenance
-- 5. Monitoring and alerting
-- 6. Performance metrics calculation
-- =====================================================
