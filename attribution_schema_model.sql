-- =====================================================
-- ATTRIBUTION DATA WAREHOUSE SCHEMA MODEL
-- =====================================================
-- This schema stores raw attribution data and ad-level analytics
-- Generated for Azure Data Factory implementation
-- =====================================================

-- =====================================================
-- 1. RAW ATTRIBUTION DATA TABLE
-- =====================================================
-- Stores individual order-level attribution data
-- Source: AttributionEngine.py output
-- =====================================================

CREATE TABLE IF NOT EXISTS attribution_raw_data (
    -- Primary Key
    id BIGSERIAL PRIMARY KEY,
    
    -- Order Information
    order_id VARCHAR(255) NOT NULL,
    order_name VARCHAR(255),
    order_date TIMESTAMP WITH TIME ZONE NOT NULL,
    order_value DECIMAL(15,2) NOT NULL,
    order_currency VARCHAR(10) DEFAULT 'INR',
    
    -- Financial Metrics
    total_cogs DECIMAL(15,2) DEFAULT 0,
    line_items_count INTEGER DEFAULT 0,
    
    -- SKU Information
    skus TEXT, -- Comma-separated SKU list
    unique_skus_count INTEGER DEFAULT 0,
    total_sku_quantity INTEGER DEFAULT 0,
    
    -- Attribution Information
    channel VARCHAR(100),
    attribution_source VARCHAR(50), -- 'customer_journey', 'custom_attributes', 'direct_utm'
    attribution_id VARCHAR(255),
    attribution_type VARCHAR(50), -- 'content', 'campaign', 'medium'
    
    -- UTM Parameters
    utm_source VARCHAR(255),
    utm_medium VARCHAR(255),
    utm_campaign VARCHAR(255),
    utm_content VARCHAR(255),
    utm_term VARCHAR(255),
    
    -- Campaign Data
    campaign_id VARCHAR(255),
    campaign_name VARCHAR(500),
    adset_id VARCHAR(255),
    adset_name VARCHAR(500),
    ad_id VARCHAR(255),
    ad_name VARCHAR(500),
    
    -- Data Quality Flags
    is_attributed BOOLEAN DEFAULT FALSE,
    is_paid_channel BOOLEAN DEFAULT FALSE,
    has_customer_journey BOOLEAN DEFAULT FALSE,
    has_custom_attributes BOOLEAN DEFAULT FALSE,
    
    -- Audit Fields
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    data_source VARCHAR(100) DEFAULT 'attribution_engine',
    batch_id VARCHAR(100) -- For tracking ADF pipeline runs
    
    -- Indexes
    CONSTRAINT idx_attribution_raw_order_id UNIQUE (order_id),
    CONSTRAINT idx_attribution_raw_order_date CHECK (order_date IS NOT NULL),
    CONSTRAINT idx_attribution_raw_order_value CHECK (order_value >= 0)
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_attribution_raw_order_date ON attribution_raw_data (order_date);
CREATE INDEX IF NOT EXISTS idx_attribution_raw_campaign_id ON attribution_raw_data (campaign_id);
CREATE INDEX IF NOT EXISTS idx_attribution_raw_ad_id ON attribution_raw_data (ad_id);
CREATE INDEX IF NOT EXISTS idx_attribution_raw_channel ON attribution_raw_data (channel);
CREATE INDEX IF NOT EXISTS idx_attribution_raw_is_attributed ON attribution_raw_data (is_attributed);
CREATE INDEX IF NOT EXISTS idx_attribution_raw_batch_id ON attribution_raw_data (batch_id);

-- =====================================================
-- 2. AD LEVEL ANALYTICS TABLE
-- =====================================================
-- Stores aggregated ad-level performance metrics
-- Source: AttributionEngine.py create_ad_level_summary() output
-- =====================================================

CREATE TABLE IF NOT EXISTS attribution_ad_level_analytics (
    -- Primary Key
    id BIGSERIAL PRIMARY KEY,
    
    -- Campaign Hierarchy
    campaign_id VARCHAR(255) NOT NULL,
    campaign_name VARCHAR(500) NOT NULL,
    adset_id VARCHAR(255) NOT NULL,
    adset_name VARCHAR(500) NOT NULL,
    ad_id VARCHAR(255) NOT NULL,
    ad_name VARCHAR(500) NOT NULL,
    channel VARCHAR(100) NOT NULL,
    
    -- Attribution Status
    attributed BOOLEAN NOT NULL, -- True if all orders are attributed, False if any are unattributed
    
    -- Order Metrics
    orders INTEGER DEFAULT 0,
    attributed_orders INTEGER DEFAULT 0,
    
    -- Financial Metrics
    total_sales DECIMAL(15,2) DEFAULT 0,
    total_cogs DECIMAL(15,2) DEFAULT 0,
    net_profit DECIMAL(15,2) DEFAULT 0,
    
    -- Ad Performance Metrics
    impressions BIGINT DEFAULT 0,
    clicks BIGINT DEFAULT 0,
    spend DECIMAL(15,2) DEFAULT 0,
    
    -- Calculated Performance Metrics
    roas DECIMAL(10,4) DEFAULT 0, -- Return on Ad Spend
    ctr DECIMAL(8,4) DEFAULT 0, -- Click-Through Rate (%)
    conversion_rate DECIMAL(8,4) DEFAULT 0, -- Conversion Rate (%)
    avg_order_value DECIMAL(15,2) DEFAULT 0,
    profit_margin DECIMAL(8,4) DEFAULT 0, -- Profit Margin (%)
    
    -- SKU Information
    all_skus TEXT, -- Comma-separated list of all SKUs
    total_unique_skus INTEGER DEFAULT 0,
    total_sku_quantity INTEGER DEFAULT 0,
    
    -- Date Range for this aggregation
    analysis_date_start DATE NOT NULL,
    analysis_date_end DATE NOT NULL,
    
    -- Audit Fields
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    data_source VARCHAR(100) DEFAULT 'attribution_engine',
    batch_id VARCHAR(100) NOT NULL, -- For tracking ADF pipeline runs
    
    -- Constraints
    CONSTRAINT idx_attribution_ad_level_unique UNIQUE (campaign_id, adset_id, ad_id, analysis_date_start, analysis_date_end, batch_id),
    CONSTRAINT idx_attribution_ad_level_orders CHECK (orders >= 0),
    CONSTRAINT idx_attribution_ad_level_attributed_orders CHECK (attributed_orders >= 0 AND attributed_orders <= orders),
    CONSTRAINT idx_attribution_ad_level_spend CHECK (spend >= 0),
    CONSTRAINT idx_attribution_ad_level_date_range CHECK (analysis_date_end >= analysis_date_start)
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_attribution_ad_level_campaign_id ON attribution_ad_level_analytics (campaign_id);
CREATE INDEX IF NOT EXISTS idx_attribution_ad_level_ad_id ON attribution_ad_level_analytics (ad_id);
CREATE INDEX IF NOT EXISTS idx_attribution_ad_level_channel ON attribution_ad_level_analytics (channel);
CREATE INDEX IF NOT EXISTS idx_attribution_ad_level_attributed ON attribution_ad_level_analytics (attributed);
CREATE INDEX IF NOT EXISTS idx_attribution_ad_level_date_range ON attribution_ad_level_analytics (analysis_date_start, analysis_date_end);
CREATE INDEX IF NOT EXISTS idx_attribution_ad_level_batch_id ON attribution_ad_level_analytics (batch_id);
CREATE INDEX IF NOT EXISTS idx_attribution_ad_level_spend ON attribution_ad_level_analytics (spend);

-- =====================================================
-- 3. PIPELINE EXECUTION LOG TABLE
-- =====================================================
-- Tracks ADF pipeline execution and data quality metrics
-- =====================================================

CREATE TABLE IF NOT EXISTS attribution_pipeline_log (
    -- Primary Key
    id BIGSERIAL PRIMARY KEY,
    
    -- Pipeline Information
    pipeline_name VARCHAR(255) NOT NULL,
    pipeline_run_id VARCHAR(255) NOT NULL,
    batch_id VARCHAR(100) NOT NULL,
    
    -- Execution Details
    execution_start_time TIMESTAMP WITH TIME ZONE NOT NULL,
    execution_end_time TIMESTAMP WITH TIME ZONE,
    execution_status VARCHAR(50) NOT NULL, -- 'RUNNING', 'SUCCESS', 'FAILED', 'CANCELLED'
    
    -- Data Processing Metrics
    total_orders_processed INTEGER DEFAULT 0,
    total_ads_processed INTEGER DEFAULT 0,
    attributed_orders_count INTEGER DEFAULT 0,
    unattributed_orders_count INTEGER DEFAULT 0,
    
    -- Data Quality Metrics
    total_spend_processed DECIMAL(15,2) DEFAULT 0,
    total_sales_processed DECIMAL(15,2) DEFAULT 0,
    data_quality_score DECIMAL(5,2) DEFAULT 0, -- 0-100 score
    
    -- Error Information
    error_message TEXT,
    error_details JSONB,
    
    -- Configuration
    analysis_date_start DATE NOT NULL,
    analysis_date_end DATE NOT NULL,
    time_range_days INTEGER,
    
    -- Audit Fields
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    
    -- Constraints
    CONSTRAINT idx_pipeline_log_unique UNIQUE (pipeline_run_id),
    CONSTRAINT idx_pipeline_log_status CHECK (execution_status IN ('RUNNING', 'SUCCESS', 'FAILED', 'CANCELLED')),
    CONSTRAINT idx_pipeline_log_date_range CHECK (analysis_date_end >= analysis_date_start)
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_pipeline_log_pipeline_name ON attribution_pipeline_log (pipeline_name);
CREATE INDEX IF NOT EXISTS idx_pipeline_log_batch_id ON attribution_pipeline_log (batch_id);
CREATE INDEX IF NOT EXISTS idx_pipeline_log_execution_start ON attribution_pipeline_log (execution_start_time);
CREATE INDEX IF NOT EXISTS idx_pipeline_log_status ON attribution_pipeline_log (execution_status);
CREATE INDEX IF NOT EXISTS idx_pipeline_log_date_range ON attribution_pipeline_log (analysis_date_start, analysis_date_end);

-- =====================================================
-- 4. ANALYTICAL VIEWS
-- =====================================================
-- Pre-aggregated views for common analytical queries
-- =====================================================

-- View: Daily Attribution Summary
CREATE OR REPLACE VIEW v_attribution_daily_summary AS
SELECT 
    DATE(order_date) as analysis_date,
    channel,
    COUNT(*) as total_orders,
    SUM(CASE WHEN is_attributed THEN 1 ELSE 0 END) as attributed_orders,
    ROUND(
        (SUM(CASE WHEN is_attributed THEN 1 ELSE 0 END)::DECIMAL / COUNT(*)) * 100, 2
    ) as attribution_rate_percent,
    SUM(order_value) as total_revenue,
    SUM(total_cogs) as total_cogs,
    SUM(order_value - total_cogs) as total_profit,
    COUNT(DISTINCT campaign_id) as unique_campaigns,
    COUNT(DISTINCT ad_id) as unique_ads
FROM attribution_raw_data
GROUP BY DATE(order_date), channel
ORDER BY analysis_date DESC, total_revenue DESC;

-- View: Campaign Performance Summary
CREATE OR REPLACE VIEW v_campaign_performance_summary AS
SELECT 
    campaign_id,
    campaign_name,
    channel,
    SUM(orders) as total_orders,
    SUM(attributed_orders) as total_attributed_orders,
    SUM(total_sales) as total_sales,
    SUM(spend) as total_spend,
    SUM(net_profit) as total_net_profit,
    CASE 
        WHEN SUM(spend) > 0 THEN ROUND((SUM(total_sales) / SUM(spend))::DECIMAL, 4)
        ELSE 0 
    END as roas,
    CASE 
        WHEN SUM(impressions) > 0 THEN ROUND((SUM(clicks)::DECIMAL / SUM(impressions)) * 100, 4)
        ELSE 0 
    END as ctr_percent,
    CASE 
        WHEN SUM(clicks) > 0 THEN ROUND((SUM(orders)::DECIMAL / SUM(clicks)) * 100, 4)
        ELSE 0 
    END as conversion_rate_percent,
    CASE 
        WHEN SUM(orders) > 0 THEN ROUND((SUM(total_sales) / SUM(orders))::DECIMAL, 2)
        ELSE 0 
    END as avg_order_value,
    CASE 
        WHEN SUM(total_sales) > 0 THEN ROUND((SUM(net_profit) / SUM(total_sales)) * 100, 2)
        ELSE 0 
    END as profit_margin_percent
FROM attribution_ad_level_analytics
GROUP BY campaign_id, campaign_name, channel
ORDER BY total_sales DESC;

-- View: Ad Performance Summary
CREATE OR REPLACE VIEW v_ad_performance_summary AS
SELECT 
    ad_id,
    ad_name,
    campaign_name,
    adset_name,
    channel,
    attributed,
    orders,
    attributed_orders,
    total_sales,
    spend,
    net_profit,
    roas,
    ctr,
    conversion_rate,
    avg_order_value,
    profit_margin,
    total_unique_skus,
    analysis_date_start,
    analysis_date_end,
    created_at
FROM attribution_ad_level_analytics
ORDER BY total_sales DESC, spend DESC;

-- View: Channel Performance Summary
CREATE OR REPLACE VIEW v_channel_performance_summary AS
SELECT 
    channel,
    COUNT(DISTINCT campaign_id) as unique_campaigns,
    COUNT(DISTINCT ad_id) as unique_ads,
    SUM(orders) as total_orders,
    SUM(attributed_orders) as total_attributed_orders,
    ROUND(
        (SUM(attributed_orders)::DECIMAL / NULLIF(SUM(orders), 0)) * 100, 2
    ) as attribution_rate_percent,
    SUM(total_sales) as total_sales,
    SUM(spend) as total_spend,
    SUM(net_profit) as total_net_profit,
    CASE 
        WHEN SUM(spend) > 0 THEN ROUND((SUM(total_sales) / SUM(spend))::DECIMAL, 4)
        ELSE 0 
    END as roas,
    CASE 
        WHEN SUM(total_sales) > 0 THEN ROUND((SUM(net_profit) / SUM(total_sales)) * 100, 2)
        ELSE 0 
    END as profit_margin_percent
FROM attribution_ad_level_analytics
GROUP BY channel
ORDER BY total_sales DESC;

-- =====================================================
-- 5. DATA QUALITY FUNCTIONS
-- =====================================================
-- Functions for data validation and quality checks
-- =====================================================

-- Function: Calculate data quality score
CREATE OR REPLACE FUNCTION calculate_attribution_data_quality_score(
    p_batch_id VARCHAR(100)
) RETURNS DECIMAL(5,2) AS $$
DECLARE
    total_records INTEGER;
    quality_score DECIMAL(5,2) := 0;
    completeness_score DECIMAL(5,2);
    consistency_score DECIMAL(5,2);
    accuracy_score DECIMAL(5,2);
BEGIN
    -- Get total records for this batch
    SELECT COUNT(*) INTO total_records
    FROM attribution_raw_data
    WHERE batch_id = p_batch_id;
    
    IF total_records = 0 THEN
        RETURN 0;
    END IF;
    
    -- Calculate completeness score (non-null required fields)
    SELECT ROUND(
        (COUNT(*)::DECIMAL / total_records) * 100, 2
    ) INTO completeness_score
    FROM attribution_raw_data
    WHERE batch_id = p_batch_id
    AND order_id IS NOT NULL
    AND order_date IS NOT NULL
    AND order_value IS NOT NULL
    AND order_value > 0;
    
    -- Calculate consistency score (attribution logic consistency)
    SELECT ROUND(
        (COUNT(*)::DECIMAL / total_records) * 100, 2
    ) INTO consistency_score
    FROM attribution_raw_data
    WHERE batch_id = p_batch_id
    AND (
        (is_attributed = TRUE AND attribution_source IS NOT NULL) OR
        (is_attributed = FALSE AND attribution_source IS NULL)
    );
    
    -- Calculate accuracy score (financial data consistency)
    SELECT ROUND(
        (COUNT(*)::DECIMAL / total_records) * 100, 2
    ) INTO accuracy_score
    FROM attribution_raw_data
    WHERE batch_id = p_batch_id
    AND order_value >= total_cogs
    AND total_cogs >= 0;
    
    -- Calculate overall quality score (weighted average)
    quality_score := (completeness_score * 0.4) + (consistency_score * 0.3) + (accuracy_score * 0.3);
    
    RETURN LEAST(quality_score, 100);
END;
$$ LANGUAGE plpgsql;

-- =====================================================
-- 6. SAMPLE QUERIES FOR TESTING
-- =====================================================
-- Common analytical queries for validation
-- =====================================================

/*
-- Query 1: Daily Attribution Performance
SELECT * FROM v_attribution_daily_summary 
WHERE analysis_date >= CURRENT_DATE - INTERVAL '7 days'
ORDER BY analysis_date DESC;

-- Query 2: Top Performing Campaigns
SELECT * FROM v_campaign_performance_summary 
WHERE total_sales > 10000
ORDER BY roas DESC
LIMIT 10;

-- Query 3: Channel Performance Comparison
SELECT * FROM v_channel_performance_summary
ORDER BY total_sales DESC;

-- Query 4: Data Quality Check
SELECT 
    batch_id,
    COUNT(*) as total_records,
    calculate_attribution_data_quality_score(batch_id) as quality_score
FROM attribution_raw_data
WHERE created_at >= CURRENT_DATE - INTERVAL '1 day'
GROUP BY batch_id
ORDER BY created_at DESC;

-- Query 5: Attribution Rate by Channel
SELECT 
    channel,
    COUNT(*) as total_orders,
    SUM(CASE WHEN is_attributed THEN 1 ELSE 0 END) as attributed_orders,
    ROUND(
        (SUM(CASE WHEN is_attributed THEN 1 ELSE 0 END)::DECIMAL / COUNT(*)) * 100, 2
    ) as attribution_rate_percent
FROM attribution_raw_data
WHERE order_date >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY channel
ORDER BY attribution_rate_percent DESC;
*/

-- =====================================================
-- SCHEMA CREATION COMPLETE
-- =====================================================
-- This schema provides:
-- 1. Raw attribution data storage
-- 2. Ad-level analytics aggregation
-- 3. Pipeline execution tracking
-- 4. Pre-built analytical views
-- 5. Data quality functions
-- 6. Performance-optimized indexes
-- =====================================================
