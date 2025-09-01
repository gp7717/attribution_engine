-- =====================================================
-- UTM DATA MODEL FOR ATTRIBUTION ANALYSIS
-- =====================================================
-- This schema stores UTM tracking data for customer journeys
-- Designed to be joined with ads_insights_hourly for hourly purchase/sale analysis
-- =====================================================

-- =====================================================
-- 1. UTM SESSION TRACKING TABLE
-- =====================================================
-- Stores individual UTM sessions/visits with timestamps
-- This is the core table for tracking customer journeys
-- =====================================================

CREATE TABLE IF NOT EXISTS utm_sessions (
    -- Primary Key
    session_id BIGSERIAL PRIMARY KEY,
    
    -- Session Information
    session_token VARCHAR(255) UNIQUE NOT NULL, -- Unique session identifier
    visitor_id VARCHAR(255), -- Anonymous visitor identifier (cookie-based)
    customer_id VARCHAR(255), -- Shopify customer ID if known
    
    -- UTM Parameters (from URL tracking)
    utm_source VARCHAR(255),
    utm_medium VARCHAR(255),
    utm_campaign VARCHAR(255),
    utm_content VARCHAR(255),
    utm_term VARCHAR(255),
    
    -- Additional Tracking Parameters
    referrer_url TEXT,
    landing_page_url TEXT,
    device_type VARCHAR(50), -- 'desktop', 'mobile', 'tablet'
    browser VARCHAR(100),
    operating_system VARCHAR(100),
    country VARCHAR(100),
    city VARCHAR(100),
    
    -- Campaign Information (derived from UTM)
    campaign_id VARCHAR(255), -- Facebook/Google campaign ID if available
    adset_id VARCHAR(255), -- Facebook adset ID if available
    ad_id VARCHAR(255), -- Facebook/Google ad ID if available
    
    -- Timestamps
    session_start_time TIMESTAMP WITH TIME ZONE NOT NULL,
    session_end_time TIMESTAMP WITH TIME ZONE,
    first_pageview_time TIMESTAMP WITH TIME ZONE NOT NULL,
    last_pageview_time TIMESTAMP WITH TIME ZONE,
    
    -- Session Metrics
    pageviews_count INTEGER DEFAULT 1,
    time_on_site_seconds INTEGER DEFAULT 0,
    
    -- Attribution Flags
    is_conversion_session BOOLEAN DEFAULT FALSE,
    conversion_type VARCHAR(50), -- 'purchase', 'add_to_cart', 'checkout_start', etc.
    conversion_value DECIMAL(15,2) DEFAULT 0,
    conversion_currency VARCHAR(10) DEFAULT 'INR',
    
    -- Data Quality
    is_bot_traffic BOOLEAN DEFAULT FALSE,
    data_source VARCHAR(100) DEFAULT 'shopify_analytics',
    
    -- Audit Fields
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    
    -- Constraints
    CONSTRAINT idx_utm_sessions_session_token UNIQUE (session_token),
    CONSTRAINT idx_utm_sessions_time_check CHECK (session_start_time <= session_end_time),
    CONSTRAINT idx_utm_sessions_conversion_value CHECK (conversion_value >= 0)
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_utm_sessions_visitor_id ON utm_sessions (visitor_id);
CREATE INDEX IF NOT EXISTS idx_utm_sessions_customer_id ON utm_sessions (customer_id);
CREATE INDEX IF NOT EXISTS idx_utm_sessions_campaign_id ON utm_sessions (campaign_id);
CREATE INDEX IF NOT EXISTS idx_utm_sessions_ad_id ON utm_sessions (ad_id);
CREATE INDEX IF NOT EXISTS idx_utm_sessions_session_start ON utm_sessions (session_start_time);
CREATE INDEX IF NOT EXISTS idx_utm_sessions_utm_source ON utm_sessions (utm_source);
CREATE INDEX IF NOT EXISTS idx_utm_sessions_utm_campaign ON utm_sessions (utm_campaign);
CREATE INDEX IF NOT EXISTS idx_utm_sessions_conversion ON utm_sessions (is_conversion_session, conversion_type);
CREATE INDEX IF NOT EXISTS idx_utm_sessions_date_range ON utm_sessions (DATE(session_start_time));

-- =====================================================
-- 2. UTM ORDER MAPPING TABLE
-- =====================================================
-- Maps UTM sessions to actual Shopify orders
-- This enables joining UTM data with order data for revenue attribution
-- =====================================================

CREATE TABLE IF NOT EXISTS utm_order_mapping (
    -- Primary Key
    id BIGSERIAL PRIMARY KEY,
    
    -- Order Information
    order_id VARCHAR(255) NOT NULL,
    order_name VARCHAR(255),
    order_date TIMESTAMP WITH TIME ZONE NOT NULL,
    order_value DECIMAL(15,2) NOT NULL,
    order_currency VARCHAR(10) DEFAULT 'INR',
    
    -- UTM Session Information
    session_id BIGINT NOT NULL,
    session_token VARCHAR(255) NOT NULL,
    
    -- Attribution Details
    attribution_model VARCHAR(50) DEFAULT 'last_click', -- 'first_click', 'last_click', 'linear', 'time_decay'
    attribution_weight DECIMAL(5,4) DEFAULT 1.0000, -- Weight for multi-touch attribution
    is_primary_attribution BOOLEAN DEFAULT FALSE, -- True for the main attribution source
    
    -- UTM Parameters (copied from session for quick access)
    utm_source VARCHAR(255),
    utm_medium VARCHAR(255),
    utm_campaign VARCHAR(255),
    utm_content VARCHAR(255),
    utm_term VARCHAR(255),
    
    -- Campaign Information
    campaign_id VARCHAR(255),
    campaign_name VARCHAR(500),
    adset_id VARCHAR(255),
    adset_name VARCHAR(500),
    ad_id VARCHAR(255),
    ad_name VARCHAR(500),
    
    -- Channel Classification
    channel VARCHAR(100),
    sub_channel VARCHAR(100),
    
    -- Audit Fields
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    data_source VARCHAR(100) DEFAULT 'attribution_engine',
    batch_id VARCHAR(100), -- For tracking data processing runs
    
    -- Constraints
    CONSTRAINT idx_utm_order_mapping_order_id UNIQUE (order_id),
    CONSTRAINT idx_utm_order_mapping_session_fk FOREIGN KEY (session_id) REFERENCES utm_sessions(session_id),
    CONSTRAINT idx_utm_order_mapping_order_value CHECK (order_value > 0),
    CONSTRAINT idx_utm_order_mapping_attribution_weight CHECK (attribution_weight >= 0 AND attribution_weight <= 1)
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_utm_order_mapping_session_id ON utm_order_mapping (session_id);
CREATE INDEX IF NOT EXISTS idx_utm_order_mapping_order_date ON utm_order_mapping (order_date);
CREATE INDEX IF NOT EXISTS idx_utm_order_mapping_campaign_id ON utm_order_mapping (campaign_id);
CREATE INDEX IF NOT EXISTS idx_utm_order_mapping_ad_id ON utm_order_mapping (ad_id);
CREATE INDEX IF NOT EXISTS idx_utm_order_mapping_channel ON utm_order_mapping (channel);
CREATE INDEX IF NOT EXISTS idx_utm_order_mapping_batch_id ON utm_order_mapping (batch_id);

-- =====================================================
-- 3. UTM HOURLY AGGREGATION TABLE
-- =====================================================
-- Pre-aggregated UTM data by hour for fast joins with ads_insights_hourly
-- This table is designed specifically for hourly purchase/sale analysis
-- =====================================================

CREATE TABLE IF NOT EXISTS utm_hourly_metrics (
    -- Primary Key
    id BIGSERIAL PRIMARY KEY,
    
    -- Time Dimension
    date_start DATE NOT NULL,
    hour_start INTEGER NOT NULL, -- 0-23 hour of day
    hour_window VARCHAR(20) NOT NULL, -- '00:00:00 - 00:59:59' format to match ads_insights_hourly
    
    -- Campaign Hierarchy
    campaign_id VARCHAR(255),
    campaign_name VARCHAR(500),
    adset_id VARCHAR(255),
    adset_name VARCHAR(500),
    ad_id VARCHAR(255),
    ad_name VARCHAR(500),
    
    -- UTM Parameters
    utm_source VARCHAR(255),
    utm_medium VARCHAR(255),
    utm_campaign VARCHAR(255),
    utm_content VARCHAR(255),
    utm_term VARCHAR(255),
    
    -- Channel Classification
    channel VARCHAR(100),
    sub_channel VARCHAR(100),
    
    -- Session Metrics
    total_sessions INTEGER DEFAULT 0,
    unique_visitors INTEGER DEFAULT 0,
    unique_customers INTEGER DEFAULT 0,
    
    -- Conversion Metrics
    total_conversions INTEGER DEFAULT 0,
    conversion_rate DECIMAL(8,4) DEFAULT 0, -- Percentage
    
    -- Revenue Metrics
    total_revenue DECIMAL(15,2) DEFAULT 0,
    total_orders INTEGER DEFAULT 0,
    avg_order_value DECIMAL(15,2) DEFAULT 0,
    
    -- Engagement Metrics
    total_pageviews INTEGER DEFAULT 0,
    avg_session_duration_seconds INTEGER DEFAULT 0,
    bounce_rate DECIMAL(8,4) DEFAULT 0, -- Percentage
    
    -- Attribution Metrics
    attributed_revenue DECIMAL(15,2) DEFAULT 0,
    attributed_orders INTEGER DEFAULT 0,
    attribution_rate DECIMAL(8,4) DEFAULT 0, -- Percentage
    
    -- Data Quality
    data_completeness_score DECIMAL(5,2) DEFAULT 0, -- 0-100 score
    
    -- Audit Fields
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    data_source VARCHAR(100) DEFAULT 'utm_aggregation',
    batch_id VARCHAR(100) NOT NULL,
    
    -- Constraints
    CONSTRAINT idx_utm_hourly_unique UNIQUE (date_start, hour_start, campaign_id, ad_id, utm_source, utm_medium, utm_campaign, batch_id),
    CONSTRAINT idx_utm_hourly_hour_check CHECK (hour_start >= 0 AND hour_start <= 23),
    CONSTRAINT idx_utm_hourly_metrics_check CHECK (total_sessions >= 0 AND total_revenue >= 0)
);

-- Indexes for performance (optimized for joins with ads_insights_hourly)
CREATE INDEX IF NOT EXISTS idx_utm_hourly_date_hour ON utm_hourly_metrics (date_start, hour_start);
CREATE INDEX IF NOT EXISTS idx_utm_hourly_campaign_id ON utm_hourly_metrics (campaign_id);
CREATE INDEX IF NOT EXISTS idx_utm_hourly_ad_id ON utm_hourly_metrics (ad_id);
CREATE INDEX IF NOT EXISTS idx_utm_hourly_channel ON utm_hourly_metrics (channel);
CREATE INDEX IF NOT EXISTS idx_utm_hourly_utm_source ON utm_hourly_metrics (utm_source);
CREATE INDEX IF NOT EXISTS idx_utm_hourly_utm_campaign ON utm_hourly_metrics (utm_campaign);
CREATE INDEX IF NOT EXISTS idx_utm_hourly_batch_id ON utm_hourly_metrics (batch_id);

-- =====================================================
-- 4. UTM CHANNEL MAPPING TABLE
-- =====================================================
-- Configurable mapping of UTM parameters to marketing channels
-- This enables consistent channel classification across the system
-- =====================================================

CREATE TABLE IF NOT EXISTS utm_channel_mapping (
    -- Primary Key
    id BIGSERIAL PRIMARY KEY,
    
    -- UTM Pattern Matching
    utm_source_pattern VARCHAR(255) NOT NULL, -- Can use regex patterns
    utm_medium_pattern VARCHAR(255),
    utm_campaign_pattern VARCHAR(255),
    utm_content_pattern VARCHAR(255),
    
    -- Channel Classification
    channel VARCHAR(100) NOT NULL,
    sub_channel VARCHAR(100),
    channel_category VARCHAR(100), -- 'paid', 'organic', 'direct', 'social', 'email'
    
    -- Campaign Platform
    platform VARCHAR(100), -- 'facebook', 'google', 'instagram', 'tiktok', etc.
    platform_campaign_id_field VARCHAR(100), -- Which UTM field contains the platform campaign ID
    
    -- Priority (for overlapping patterns)
    priority INTEGER DEFAULT 1, -- Higher number = higher priority
    
    -- Status
    is_active BOOLEAN DEFAULT TRUE,
    
    -- Audit Fields
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    created_by VARCHAR(100) DEFAULT 'system',
    
    -- Constraints
    CONSTRAINT idx_utm_channel_mapping_unique UNIQUE (utm_source_pattern, utm_medium_pattern, utm_campaign_pattern, priority),
    CONSTRAINT idx_utm_channel_mapping_priority CHECK (priority > 0)
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_utm_channel_mapping_channel ON utm_channel_mapping (channel);
CREATE INDEX IF NOT EXISTS idx_utm_channel_mapping_platform ON utm_channel_mapping (platform);
CREATE INDEX IF NOT EXISTS idx_utm_channel_mapping_active ON utm_channel_mapping (is_active);

-- =====================================================
-- 5. ANALYTICAL VIEWS FOR HOURLY ANALYSIS
-- =====================================================
-- Pre-built views for common UTM + Ad Insights analysis
-- =====================================================

-- View: Hourly UTM + Ad Performance (Main View for Analysis)
CREATE OR REPLACE VIEW v_utm_ad_hourly_performance AS
SELECT 
    -- Time Dimensions
    u.date_start,
    u.hour_start,
    u.hour_window,
    
    -- Campaign Information
    COALESCE(u.campaign_id, a.campaign_id) as campaign_id,
    COALESCE(u.campaign_name, a.campaign_name) as campaign_name,
    COALESCE(u.adset_id, a.adset_id) as adset_id,
    COALESCE(u.adset_name, a.adset_name) as adset_name,
    COALESCE(u.ad_id, a.ad_id) as ad_id,
    COALESCE(u.ad_name, a.ad_name) as ad_name,
    
    -- UTM Parameters
    u.utm_source,
    u.utm_medium,
    u.utm_campaign,
    u.utm_content,
    u.utm_term,
    
    -- Channel Classification
    u.channel,
    u.sub_channel,
    
    -- Ad Performance Metrics (from ads_insights_hourly)
    COALESCE(a.impressions, 0) as ad_impressions,
    COALESCE(a.clicks, 0) as ad_clicks,
    COALESCE(a.spend, 0) as ad_spend,
    COALESCE(a.cpm, 0) as ad_cpm,
    COALESCE(a.cpc, 0) as ad_cpc,
    COALESCE(a.ctr, 0) as ad_ctr,
    
    -- UTM Session Metrics
    COALESCE(u.total_sessions, 0) as utm_sessions,
    COALESCE(u.unique_visitors, 0) as utm_visitors,
    COALESCE(u.unique_customers, 0) as utm_customers,
    
    -- Conversion & Revenue Metrics
    COALESCE(u.total_conversions, 0) as utm_conversions,
    COALESCE(u.conversion_rate, 0) as utm_conversion_rate,
    COALESCE(u.total_revenue, 0) as utm_revenue,
    COALESCE(u.total_orders, 0) as utm_orders,
    COALESCE(u.avg_order_value, 0) as utm_avg_order_value,
    
    -- Attribution Metrics
    COALESCE(u.attributed_revenue, 0) as attributed_revenue,
    COALESCE(u.attributed_orders, 0) as attributed_orders,
    COALESCE(u.attribution_rate, 0) as attribution_rate,
    
    -- Calculated Performance Metrics
    CASE 
        WHEN COALESCE(a.spend, 0) > 0 THEN ROUND((COALESCE(u.attributed_revenue, 0) / a.spend)::DECIMAL, 4)
        ELSE 0 
    END as roas,
    
    CASE 
        WHEN COALESCE(a.clicks, 0) > 0 THEN ROUND((COALESCE(u.utm_conversions, 0)::DECIMAL / a.clicks) * 100, 4)
        ELSE 0 
    END as conversion_rate_percent,
    
    CASE 
        WHEN COALESCE(u.utm_sessions, 0) > 0 THEN ROUND((COALESCE(u.utm_conversions, 0)::DECIMAL / u.utm_sessions) * 100, 4)
        ELSE 0 
    END as session_to_conversion_rate,
    
    -- Data Quality
    COALESCE(u.data_completeness_score, 0) as utm_data_quality,
    CASE 
        WHEN a.campaign_id IS NOT NULL AND u.campaign_id IS NOT NULL THEN 'matched'
        WHEN a.campaign_id IS NOT NULL THEN 'ads_only'
        WHEN u.campaign_id IS NOT NULL THEN 'utm_only'
        ELSE 'unmatched'
    END as data_match_status,
    
    -- Audit Information
    u.batch_id as utm_batch_id,
    a.date_start as ads_date_start,
    u.created_at as utm_created_at,
    a.date_start as ads_created_at
    
FROM utm_hourly_metrics u
FULL OUTER JOIN ads_insights_hourly a ON 
    u.date_start = a.date_start 
    AND u.hour_start = EXTRACT(hour FROM a.hourly_window::time)
    AND COALESCE(u.campaign_id, '') = COALESCE(a.campaign_id, '')
    AND COALESCE(u.ad_id, '') = COALESCE(a.ad_id, '')

WHERE u.date_start >= CURRENT_DATE - INTERVAL '30 days'
ORDER BY u.date_start DESC, u.hour_start DESC, u.utm_revenue DESC;

-- View: Daily UTM Performance Summary
CREATE OR REPLACE VIEW v_utm_daily_summary AS
SELECT 
    date_start,
    channel,
    sub_channel,
    utm_source,
    utm_campaign,
    
    -- Daily Aggregations
    SUM(utm_sessions) as total_sessions,
    SUM(utm_visitors) as total_visitors,
    SUM(utm_customers) as total_customers,
    SUM(utm_conversions) as total_conversions,
    SUM(utm_revenue) as total_revenue,
    SUM(utm_orders) as total_orders,
    SUM(ad_spend) as total_ad_spend,
    
    -- Calculated Metrics
    CASE 
        WHEN SUM(utm_sessions) > 0 THEN ROUND((SUM(utm_conversions)::DECIMAL / SUM(utm_sessions)) * 100, 4)
        ELSE 0 
    END as conversion_rate_percent,
    
    CASE 
        WHEN SUM(ad_spend) > 0 THEN ROUND((SUM(utm_revenue) / SUM(ad_spend))::DECIMAL, 4)
        ELSE 0 
    END as roas,
    
    CASE 
        WHEN SUM(utm_orders) > 0 THEN ROUND((SUM(utm_revenue) / SUM(utm_orders))::DECIMAL, 2)
        ELSE 0 
    END as avg_order_value,
    
    -- Attribution Metrics
    SUM(attributed_revenue) as total_attributed_revenue,
    SUM(attributed_orders) as total_attributed_orders,
    
    CASE 
        WHEN SUM(utm_revenue) > 0 THEN ROUND((SUM(attributed_revenue) / SUM(utm_revenue)) * 100, 4)
        ELSE 0 
    END as attribution_rate_percent
    
FROM v_utm_ad_hourly_performance
GROUP BY date_start, channel, sub_channel, utm_source, utm_campaign
ORDER BY date_start DESC, total_revenue DESC;

-- =====================================================
-- 6. FUNCTIONS FOR UTM DATA PROCESSING
-- =====================================================
-- Helper functions for UTM data management and analysis
-- =====================================================

-- Function: Get channel classification based on UTM parameters
CREATE OR REPLACE FUNCTION get_utm_channel(
    p_utm_source VARCHAR(255),
    p_utm_medium VARCHAR(255),
    p_utm_campaign VARCHAR(255),
    p_utm_content VARCHAR(255)
) RETURNS TABLE(
    channel VARCHAR(100),
    sub_channel VARCHAR(100),
    channel_category VARCHAR(100),
    platform VARCHAR(100)
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        ucm.channel,
        ucm.sub_channel,
        ucm.channel_category,
        ucm.platform
    FROM utm_channel_mapping ucm
    WHERE ucm.is_active = TRUE
    AND (
        p_utm_source ~ ucm.utm_source_pattern OR 
        (ucm.utm_source_pattern IS NULL AND p_utm_source IS NULL)
    )
    AND (
        p_utm_medium ~ ucm.utm_medium_pattern OR 
        (ucm.utm_medium_pattern IS NULL AND p_utm_medium IS NULL)
    )
    AND (
        p_utm_campaign ~ ucm.utm_campaign_pattern OR 
        (ucm.utm_campaign_pattern IS NULL AND p_utm_campaign IS NULL)
    )
    AND (
        p_utm_content ~ ucm.utm_content_pattern OR 
        (ucm.utm_content_pattern IS NULL AND p_utm_content IS NULL)
    )
    ORDER BY ucm.priority DESC
    LIMIT 1;
END;
$$ LANGUAGE plpgsql;

-- Function: Calculate UTM attribution for a given time period
CREATE OR REPLACE FUNCTION calculate_utm_attribution(
    p_start_date DATE,
    p_end_date DATE,
    p_attribution_model VARCHAR(50) DEFAULT 'last_click'
) RETURNS TABLE(
    date_start DATE,
    hour_start INTEGER,
    campaign_id VARCHAR(255),
    ad_id VARCHAR(255),
    utm_source VARCHAR(255),
    utm_medium VARCHAR(255),
    utm_campaign VARCHAR(255),
    channel VARCHAR(100),
    sessions INTEGER,
    conversions INTEGER,
    revenue DECIMAL(15,2),
    orders INTEGER
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        u.date_start,
        u.hour_start,
        u.campaign_id,
        u.ad_id,
        u.utm_source,
        u.utm_medium,
        u.utm_campaign,
        u.channel,
        u.total_sessions as sessions,
        u.total_conversions as conversions,
        u.total_revenue as revenue,
        u.total_orders as orders
    FROM utm_hourly_metrics u
    WHERE u.date_start >= p_start_date 
    AND u.date_start <= p_end_date
    ORDER BY u.date_start DESC, u.hour_start DESC;
END;
$$ LANGUAGE plpgsql;

-- =====================================================
-- 7. SAMPLE DATA INSERTION FOR TESTING
-- =====================================================
-- Insert sample UTM channel mappings for common scenarios
-- =====================================================

-- Insert default UTM channel mappings
INSERT INTO utm_channel_mapping (utm_source_pattern, utm_medium_pattern, utm_campaign_pattern, channel, sub_channel, channel_category, platform, priority) VALUES
-- Facebook/Instagram
('facebook|fb', 'cpc|paid', NULL, 'Facebook', 'Paid Social', 'paid', 'facebook', 10),
('instagram|ig', 'cpc|paid', NULL, 'Instagram', 'Paid Social', 'paid', 'instagram', 10),

-- Google Ads
('google', 'cpc|paid', NULL, 'Google', 'Paid Search', 'paid', 'google', 10),
('google', 'display', NULL, 'Google', 'Display', 'paid', 'google', 9),

-- Organic Social
('facebook|fb', 'social|organic', NULL, 'Facebook', 'Organic Social', 'organic', 'facebook', 5),
('instagram|ig', 'social|organic', NULL, 'Instagram', 'Organic Social', 'organic', 'instagram', 5),

-- Email Marketing
('email|mail', 'email', NULL, 'Email', 'Newsletter', 'email', 'email', 8),

-- Direct Traffic
('direct|(null)|^$', NULL, NULL, 'Direct', 'Direct', 'direct', 'direct', 1),

-- Referral Traffic
('referral', NULL, NULL, 'Referral', 'External Sites', 'organic', 'referral', 3)

ON CONFLICT (utm_source_pattern, utm_medium_pattern, utm_campaign_pattern, priority) 
DO NOTHING;

-- =====================================================
-- 8. SAMPLE QUERIES FOR HOURLY ANALYSIS
-- =====================================================
-- Example queries showing how to use the UTM data model
-- =====================================================

/*
-- Query 1: Hourly UTM + Ad Performance (Main Analysis)
SELECT * FROM v_utm_ad_hourly_performance 
WHERE date_start = CURRENT_DATE - INTERVAL '1 day'
AND channel = 'Facebook'
ORDER BY hour_start DESC;

-- Query 2: Daily UTM Performance Summary
SELECT * FROM v_utm_daily_summary 
WHERE date_start >= CURRENT_DATE - INTERVAL '7 days'
ORDER BY date_start DESC, total_revenue DESC;

-- Query 3: Campaign Performance with UTM Attribution
SELECT 
    campaign_name,
    channel,
    SUM(utm_revenue) as total_revenue,
    SUM(ad_spend) as total_spend,
    CASE 
        WHEN SUM(ad_spend) > 0 THEN ROUND((SUM(utm_revenue) / SUM(ad_spend))::DECIMAL, 4)
        ELSE 0 
    END as roas,
    SUM(utm_conversions) as total_conversions,
    SUM(utm_sessions) as total_sessions
FROM v_utm_ad_hourly_performance
WHERE date_start >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY campaign_name, channel
HAVING SUM(utm_revenue) > 0
ORDER BY total_revenue DESC;

-- Query 4: UTM Source Performance Analysis
SELECT 
    utm_source,
    utm_medium,
    utm_campaign,
    COUNT(DISTINCT date_start) as active_days,
    SUM(utm_sessions) as total_sessions,
    SUM(utm_conversions) as total_conversions,
    SUM(utm_revenue) as total_revenue,
    CASE 
        WHEN SUM(utm_sessions) > 0 THEN ROUND((SUM(utm_conversions)::DECIMAL / SUM(utm_sessions)) * 100, 4)
        ELSE 0 
    END as conversion_rate_percent
FROM v_utm_ad_hourly_performance
WHERE date_start >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY utm_source, utm_medium, utm_campaign
HAVING SUM(utm_revenue) > 0
ORDER BY total_revenue DESC;

-- Query 5: Hourly Conversion Patterns
SELECT 
    hour_start,
    SUM(utm_conversions) as total_conversions,
    SUM(utm_revenue) as total_revenue,
    AVG(utm_avg_order_value) as avg_order_value,
    COUNT(DISTINCT campaign_id) as active_campaigns
FROM v_utm_ad_hourly_performance
WHERE date_start >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY hour_start
ORDER BY hour_start;
*/

-- =====================================================
-- SCHEMA CREATION COMPLETE
-- =====================================================
-- This UTM data model provides:
-- 1. Session-level UTM tracking with full customer journey data
-- 2. Order mapping for revenue attribution
-- 3. Hourly aggregation optimized for ads_insights_hourly joins
-- 4. Configurable channel mapping system
-- 5. Pre-built analytical views for common analysis
-- 6. Helper functions for data processing
-- 7. Performance-optimized indexes for fast queries
-- =====================================================
