-- =====================================================
-- EXAMPLE QUERIES FOR UTM DATA MODEL
-- =====================================================
-- These queries demonstrate how to use the UTM data model
-- to get hourly purchase/sale analysis by joining with ads_insights_hourly
-- =====================================================

-- =====================================================
-- 1. BASIC HOURLY ANALYSIS
-- =====================================================

-- Get hourly UTM performance for the last 7 days
SELECT 
    date_start,
    hour_start,
    hour_window,
    channel,
    utm_source,
    utm_campaign,
    total_sessions,
    total_conversions,
    total_revenue,
    total_orders,
    conversion_rate,
    attribution_rate
FROM utm_hourly_metrics
WHERE date_start >= CURRENT_DATE - INTERVAL '7 days'
ORDER BY date_start DESC, hour_start DESC;

-- =====================================================
-- 2. HOURLY UTM + AD INSIGHTS JOIN (MAIN ANALYSIS)
-- =====================================================

-- Comprehensive hourly analysis joining UTM data with ad insights
SELECT 
    u.date_start,
    u.hour_start,
    u.hour_window,
    
    -- Campaign Information
    COALESCE(u.campaign_id, a.campaign_id) as campaign_id,
    COALESCE(u.campaign_name, a.campaign_name) as campaign_name,
    COALESCE(u.ad_id, a.ad_id) as ad_id,
    COALESCE(u.ad_name, a.ad_name) as ad_name,
    
    -- UTM Parameters
    u.utm_source,
    u.utm_medium,
    u.utm_campaign,
    u.channel,
    
    -- Ad Performance Metrics
    COALESCE(a.impressions, 0) as ad_impressions,
    COALESCE(a.clicks, 0) as ad_clicks,
    COALESCE(a.spend, 0) as ad_spend,
    COALESCE(a.cpm, 0) as ad_cpm,
    COALESCE(a.cpc, 0) as ad_cpc,
    COALESCE(a.ctr, 0) as ad_ctr,
    
    -- UTM Conversion Metrics
    COALESCE(u.total_sessions, 0) as utm_sessions,
    COALESCE(u.total_conversions, 0) as utm_conversions,
    COALESCE(u.total_revenue, 0) as utm_revenue,
    COALESCE(u.total_orders, 0) as utm_orders,
    COALESCE(u.avg_order_value, 0) as utm_avg_order_value,
    
    -- Calculated Performance Metrics
    CASE 
        WHEN COALESCE(a.spend, 0) > 0 THEN ROUND((COALESCE(u.total_revenue, 0) / a.spend)::DECIMAL, 4)
        ELSE 0 
    END as roas,
    
    CASE 
        WHEN COALESCE(a.clicks, 0) > 0 THEN ROUND((COALESCE(u.total_conversions, 0)::DECIMAL / a.clicks) * 100, 4)
        ELSE 0 
    END as conversion_rate_percent
    
FROM utm_hourly_metrics u
FULL OUTER JOIN ads_insights_hourly a ON 
    u.date_start = a.date_start 
    AND u.hour_start = EXTRACT(hour FROM a.hourly_window::time)
    AND COALESCE(u.campaign_id, '') = COALESCE(a.campaign_id, '')
    AND COALESCE(u.ad_id, '') = COALESCE(a.ad_id, '')

WHERE u.date_start >= CURRENT_DATE - INTERVAL '7 days'
ORDER BY u.date_start DESC, u.hour_start DESC, u.total_revenue DESC;

-- =====================================================
-- 3. CHANNEL PERFORMANCE ANALYSIS
-- =====================================================

-- Channel performance summary for the last 30 days
SELECT 
    channel,
    sub_channel,
    COUNT(DISTINCT date_start) as active_days,
    SUM(total_sessions) as total_sessions,
    SUM(total_conversions) as total_conversions,
    SUM(total_revenue) as total_revenue,
    SUM(total_orders) as total_orders,
    CASE 
        WHEN SUM(total_sessions) > 0 THEN ROUND((SUM(total_conversions)::DECIMAL / SUM(total_sessions)) * 100, 4)
        ELSE 0 
    END as conversion_rate_percent,
    CASE 
        WHEN SUM(total_orders) > 0 THEN ROUND((SUM(total_revenue) / SUM(total_orders))::DECIMAL, 2)
        ELSE 0 
    END as avg_order_value
FROM utm_hourly_metrics
WHERE date_start >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY channel, sub_channel
HAVING SUM(total_revenue) > 0
ORDER BY total_revenue DESC;

-- =====================================================
-- 4. CAMPAIGN PERFORMANCE WITH UTM ATTRIBUTION
-- =====================================================

-- Top performing campaigns with UTM attribution data
SELECT 
    campaign_name,
    channel,
    utm_source,
    utm_medium,
    COUNT(DISTINCT date_start) as active_days,
    SUM(total_sessions) as total_sessions,
    SUM(total_conversions) as total_conversions,
    SUM(total_revenue) as total_revenue,
    SUM(total_orders) as total_orders,
    CASE 
        WHEN SUM(total_sessions) > 0 THEN ROUND((SUM(total_conversions)::DECIMAL / SUM(total_sessions)) * 100, 4)
        ELSE 0 
    END as conversion_rate_percent,
    CASE 
        WHEN SUM(total_orders) > 0 THEN ROUND((SUM(total_revenue) / SUM(total_orders))::DECIMAL, 2)
        ELSE 0 
    END as avg_order_value
FROM utm_hourly_metrics
WHERE date_start >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY campaign_name, channel, utm_source, utm_medium
HAVING SUM(total_revenue) > 0
ORDER BY total_revenue DESC
LIMIT 20;

-- =====================================================
-- 5. HOURLY PATTERN ANALYSIS
-- =====================================================

-- Hourly performance patterns for the last 7 days
SELECT 
    hour_start,
    COUNT(DISTINCT date_start) as active_days,
    SUM(total_sessions) as total_sessions,
    SUM(total_conversions) as total_conversions,
    SUM(total_revenue) as total_revenue,
    SUM(total_orders) as total_orders,
    CASE 
        WHEN SUM(total_sessions) > 0 THEN ROUND((SUM(total_conversions)::DECIMAL / SUM(total_sessions)) * 100, 4)
        ELSE 0 
    END as conversion_rate_percent,
    CASE 
        WHEN SUM(total_orders) > 0 THEN ROUND((SUM(total_revenue) / SUM(total_orders))::DECIMAL, 2)
        ELSE 0 
    END as avg_order_value
FROM utm_hourly_metrics
WHERE date_start >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY hour_start
ORDER BY hour_start;

-- =====================================================
-- 6. UTM SOURCE PERFORMANCE ANALYSIS
-- =====================================================

-- UTM source performance breakdown
SELECT 
    utm_source,
    utm_medium,
    utm_campaign,
    COUNT(DISTINCT date_start) as active_days,
    SUM(total_sessions) as total_sessions,
    SUM(total_conversions) as total_conversions,
    SUM(total_revenue) as total_revenue,
    SUM(total_orders) as total_orders,
    CASE 
        WHEN SUM(total_sessions) > 0 THEN ROUND((SUM(total_conversions)::DECIMAL / SUM(total_sessions)) * 100, 4)
        ELSE 0 
    END as conversion_rate_percent,
    CASE 
        WHEN SUM(total_orders) > 0 THEN ROUND((SUM(total_revenue) / SUM(total_orders))::DECIMAL, 2)
        ELSE 0 
    END as avg_order_value
FROM utm_hourly_metrics
WHERE date_start >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY utm_source, utm_medium, utm_campaign
HAVING SUM(total_revenue) > 0
ORDER BY total_revenue DESC;

-- =====================================================
-- 7. ATTRIBUTION ANALYSIS
-- =====================================================

-- Attribution performance by channel and campaign
SELECT 
    channel,
    campaign_name,
    SUM(attributed_revenue) as attributed_revenue,
    SUM(attributed_orders) as attributed_orders,
    SUM(total_revenue) as total_revenue,
    SUM(total_orders) as total_orders,
    CASE 
        WHEN SUM(total_revenue) > 0 THEN ROUND((SUM(attributed_revenue) / SUM(total_revenue)) * 100, 4)
        ELSE 0 
    END as attribution_rate_percent
FROM utm_hourly_metrics
WHERE date_start >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY channel, campaign_name
HAVING SUM(total_revenue) > 0
ORDER BY attribution_rate_percent DESC;

-- =====================================================
-- 8. DATA QUALITY ANALYSIS
-- =====================================================

-- Data quality assessment by batch
SELECT 
    batch_id,
    COUNT(*) as total_records,
    AVG(data_completeness_score) as avg_data_quality,
    MIN(data_completeness_score) as min_data_quality,
    MAX(data_completeness_score) as max_data_quality,
    COUNT(CASE WHEN data_completeness_score >= 90 THEN 1 END) as high_quality_records,
    COUNT(CASE WHEN data_completeness_score < 70 THEN 1 END) as low_quality_records
FROM utm_hourly_metrics
WHERE created_at >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY batch_id
ORDER BY created_at DESC;

-- =====================================================
-- 9. COMPARATIVE ANALYSIS (UTM vs AD INSIGHTS)
-- =====================================================

-- Compare UTM data with ad insights data availability
SELECT 
    u.date_start,
    u.hour_start,
    COUNT(DISTINCT u.campaign_id) as utm_campaigns,
    COUNT(DISTINCT u.ad_id) as utm_ads,
    COUNT(DISTINCT a.campaign_id) as ads_campaigns,
    COUNT(DISTINCT a.ad_id) as ads_ads,
    SUM(u.total_revenue) as utm_revenue,
    SUM(a.spend) as ads_spend,
    CASE 
        WHEN SUM(a.spend) > 0 THEN ROUND((SUM(u.total_revenue) / SUM(a.spend))::DECIMAL, 4)
        ELSE 0 
    END as roas
FROM utm_hourly_metrics u
FULL OUTER JOIN ads_insights_hourly a ON 
    u.date_start = a.date_start 
    AND u.hour_start = EXTRACT(hour FROM a.hourly_window::time)
WHERE u.date_start >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY u.date_start, u.hour_start
ORDER BY u.date_start DESC, u.hour_start DESC;

-- =====================================================
-- 10. REAL-TIME DASHBOARD QUERIES
-- =====================================================

-- Today's performance summary (for real-time dashboards)
SELECT 
    'Today' as period,
    COUNT(DISTINCT campaign_id) as active_campaigns,
    COUNT(DISTINCT ad_id) as active_ads,
    SUM(total_sessions) as total_sessions,
    SUM(total_conversions) as total_conversions,
    SUM(total_revenue) as total_revenue,
    SUM(total_orders) as total_orders,
    CASE 
        WHEN SUM(total_sessions) > 0 THEN ROUND((SUM(total_conversions)::DECIMAL / SUM(total_sessions)) * 100, 4)
        ELSE 0 
    END as conversion_rate_percent
FROM utm_hourly_metrics
WHERE date_start = CURRENT_DATE

UNION ALL

-- Yesterday's performance summary
SELECT 
    'Yesterday' as period,
    COUNT(DISTINCT campaign_id) as active_campaigns,
    COUNT(DISTINCT ad_id) as active_ads,
    SUM(total_sessions) as total_sessions,
    SUM(total_conversions) as total_conversions,
    SUM(total_revenue) as total_revenue,
    SUM(total_orders) as total_orders,
    CASE 
        WHEN SUM(total_sessions) > 0 THEN ROUND((SUM(total_conversions)::DECIMAL / SUM(total_sessions)) * 100, 4)
        ELSE 0 
    END as conversion_rate_percent
FROM utm_hourly_metrics
WHERE date_start = CURRENT_DATE - INTERVAL '1 day'

UNION ALL

-- Last 7 days performance summary
SELECT 
    'Last 7 Days' as period,
    COUNT(DISTINCT campaign_id) as active_campaigns,
    COUNT(DISTINCT ad_id) as active_ads,
    SUM(total_sessions) as total_sessions,
    SUM(total_conversions) as total_conversions,
    SUM(total_revenue) as total_revenue,
    SUM(total_orders) as total_orders,
    CASE 
        WHEN SUM(total_sessions) > 0 THEN ROUND((SUM(total_conversions)::DECIMAL / SUM(total_sessions)) * 100, 4)
        ELSE 0 
    END as conversion_rate_percent
FROM utm_hourly_metrics
WHERE date_start >= CURRENT_DATE - INTERVAL '7 days';

-- =====================================================
-- QUERY COMPLETION
-- =====================================================
-- These example queries demonstrate the full capabilities
-- of the UTM data model for hourly purchase/sale analysis
-- =====================================================
