-- Attribution Analysis Views
-- ===========================
-- This file contains SQL views for fast attribution analytics
-- These views provide pre-aggregated data for common attribution queries

-- 1. Daily Attribution Summary View
-- Provides daily aggregated metrics for attribution analysis
CREATE OR REPLACE VIEW v_daily_attribution_summary AS
SELECT 
    DATE_TRUNC('day', order_date) as date_start,
    COUNT(*) as total_orders,
    SUM(order_value) as total_revenue,
    SUM(total_cogs) as total_cogs,
    COUNT(CASE WHEN is_attributed THEN 1 END) as attributed_orders,
    COUNT(CASE WHEN is_paid_channel THEN 1 END) as paid_channel_orders,
    ROUND(COUNT(CASE WHEN is_attributed THEN 1 END) * 100.0 / COUNT(*), 2) as attribution_rate,
    ROUND(COUNT(CASE WHEN is_paid_channel THEN 1 END) * 100.0 / COUNT(*), 2) as paid_channel_rate,
    ROUND(SUM(total_cogs) * 100.0 / SUM(order_value), 2) as cogs_percentage,
    AVG(order_value) as avg_order_value
FROM attribution_raw
GROUP BY DATE_TRUNC('day', order_date)
ORDER BY date_start DESC;

-- 2. Channel Performance View
-- Channel-level performance metrics
CREATE OR REPLACE VIEW v_channel_performance AS
SELECT 
    channel,
    COUNT(*) as total_orders,
    SUM(order_value) as total_revenue,
    SUM(total_cogs) as total_cogs,
    COUNT(CASE WHEN is_attributed THEN 1 END) as attributed_orders,
    ROUND(COUNT(CASE WHEN is_attributed THEN 1 END) * 100.0 / COUNT(*), 2) as attribution_rate,
    ROUND(SUM(total_cogs) * 100.0 / SUM(order_value), 2) as cogs_percentage,
    AVG(order_value) as avg_order_value,
    MIN(order_date) as first_order_date,
    MAX(order_date) as last_order_date
FROM attribution_raw
GROUP BY channel
ORDER BY total_revenue DESC;

-- 3. Campaign Performance View
-- Campaign-level performance with attribution details
CREATE OR REPLACE VIEW v_campaign_performance AS
SELECT 
    campaign_id,
    campaign_name,
    adset_id,
    adset_name,
    ad_id,
    ad_name,
    channel,
    COUNT(*) as total_orders,
    SUM(order_value) as total_revenue,
    SUM(total_cogs) as total_cogs,
    ROUND(SUM(total_cogs) * 100.0 / SUM(order_value), 2) as cogs_percentage,
    AVG(order_value) as avg_order_value,
    COUNT(DISTINCT order_id) as unique_orders,
    MIN(order_date) as first_order_date,
    MAX(order_date) as last_order_date,
    -- Attribution metrics
    COUNT(CASE WHEN attribution_source = 'customer_journey' THEN 1 END) as journey_attributions,
    COUNT(CASE WHEN attribution_source = 'custom_attributes' THEN 1 END) as attributes_attributions,
    COUNT(CASE WHEN attribution_source = 'direct_utm' THEN 1 END) as direct_utm_attributions
FROM attribution_raw
WHERE is_attributed = true
GROUP BY campaign_id, campaign_name, adset_id, adset_name, ad_id, ad_name, channel
ORDER BY total_revenue DESC;

-- 4. Attribution Source Analysis View
-- Analysis of attribution sources and their effectiveness
CREATE OR REPLACE VIEW v_attribution_source_analysis AS
SELECT 
    attribution_source,
    channel,
    COUNT(*) as total_orders,
    SUM(order_value) as total_revenue,
    AVG(order_value) as avg_order_value,
    COUNT(DISTINCT attribution_id) as unique_attribution_ids,
    MIN(order_date) as first_order_date,
    MAX(order_date) as last_order_date
FROM attribution_raw
WHERE is_attributed = true
GROUP BY attribution_source, channel
ORDER BY total_revenue DESC;

-- 5. SKU Performance View
-- SKU-level performance across channels and campaigns
CREATE OR REPLACE VIEW v_sku_performance AS
SELECT 
    UNNEST(STRING_TO_ARRAY(skus, ', ')) as sku,
    channel,
    campaign_name,
    COUNT(*) as orders,
    SUM(order_value) as revenue,
    SUM(total_cogs) as cogs,
    ROUND(SUM(total_cogs) * 100.0 / SUM(order_value), 2) as cogs_percentage,
    AVG(order_value) as avg_order_value,
    SUM(total_sku_quantity) as total_quantity_sold
FROM attribution_raw
WHERE is_attributed = true AND skus IS NOT NULL
GROUP BY UNNEST(STRING_TO_ARRAY(skus, ', ')), channel, campaign_name
ORDER BY revenue DESC;

-- 6. Monthly Attribution Trends View
-- Monthly trends for attribution analysis
CREATE OR REPLACE VIEW v_monthly_attribution_trends AS
SELECT 
    DATE_TRUNC('month', order_date) as month_start,
    COUNT(*) as total_orders,
    SUM(order_value) as total_revenue,
    COUNT(CASE WHEN is_attributed THEN 1 END) as attributed_orders,
    COUNT(CASE WHEN is_paid_channel THEN 1 END) as paid_channel_orders,
    ROUND(COUNT(CASE WHEN is_attributed THEN 1 END) * 100.0 / COUNT(*), 2) as attribution_rate,
    ROUND(COUNT(CASE WHEN is_paid_channel THEN 1 END) * 100.0 / COUNT(*), 2) as paid_channel_rate,
    AVG(order_value) as avg_order_value,
    -- Channel breakdown
    COUNT(CASE WHEN channel = 'Facebook' THEN 1 END) as facebook_orders,
    COUNT(CASE WHEN channel = 'Instagram' THEN 1 END) as instagram_orders,
    COUNT(CASE WHEN channel = 'Google' THEN 1 END) as google_orders,
    COUNT(CASE WHEN channel = 'Direct/Unknown' THEN 1 END) as direct_orders
FROM attribution_raw
GROUP BY DATE_TRUNC('month', order_date)
ORDER BY month_start DESC;

-- 7. Attribution Quality Analysis View
-- Analysis of attribution data quality and completeness
CREATE OR REPLACE VIEW v_attribution_quality_analysis AS
SELECT 
    DATE_TRUNC('day', order_date) as date_start,
    COUNT(*) as total_orders,
    COUNT(CASE WHEN is_attributed THEN 1 END) as attributed_orders,
    COUNT(CASE WHEN attribution_source = 'customer_journey' THEN 1 END) as journey_attributions,
    COUNT(CASE WHEN attribution_source = 'custom_attributes' THEN 1 END) as attributes_attributions,
    COUNT(CASE WHEN attribution_source = 'direct_utm' THEN 1 END) as direct_utm_attributions,
    COUNT(CASE WHEN campaign_id IS NOT NULL THEN 1 END) as campaign_matched_orders,
    ROUND(COUNT(CASE WHEN is_attributed THEN 1 END) * 100.0 / COUNT(*), 2) as attribution_rate,
    ROUND(COUNT(CASE WHEN campaign_id IS NOT NULL THEN 1 END) * 100.0 / COUNT(CASE WHEN is_attributed THEN 1 END), 2) as campaign_match_rate
FROM attribution_raw
GROUP BY DATE_TRUNC('day', order_date)
ORDER BY date_start DESC;

-- 8. Top Performing Campaigns View
-- Top performing campaigns with detailed metrics
CREATE OR REPLACE VIEW v_top_performing_campaigns AS
SELECT 
    campaign_name,
    channel,
    COUNT(*) as total_orders,
    SUM(order_value) as total_revenue,
    SUM(total_cogs) as total_cogs,
    ROUND(SUM(order_value) - SUM(total_cogs), 2) as gross_profit,
    ROUND((SUM(order_value) - SUM(total_cogs)) * 100.0 / SUM(order_value), 2) as profit_margin,
    AVG(order_value) as avg_order_value,
    COUNT(DISTINCT DATE_TRUNC('day', order_date)) as active_days,
    MIN(order_date) as first_order_date,
    MAX(order_date) as last_order_date
FROM attribution_raw
WHERE is_attributed = true AND campaign_name IS NOT NULL
GROUP BY campaign_name, channel
HAVING COUNT(*) >= 5  -- Only campaigns with at least 5 orders
ORDER BY total_revenue DESC
LIMIT 100;

-- 9. Attribution Funnel Analysis View
-- Analysis of attribution funnel from impressions to orders
CREATE OR REPLACE VIEW v_attribution_funnel_analysis AS
SELECT 
    campaign_name,
    channel,
    -- Ad performance metrics (from ads_insights_hourly)
    SUM(ad_impressions) as total_impressions,
    SUM(ad_clicks) as total_clicks,
    SUM(ad_spend) as total_spend,
    -- Order metrics (from attribution_raw)
    COUNT(*) as total_orders,
    SUM(order_value) as total_revenue,
    -- Calculated funnel metrics
    ROUND(SUM(ad_clicks) * 100.0 / SUM(ad_impressions), 2) as ctr,
    ROUND(COUNT(*) * 100.0 / SUM(ad_clicks), 2) as conversion_rate,
    ROUND(SUM(order_value) / SUM(ad_spend), 2) as roas,
    ROUND(SUM(ad_spend) / COUNT(*), 2) as cost_per_order
FROM attribution_raw ar
LEFT JOIN campaign_performance cp ON ar.campaign_id = cp.campaign_id
WHERE ar.is_attributed = true
GROUP BY campaign_name, channel
HAVING SUM(ad_impressions) > 0
ORDER BY total_revenue DESC;

-- 10. Geographic Attribution Analysis View
-- Geographic analysis of attribution data
CREATE OR REPLACE VIEW v_geographic_attribution_analysis AS
SELECT 
    ship_country,
    ship_province,
    ship_city,
    channel,
    COUNT(*) as total_orders,
    SUM(order_value) as total_revenue,
    AVG(order_value) as avg_order_value,
    COUNT(CASE WHEN is_attributed THEN 1 END) as attributed_orders,
    ROUND(COUNT(CASE WHEN is_attributed THEN 1 END) * 100.0 / COUNT(*), 2) as attribution_rate
FROM attribution_raw
WHERE ship_country IS NOT NULL
GROUP BY ship_country, ship_province, ship_city, channel
ORDER BY total_revenue DESC;

-- Create indexes for better view performance
CREATE INDEX IF NOT EXISTS idx_attribution_raw_order_date ON attribution_raw(order_date);
CREATE INDEX IF NOT EXISTS idx_attribution_raw_channel ON attribution_raw(channel);
CREATE INDEX IF NOT EXISTS idx_attribution_raw_campaign_id ON attribution_raw(campaign_id);
CREATE INDEX IF NOT EXISTS idx_attribution_raw_is_attributed ON attribution_raw(is_attributed);
CREATE INDEX IF NOT EXISTS idx_attribution_raw_is_paid_channel ON attribution_raw(is_paid_channel);

-- Grant permissions (adjust as needed for your environment)
-- GRANT SELECT ON ALL TABLES IN SCHEMA public TO analytics_user;
-- GRANT SELECT ON ALL VIEWS IN SCHEMA public TO analytics_user;
