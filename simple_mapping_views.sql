-- Simple Order-Ad Mapping Views
-- =============================
-- Views for analyzing the direct order-to-ad mapping
-- Each order line item (SKU) is mapped to its corresponding ad

-- 1. SKU Performance by Ad
-- Shows how each SKU performs across different ads
CREATE OR REPLACE VIEW v_sku_ad_performance AS
SELECT 
    sku,
    product_title,
    ad_id,
    ad_name,
    campaign_name,
    COUNT(*) as orders,
    SUM(quantity) as total_quantity_sold,
    SUM(line_item_revenue) as total_revenue,
    SUM(line_item_cogs) as total_cogs,
    SUM(line_item_profit) as total_profit,
    ROUND(AVG(profit_margin), 2) as avg_profit_margin,
    ROUND(AVG(roas), 2) as avg_roas,
    ROUND(SUM(line_item_revenue) / SUM(quantity), 2) as revenue_per_unit,
    ROUND(SUM(line_item_profit) / SUM(line_item_revenue) * 100, 2) as overall_profit_margin
FROM order_ad_mapping
GROUP BY sku, product_title, ad_id, ad_name, campaign_name
ORDER BY total_revenue DESC;

-- 2. Ad Performance by SKU
-- Shows how each ad performs across different SKUs
CREATE OR REPLACE VIEW v_ad_sku_performance AS
SELECT 
    ad_id,
    ad_name,
    campaign_name,
    COUNT(DISTINCT sku) as unique_skus,
    COUNT(*) as total_orders,
    SUM(quantity) as total_quantity_sold,
    SUM(line_item_revenue) as total_revenue,
    SUM(ad_spend) as total_spend,
    ROUND(AVG(roas), 2) as avg_roas,
    ROUND(SUM(line_item_revenue) / SUM(ad_spend), 2) as overall_roas,
    ROUND(SUM(ad_spend) / COUNT(*), 2) as cost_per_order,
    ROUND(SUM(line_item_revenue) / COUNT(*), 2) as revenue_per_order
FROM order_ad_mapping
GROUP BY ad_id, ad_name, campaign_name
ORDER BY total_revenue DESC;

-- 3. Daily Performance Summary
-- Daily aggregated performance across all orders and ads
CREATE OR REPLACE VIEW v_daily_order_ad_performance AS
SELECT 
    order_date,
    COUNT(DISTINCT order_id) as total_orders,
    COUNT(DISTINCT sku) as unique_skus,
    COUNT(DISTINCT ad_id) as unique_ads,
    COUNT(DISTINCT campaign_name) as unique_campaigns,
    SUM(line_item_revenue) as total_revenue,
    SUM(ad_spend) as total_spend,
    ROUND(SUM(line_item_revenue) / SUM(ad_spend), 2) as daily_roas,
    ROUND(SUM(ad_spend) / COUNT(DISTINCT order_id), 2) as cost_per_order,
    ROUND(SUM(line_item_revenue) / COUNT(DISTINCT order_id), 2) as revenue_per_order
FROM order_ad_mapping
GROUP BY order_date
ORDER BY order_date DESC;

-- 4. Campaign Performance with SKU Breakdown
-- Campaign-level performance with SKU insights
CREATE OR REPLACE VIEW v_campaign_sku_breakdown AS
SELECT 
    campaign_name,
    COUNT(DISTINCT sku) as unique_skus,
    COUNT(DISTINCT ad_id) as unique_ads,
    COUNT(*) as total_orders,
    SUM(quantity) as total_quantity_sold,
    SUM(line_item_revenue) as total_revenue,
    SUM(ad_spend) as total_spend,
    ROUND(SUM(line_item_revenue) / SUM(ad_spend), 2) as campaign_roas,
    ROUND(SUM(line_item_revenue) / COUNT(DISTINCT sku), 2) as revenue_per_sku,
    ROUND(SUM(ad_spend) / COUNT(DISTINCT ad_id), 2) as spend_per_ad,
    ROUND(SUM(line_item_revenue) / COUNT(*), 2) as revenue_per_order
FROM order_ad_mapping
GROUP BY campaign_name
ORDER BY total_revenue DESC;

-- 5. Top Performing SKUs
-- Best performing SKUs across all ads
CREATE OR REPLACE VIEW v_top_performing_skus AS
SELECT 
    sku,
    product_title,
    COUNT(DISTINCT ad_id) as ads_count,
    COUNT(*) as total_orders,
    SUM(quantity) as total_quantity_sold,
    SUM(line_item_revenue) as total_revenue,
    SUM(line_item_profit) as total_profit,
    ROUND(AVG(profit_margin), 2) as avg_profit_margin,
    ROUND(SUM(line_item_revenue) / SUM(quantity), 2) as revenue_per_unit,
    ROUND(SUM(line_item_profit) / SUM(line_item_revenue) * 100, 2) as overall_profit_margin
FROM order_ad_mapping
GROUP BY sku, product_title
HAVING COUNT(*) >= 5  -- Only SKUs with at least 5 orders
ORDER BY total_revenue DESC;

-- 6. Top Performing Ads
-- Best performing ads across all SKUs
CREATE OR REPLACE VIEW v_top_performing_ads AS
SELECT 
    ad_id,
    ad_name,
    campaign_name,
    COUNT(DISTINCT sku) as unique_skus,
    COUNT(*) as total_orders,
    SUM(line_item_revenue) as total_revenue,
    SUM(ad_spend) as total_spend,
    ROUND(SUM(line_item_revenue) / SUM(ad_spend), 2) as overall_roas,
    ROUND(SUM(ad_spend) / COUNT(*), 2) as cost_per_order,
    ROUND(SUM(line_item_revenue) / COUNT(*), 2) as revenue_per_order
FROM order_ad_mapping
GROUP BY ad_id, ad_name, campaign_name
HAVING COUNT(*) >= 3  -- Only ads with at least 3 orders
ORDER BY total_revenue DESC;

-- 7. SKU-Ad Efficiency Analysis
-- Efficiency metrics for SKU-Ad combinations
CREATE OR REPLACE VIEW v_sku_ad_efficiency AS
SELECT 
    sku,
    product_title,
    ad_id,
    ad_name,
    campaign_name,
    COUNT(*) as orders,
    SUM(quantity) as quantity_sold,
    SUM(line_item_revenue) as revenue,
    SUM(ad_spend) as spend,
    ROUND(SUM(line_item_revenue) / SUM(ad_spend), 2) as roas,
    ROUND(SUM(ad_spend) / COUNT(*), 2) as cost_per_order,
    ROUND(SUM(line_item_revenue) / COUNT(*), 2) as revenue_per_order,
    ROUND(SUM(line_item_revenue) / SUM(quantity), 2) as revenue_per_unit,
    -- Efficiency score (revenue per spend)
    ROUND(SUM(line_item_revenue) / NULLIF(SUM(ad_spend), 0), 2) as efficiency_score
FROM order_ad_mapping
GROUP BY sku, product_title, ad_id, ad_name, campaign_name
HAVING SUM(ad_spend) > 0  -- Only combinations with ad spend
ORDER BY efficiency_score DESC;

-- 8. Campaign-SKU Performance Matrix
-- Matrix showing how each campaign performs with each SKU
CREATE OR REPLACE VIEW v_campaign_sku_matrix AS
SELECT 
    campaign_name,
    sku,
    product_title,
    COUNT(*) as orders,
    SUM(quantity) as quantity_sold,
    SUM(line_item_revenue) as revenue,
    SUM(ad_spend) as spend,
    ROUND(SUM(line_item_revenue) / NULLIF(SUM(ad_spend), 0), 2) as roas,
    ROUND(SUM(line_item_revenue) / COUNT(*), 2) as revenue_per_order
FROM order_ad_mapping
GROUP BY campaign_name, sku, product_title
ORDER BY campaign_name, revenue DESC;

-- 9. Daily SKU Performance Trends
-- Daily performance trends for SKUs
CREATE OR REPLACE VIEW v_daily_sku_performance AS
SELECT 
    order_date,
    sku,
    product_title,
    COUNT(*) as orders,
    SUM(quantity) as quantity_sold,
    SUM(line_item_revenue) as revenue,
    ROUND(SUM(line_item_revenue) / COUNT(*), 2) as revenue_per_order,
    ROUND(SUM(line_item_revenue) / SUM(quantity), 2) as revenue_per_unit
FROM order_ad_mapping
GROUP BY order_date, sku, product_title
ORDER BY order_date DESC, revenue DESC;

-- 10. Ad Performance Trends
-- Daily performance trends for ads
CREATE OR REPLACE VIEW v_daily_ad_performance AS
SELECT 
    order_date,
    ad_id,
    ad_name,
    campaign_name,
    COUNT(*) as orders,
    SUM(line_item_revenue) as revenue,
    SUM(ad_spend) as spend,
    ROUND(SUM(line_item_revenue) / NULLIF(SUM(ad_spend), 0), 2) as roas,
    ROUND(SUM(ad_spend) / COUNT(*), 2) as cost_per_order
FROM order_ad_mapping
GROUP BY order_date, ad_id, ad_name, campaign_name
ORDER BY order_date DESC, revenue DESC;

-- Create indexes for better performance
CREATE INDEX IF NOT EXISTS idx_order_ad_mapping_order_date ON order_ad_mapping(order_date);
CREATE INDEX IF NOT EXISTS idx_order_ad_mapping_sku ON order_ad_mapping(sku);
CREATE INDEX IF NOT EXISTS idx_order_ad_mapping_ad_id ON order_ad_mapping(ad_id);
CREATE INDEX IF NOT EXISTS idx_order_ad_mapping_campaign_name ON order_ad_mapping(campaign_name);
CREATE INDEX IF NOT EXISTS idx_order_ad_mapping_sku_ad ON order_ad_mapping(sku, ad_id);
CREATE INDEX IF NOT EXISTS idx_order_ad_mapping_campaign_sku ON order_ad_mapping(campaign_name, sku);

-- Grant permissions (adjust as needed for your environment)
-- GRANT SELECT ON ALL TABLES IN SCHEMA public TO analytics_user;
-- GRANT SELECT ON ALL VIEWS IN SCHEMA public TO analytics_user;
