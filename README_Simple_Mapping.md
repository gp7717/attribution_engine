# Simple Order-Ad Mapping

A straightforward solution that creates a direct mapping between orders and ads by rolling up hourly ad data to daily and mapping order SKUs to UTM content (ad IDs).

## 🎯 **What This Does**

### **Simple Process:**
1. **Rolls up hourly ad data to daily level**
2. **Maps each order's SKUs to the UTM content (ad ID)**
3. **Creates a direct order-to-ad mapping table**
4. **Provides analysis views for SKU and campaign performance**

### **Result:**
- **Direct relationship**: Order → SKU → Ad ID → Ad Performance
- **Every order line item mapped to its corresponding ad**
- **Daily ad performance metrics attached to each order**

## 🏗️ **Architecture**

```
Hourly Ads → Daily Rollup → Order SKUs → UTM Content → Ad Mapping
     ↓              ↓           ↓            ↓           ↓
ads_insights_hourly → daily_ads → order_line_items → utm_content → order_ad_mapping
```

## 📊 **Data Flow**

### **1. Ad Data Rollup:**
```sql
-- Hourly ads rolled up to daily
SELECT 
    campaign_id, campaign_name, adset_id, adset_name, ad_id, ad_name,
    date_start,
    SUM(impressions) as daily_impressions,
    SUM(clicks) as daily_clicks,
    SUM(spend) as daily_spend,
    SUM(action_onsite_web_purchase) as daily_purchases
FROM ads_insights_hourly 
GROUP BY campaign_id, campaign_name, adset_id, adset_name, ad_id, ad_name, date_start
```

### **2. Order-SKU Mapping:**
```sql
-- Orders with SKU information
SELECT 
    o.order_id, o.order_name, o.created_at_ist as order_date,
    o.customer_utm_content,  -- This is the ad ID
    li.quantity, pv.sku, pv.unit_cost_amount, li.discounted_unit_price_amount
FROM shopify_orders o
LEFT JOIN shopify_order_line_items li ON o.order_id = li.order_id
LEFT JOIN shopify_product_variants pv ON li.variant_id = pv.variant_id
```

### **3. Final Mapping:**
```sql
-- Direct order-to-ad mapping
SELECT 
    order_id, sku, ad_id, campaign_name,
    line_item_revenue, ad_spend, roas
FROM order_ad_mapping
```

## 🚀 **Usage**

### **Run the Mapping:**
```bash
python simple_order_ad_mapping.py
```

### **Query the Results:**
```sql
-- Top performing SKUs by ad
SELECT * FROM v_sku_ad_performance 
ORDER BY total_revenue DESC;

-- Top performing ads by SKU
SELECT * FROM v_ad_sku_performance 
ORDER BY total_revenue DESC;

-- Daily performance summary
SELECT * FROM v_daily_order_ad_performance 
ORDER BY order_date DESC;
```

## 📈 **Analysis Views**

### **1. SKU Performance by Ad (`v_sku_ad_performance`)**
```sql
SELECT 
    sku, product_title, ad_id, ad_name, campaign_name,
    COUNT(*) as orders,
    SUM(line_item_revenue) as total_revenue,
    ROUND(AVG(roas), 2) as avg_roas
FROM order_ad_mapping
GROUP BY sku, product_title, ad_id, ad_name, campaign_name
ORDER BY total_revenue DESC;
```

**What you can see:**
- Which SKUs perform best with which ads
- SKU-level revenue and profitability
- Ad effectiveness for specific products

### **2. Ad Performance by SKU (`v_ad_sku_performance`)**
```sql
SELECT 
    ad_id, ad_name, campaign_name,
    COUNT(DISTINCT sku) as unique_skus,
    SUM(line_item_revenue) as total_revenue,
    ROUND(SUM(line_item_revenue) / SUM(ad_spend), 2) as overall_roas
FROM order_ad_mapping
GROUP BY ad_id, ad_name, campaign_name
ORDER BY total_revenue DESC;
```

**What you can see:**
- Which ads drive the most revenue
- Ad performance across different SKUs
- Campaign effectiveness

### **3. Daily Performance Summary (`v_daily_order_ad_performance`)**
```sql
SELECT 
    order_date,
    COUNT(DISTINCT order_id) as total_orders,
    SUM(line_item_revenue) as total_revenue,
    ROUND(SUM(line_item_revenue) / SUM(ad_spend), 2) as daily_roas
FROM order_ad_mapping
GROUP BY order_date
ORDER BY order_date DESC;
```

**What you can see:**
- Daily performance trends
- Revenue and ROAS by day
- Order volume patterns

### **4. Campaign-SKU Breakdown (`v_campaign_sku_breakdown`)**
```sql
SELECT 
    campaign_name,
    COUNT(DISTINCT sku) as unique_skus,
    SUM(line_item_revenue) as total_revenue,
    ROUND(SUM(line_item_revenue) / SUM(ad_spend), 2) as campaign_roas
FROM order_ad_mapping
GROUP BY campaign_name
ORDER BY total_revenue DESC;
```

**What you can see:**
- Campaign performance with SKU insights
- Which campaigns work best for which products
- Campaign efficiency metrics

## 🔍 **Key Insights You Can Get**

### **SKU Analysis:**
- **Top performing SKUs** across all ads
- **SKU performance by ad** (which ads work best for which products)
- **Product profitability** by ad and campaign
- **SKU trends** over time

### **Ad Analysis:**
- **Top performing ads** across all SKUs
- **Ad efficiency** (ROAS, cost per order)
- **Ad performance by product category**
- **Campaign effectiveness** with SKU breakdown

### **Campaign Analysis:**
- **Campaign performance** with detailed SKU insights
- **Budget allocation** effectiveness
- **Cross-campaign comparison** with SKU data
- **Campaign optimization** opportunities

## 📊 **Sample Queries**

### **Find Best SKU-Ad Combinations:**
```sql
SELECT 
    sku, product_title, ad_name, campaign_name,
    SUM(line_item_revenue) as revenue,
    ROUND(SUM(line_item_revenue) / SUM(ad_spend), 2) as roas
FROM order_ad_mapping
GROUP BY sku, product_title, ad_name, campaign_name
HAVING SUM(ad_spend) > 0
ORDER BY roas DESC
LIMIT 10;
```

### **Find Underperforming Ads:**
```sql
SELECT 
    ad_id, ad_name, campaign_name,
    SUM(line_item_revenue) as revenue,
    SUM(ad_spend) as spend,
    ROUND(SUM(line_item_revenue) / SUM(ad_spend), 2) as roas
FROM order_ad_mapping
GROUP BY ad_id, ad_name, campaign_name
HAVING SUM(ad_spend) > 100  -- Only ads with significant spend
ORDER BY roas ASC
LIMIT 10;
```

### **Daily Performance Trends:**
```sql
SELECT 
    order_date,
    COUNT(DISTINCT order_id) as orders,
    SUM(line_item_revenue) as revenue,
    ROUND(SUM(line_item_revenue) / SUM(ad_spend), 2) as roas
FROM order_ad_mapping
WHERE order_date >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY order_date
ORDER BY order_date DESC;
```

## 🎯 **Business Questions You Can Answer**

### **Product Questions:**
- "Which SKUs perform best with which ads?"
- "What's the profitability of each product by campaign?"
- "Which products should we promote more?"

### **Ad Questions:**
- "Which ads drive the highest ROAS?"
- "What's the cost per order for each ad?"
- "Which ads should we optimize or pause?"

### **Campaign Questions:**
- "Which campaigns are most effective for which products?"
- "How should we allocate budget across campaigns?"
- "What's the overall campaign performance?"

### **Strategic Questions:**
- "What's our daily performance trend?"
- "Which SKU-ad combinations are most profitable?"
- "How can we optimize our ad spend?"

## 🔧 **Configuration**

### **Database Setup:**
```python
from config import POSTGRESQL_CONFIG

# Initialize mapping engine
mapper = SimpleOrderAdMapping(POSTGRESQL_CONFIG)

# Run mapping
mapping_df = mapper.run_simple_mapping('2025-01-01', '2025-01-31')
```

### **Date Range:**
```python
# Run for specific date range
start_date = "2025-01-01"
end_date = "2025-01-31"
mapping_df = mapper.run_simple_mapping(start_date, end_date)
```

## 📋 **Output**

### **Database Table:**
- **`order_ad_mapping`** - Main mapping table with all order-ad relationships

### **Analysis Views:**
- **`v_sku_ad_performance`** - SKU performance by ad
- **`v_ad_sku_performance`** - Ad performance by SKU
- **`v_daily_order_ad_performance`** - Daily performance summary
- **`v_campaign_sku_breakdown`** - Campaign performance with SKU breakdown
- **`v_top_performing_skus`** - Best performing SKUs
- **`v_top_performing_ads`** - Best performing ads
- **`v_sku_ad_efficiency`** - SKU-Ad efficiency analysis
- **`v_campaign_sku_matrix`** - Campaign-SKU performance matrix
- **`v_daily_sku_performance`** - Daily SKU performance trends
- **`v_daily_ad_performance`** - Daily ad performance trends

## 🚀 **Benefits**

1. **Simple and Direct**: Easy to understand order-to-ad mapping
2. **Comprehensive**: Covers SKU, ad, and campaign analysis
3. **Real-time**: Daily rollup keeps data current
4. **Flexible**: Multiple analysis views for different needs
5. **Performance**: Optimized queries and indexes

## 📈 **Next Steps**

1. **Run the mapping** for your date range
2. **Explore the views** to understand your data
3. **Identify optimization opportunities** using the insights
4. **Set up regular runs** for ongoing analysis
5. **Create dashboards** using the analysis views

This simple approach gives you everything you need for SKU and campaign analysis with a direct, easy-to-understand mapping between orders and ads.
