# Attribution Data Enhancement Plan

## Current Data Structure Analysis

### ✅ **Available Data for SKU Analysis:**
- SKU codes and product information
- Quantity and pricing data
- Order-level aggregation
- Basic financial metrics

### ✅ **Available Data for Campaign Analysis:**
- Campaign hierarchy (campaign → adset → ad)
- Channel information
- Basic attribution data
- Order-level campaign mapping

## 🚨 **Missing Data for Complete Analysis**

### **SKU-Level Analysis Gaps:**

1. **Individual SKU Performance Metrics**
   - SKU-specific revenue and COGS
   - SKU-level attribution mapping
   - Product category/type information
   - SKU performance trends

2. **Product Hierarchy Data**
   - Product categories
   - Product types
   - Brand information
   - Product lifecycle stage

3. **SKU-Campaign Mapping**
   - Direct SKU to campaign attribution
   - SKU performance by campaign
   - Product-specific campaign effectiveness

### **Campaign Analysis Gaps:**

1. **Ad Performance Metrics**
   - Impressions, clicks, spend per campaign
   - Campaign ROI and ROAS
   - Cost per acquisition (CPA)
   - Campaign efficiency metrics

2. **Attribution Quality Metrics**
   - Attribution source effectiveness
   - Campaign match rates
   - Data quality indicators

3. **Cross-Campaign Analysis**
   - Campaign comparison metrics
   - Budget allocation insights
   - Performance benchmarking

## 🔧 **Recommended Enhancements**

### **1. Enhanced SKU Analysis Structure**

```sql
-- Enhanced SKU performance table
CREATE TABLE sku_performance_detailed (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    sku VARCHAR(100) NOT NULL,
    product_title VARCHAR(500),
    product_category VARCHAR(100),
    product_type VARCHAR(100),
    brand VARCHAR(100),
    vendor VARCHAR(100),
    
    -- Campaign attribution
    campaign_id VARCHAR(50),
    campaign_name VARCHAR(200),
    channel VARCHAR(50),
    
    -- Performance metrics
    total_orders INTEGER DEFAULT 0,
    total_quantity_sold INTEGER DEFAULT 0,
    total_revenue DECIMAL(10,2) DEFAULT 0.0,
    total_cogs DECIMAL(10,2) DEFAULT 0.0,
    avg_unit_price DECIMAL(10,2),
    avg_unit_cost DECIMAL(10,2),
    
    -- Calculated metrics
    profit_margin DECIMAL(5,2),
    revenue_per_order DECIMAL(10,2),
    quantity_per_order DECIMAL(10,2),
    
    -- Date range
    date_start DATE NOT NULL,
    date_end DATE NOT NULL,
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### **2. Enhanced Campaign Analysis Structure**

```sql
-- Enhanced campaign performance table
CREATE TABLE campaign_performance_detailed (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    campaign_id VARCHAR(50) NOT NULL,
    campaign_name VARCHAR(200) NOT NULL,
    adset_id VARCHAR(50),
    adset_name VARCHAR(200),
    ad_id VARCHAR(50),
    ad_name VARCHAR(200),
    channel VARCHAR(50) NOT NULL,
    
    -- Ad performance metrics
    total_impressions BIGINT DEFAULT 0,
    total_clicks BIGINT DEFAULT 0,
    total_spend DECIMAL(10,2) DEFAULT 0.0,
    
    -- Order metrics
    total_orders INTEGER DEFAULT 0,
    total_revenue DECIMAL(10,2) DEFAULT 0.0,
    total_cogs DECIMAL(10,2) DEFAULT 0.0,
    
    -- Calculated metrics
    roas DECIMAL(10,2),
    ctr DECIMAL(5,2),
    conversion_rate DECIMAL(5,2),
    cost_per_order DECIMAL(10,2),
    profit_margin DECIMAL(5,2),
    
    -- Attribution quality
    attribution_rate DECIMAL(5,2),
    campaign_match_rate DECIMAL(5,2),
    
    -- Date range
    date_start DATE NOT NULL,
    date_end DATE NOT NULL,
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### **3. SKU-Campaign Mapping Table**

```sql
-- SKU-Campaign mapping table
CREATE TABLE sku_campaign_mapping (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    sku VARCHAR(100) NOT NULL,
    campaign_id VARCHAR(50) NOT NULL,
    campaign_name VARCHAR(200),
    channel VARCHAR(50),
    
    -- Performance metrics
    total_orders INTEGER DEFAULT 0,
    total_quantity_sold INTEGER DEFAULT 0,
    total_revenue DECIMAL(10,2) DEFAULT 0.0,
    total_cogs DECIMAL(10,2) DEFAULT 0.0,
    
    -- Calculated metrics
    revenue_per_sku DECIMAL(10,2),
    quantity_per_order DECIMAL(10,2),
    profit_margin DECIMAL(5,2),
    
    -- Date range
    date_start DATE NOT NULL,
    date_end DATE NOT NULL,
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    UNIQUE(sku, campaign_id, date_start)
);
```

## 📊 **Enhanced Views for Analysis**

### **1. SKU Performance by Campaign**

```sql
CREATE VIEW v_sku_campaign_performance AS
SELECT 
    sku,
    product_title,
    product_category,
    campaign_name,
    channel,
    SUM(total_orders) as total_orders,
    SUM(total_quantity_sold) as total_quantity_sold,
    SUM(total_revenue) as total_revenue,
    SUM(total_cogs) as total_cogs,
    ROUND(SUM(total_cogs) * 100.0 / SUM(total_revenue), 2) as cogs_percentage,
    ROUND(SUM(total_revenue) / SUM(total_orders), 2) as revenue_per_order,
    ROUND(SUM(total_quantity_sold) / SUM(total_orders), 2) as quantity_per_order
FROM sku_campaign_mapping
GROUP BY sku, product_title, product_category, campaign_name, channel
ORDER BY total_revenue DESC;
```

### **2. Campaign Performance with SKU Breakdown**

```sql
CREATE VIEW v_campaign_sku_breakdown AS
SELECT 
    campaign_name,
    channel,
    COUNT(DISTINCT sku) as unique_skus,
    SUM(total_orders) as total_orders,
    SUM(total_revenue) as total_revenue,
    SUM(total_cogs) as total_cogs,
    ROUND(SUM(total_cogs) * 100.0 / SUM(total_revenue), 2) as cogs_percentage,
    ROUND(SUM(total_revenue) / COUNT(DISTINCT sku), 2) as revenue_per_sku,
    ROUND(SUM(total_orders) / COUNT(DISTINCT sku), 2) as orders_per_sku
FROM sku_campaign_mapping
GROUP BY campaign_name, channel
ORDER BY total_revenue DESC;
```

### **3. Product Category Performance**

```sql
CREATE VIEW v_product_category_performance AS
SELECT 
    product_category,
    channel,
    COUNT(DISTINCT sku) as unique_skus,
    SUM(total_orders) as total_orders,
    SUM(total_revenue) as total_revenue,
    SUM(total_cogs) as total_cogs,
    ROUND(SUM(total_cogs) * 100.0 / SUM(total_revenue), 2) as cogs_percentage,
    ROUND(SUM(total_revenue) / COUNT(DISTINCT sku), 2) as revenue_per_sku
FROM sku_campaign_mapping
WHERE product_category IS NOT NULL
GROUP BY product_category, channel
ORDER BY total_revenue DESC;
```

## 🚀 **Implementation Steps**

### **Phase 1: Data Enhancement**
1. Add product category and type information
2. Create SKU-level performance calculations
3. Implement SKU-campaign mapping

### **Phase 2: Campaign Enhancement**
1. Add ad performance metrics (impressions, clicks, spend)
2. Calculate campaign ROI and efficiency metrics
3. Implement attribution quality tracking

### **Phase 3: Advanced Analytics**
1. Create enhanced views for SKU and campaign analysis
2. Implement cross-campaign comparison tools
3. Add predictive analytics capabilities

## 📈 **Expected Benefits**

### **SKU Analysis Benefits:**
- Individual SKU performance tracking
- Product category insights
- SKU-specific campaign effectiveness
- Inventory optimization insights

### **Campaign Analysis Benefits:**
- Complete campaign ROI analysis
- Ad performance optimization
- Budget allocation insights
- Campaign comparison capabilities

### **Business Intelligence Benefits:**
- Data-driven product decisions
- Optimized campaign strategies
- Improved profit margins
- Enhanced customer insights

## 🔍 **Data Quality Requirements**

### **SKU Data Quality:**
- Complete product categorization
- Accurate cost and pricing data
- Consistent SKU naming
- Product lifecycle tracking

### **Campaign Data Quality:**
- Complete ad performance metrics
- Accurate attribution data
- Consistent campaign naming
- Regular data validation

## 📋 **Next Steps**

1. **Audit Current Data**: Review existing data quality and completeness
2. **Design Enhanced Schema**: Implement the recommended table structures
3. **Data Migration**: Migrate existing data to enhanced structure
4. **Create Views**: Implement the enhanced analytical views
5. **Testing**: Validate data accuracy and view performance
6. **Documentation**: Update documentation and user guides
