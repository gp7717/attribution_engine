# Platform-Specific Attribution ETL Pipeline

Enhanced ETL pipeline with separate processing for **Google** and **Meta** (Facebook/Instagram) platforms, handling their different UTM structures and attribution models.

## 🎯 **Why Platform-Specific Views?**

### **Google vs Meta Differences:**

| Aspect | Google | Meta (Facebook/Instagram) |
|--------|--------|---------------------------|
| **UTM Structure** | Campaign → Ad Group → Keyword | Campaign → Ad Set → Ad |
| **Attribution ID** | Content = Ad Group, Term = Keyword | Content = Ad ID, Term = Ad Set |
| **Campaign Hierarchy** | Campaign → Ad Group → Keyword | Campaign → Ad Set → Ad |
| **Performance Metrics** | CTR, CPC, Quality Score | CTR, CPM, Engagement Rate |
| **Attribution Model** | Last-click, Time-decay | 1-day, 7-day, 28-day view |

## 🏗️ **Platform-Specific Architecture**

### **Enhanced Database Schema:**
- **`attribution_raw_platform_specific`** - Main table with platform-specific columns
- **`google_campaign_performance`** - Google-specific performance metrics
- **`meta_campaign_performance`** - Meta-specific performance metrics
- **Platform-specific materialized views** for fast analytics

### **Platform Detection Logic:**
```python
def determine_platform(source, medium, campaign):
    if source in ['google', 'googleads'] or medium in ['cpc', 'paid_search']:
        return 'Google'
    elif source in ['facebook', 'instagram'] or medium in ['social']:
        return 'Meta'
    else:
        return 'Other'
```

## 📊 **Platform-Specific Views**

### **Google Platform Views:**

#### 1. **`v_google_campaign_performance`** - Google Campaign Analysis
```sql
SELECT * FROM v_google_campaign_performance 
WHERE google_campaign = 'Summer Sale 2025'
ORDER BY total_revenue DESC;
```

**What you can see:**
- **Campaign performance** by Google campaign
- **Ad group performance** (utm_content)
- **Keyword performance** (utm_term)
- **Match type analysis** (exact, phrase, broad)
- **Network performance** (search, display, shopping)

**Example insights:**
- "Campaign 'Summer Sale' has 15 ad groups with ₹50,000 total revenue"
- "Ad group 'Product A' generates highest revenue with ₹15,000"
- "Keyword 'buy product' has 8% CTR and ₹2.50 CPC"

#### 2. **`v_google_keyword_performance`** - Keyword Deep Dive
```sql
SELECT * FROM v_google_keyword_performance 
WHERE google_campaign = 'Product Campaign'
ORDER BY revenue DESC;
```

**What you can see:**
- **Keyword-level performance** with revenue and orders
- **Match type effectiveness** (exact vs phrase vs broad)
- **Ad group performance** by keyword
- **Attribution quality** by keyword type

**Example insights:**
- "Exact match keywords have 90% attribution rate vs 60% for broad match"
- "Keyword 'premium product' generates ₹5,000 revenue with 2.5% CTR"
- "Ad group 'Product A' has 25 keywords with varying performance"

#### 3. **`v_google_ad_group_performance`** - Ad Group Analysis
```sql
SELECT * FROM v_google_ad_group_performance 
ORDER BY revenue DESC;
```

**What you can see:**
- **Ad group performance** across campaigns
- **Keyword diversity** per ad group
- **Ad group efficiency** metrics
- **Performance trends** over time

---

### **Meta Platform Views:**

#### 4. **`v_meta_campaign_performance`** - Meta Campaign Analysis
```sql
SELECT * FROM v_meta_campaign_performance 
WHERE meta_platform = 'Facebook'
ORDER BY total_revenue DESC;
```

**What you can see:**
- **Campaign performance** by Meta campaign
- **Ad set performance** (campaign → ad set → ad hierarchy)
- **Ad-level performance** with individual ad metrics
- **Platform comparison** (Facebook vs Instagram)

**Example insights:**
- "Campaign 'Holiday Sale' has 8 ad sets with ₹75,000 revenue"
- "Ad set 'Lookalike 1%' generates highest revenue with ₹20,000"
- "Ad 'Video Creative A' has 95% attribution rate"

#### 5. **`v_meta_adset_performance`** - Ad Set Analysis
```sql
SELECT * FROM v_meta_adset_performance 
WHERE meta_platform = 'Instagram'
ORDER BY revenue DESC;
```

**What you can see:**
- **Ad set performance** across campaigns
- **Ad diversity** per ad set
- **Platform-specific performance** (Facebook vs Instagram)
- **Attribution quality** by ad set

**Example insights:**
- "Instagram ad sets have 85% attribution rate vs 75% for Facebook"
- "Ad set 'Interest Targeting' has 12 unique ads"
- "Lookalike ad sets perform 40% better than interest-based"

#### 6. **`v_meta_ad_performance`** - Individual Ad Analysis
```sql
SELECT * FROM v_meta_ad_performance 
WHERE meta_platform = 'Facebook'
ORDER BY revenue DESC;
```

**What you can see:**
- **Individual ad performance** with detailed metrics
- **Creative performance** analysis
- **Ad-level attribution** quality
- **Performance trends** by ad

---

### **Cross-Platform Views:**

#### 7. **`v_platform_performance_comparison`** - Google vs Meta
```sql
SELECT * FROM v_platform_performance_comparison 
ORDER BY total_revenue DESC;
```

**What you can see:**
- **Platform comparison** (Google vs Meta vs Other)
- **Revenue distribution** across platforms
- **Attribution rates** by platform
- **Performance metrics** comparison

**Example insights:**
- "Google generates 60% of revenue with 85% attribution rate"
- "Meta has 90% attribution rate but lower average order value"
- "Google campaigns have higher CTR but Meta has better conversion rates"

#### 8. **`v_attribution_source_effectiveness_by_platform`** - Attribution Quality
```sql
SELECT * FROM v_attribution_source_effectiveness_by_platform 
ORDER BY platform_group, total_revenue DESC;
```

**What you can see:**
- **Attribution source effectiveness** by platform
- **Data quality** comparison across platforms
- **Attribution method performance** (journey vs attributes vs direct)

---

## 🚀 **Usage Examples**

### **Command Line Interface:**
```bash
# Run platform-specific ETL
python attribution_etl_architecture_platform_specific.py

# Run with specific date range
python attribution_etl_runner.py --start-date 2025-01-01 --end-date 2025-01-31
```

### **Programmatic Usage:**
```python
from attribution_etl_architecture_platform_specific import PlatformSpecificAttributionETL
from attribution_etl_config import DEFAULT_CONFIG

# Initialize platform-specific ETL
etl = PlatformSpecificAttributionETL(DEFAULT_CONFIG['postgresql'])

# Run ETL
etl.run_platform_specific_etl('2025-01-01', '2025-01-31')

# Get platform-specific analytics
analytics = etl.get_platform_specific_analytics('2025-01-01', '2025-01-31')
```

## 📈 **Platform-Specific Analytics**

### **Google Analytics:**
```python
# Google campaign performance
google_performance = analytics['google_performance']
print(f"Google campaigns: {len(google_performance)}")

# Google keyword performance
google_keywords = analytics['google_keyword_performance']
top_keywords = google_keywords.head(10)
print(f"Top keywords: {top_keywords['google_keyword'].tolist()}")
```

### **Meta Analytics:**
```python
# Meta campaign performance
meta_performance = analytics['meta_performance']
print(f"Meta campaigns: {len(meta_performance)}")

# Meta ad performance
meta_ads = analytics['meta_ad_performance']
top_ads = meta_ads.head(10)
print(f"Top ads: {top_ads['meta_ad_name'].tolist()}")
```

### **Cross-Platform Comparison:**
```python
# Platform comparison
platform_comparison = analytics['platform_comparison']
google_revenue = platform_comparison[platform_comparison['platform_group'] == 'Google']['total_revenue'].sum()
meta_revenue = platform_comparison[platform_comparison['platform_group'] == 'Meta']['total_revenue'].sum()
print(f"Google: ₹{google_revenue:,.2f}, Meta: ₹{meta_revenue:,.2f}")
```

## 🔍 **Platform-Specific Insights**

### **Google Insights:**
- **Keyword Performance**: Which keywords drive highest revenue
- **Ad Group Efficiency**: Which ad groups are most profitable
- **Match Type Analysis**: Exact vs phrase vs broad match performance
- **Network Performance**: Search vs display vs shopping performance
- **Quality Score Impact**: How quality scores affect performance

### **Meta Insights:**
- **Campaign Structure**: Campaign → ad set → ad performance hierarchy
- **Creative Performance**: Which ad creatives perform best
- **Audience Targeting**: Lookalike vs interest-based performance
- **Platform Comparison**: Facebook vs Instagram performance
- **Attribution Windows**: 1-day vs 7-day vs 28-day attribution

### **Cross-Platform Insights:**
- **Platform ROI**: Which platform provides better ROI
- **Attribution Quality**: Which platform has better attribution data
- **Customer Behavior**: Different customer journeys by platform
- **Seasonal Performance**: Platform performance during different seasons
- **Budget Allocation**: Optimal budget distribution across platforms

## 📊 **Sample Queries**

### **Google-Specific Queries:**
```sql
-- Top performing Google keywords
SELECT google_campaign, google_keyword, 
       SUM(total_orders) as orders,
       SUM(total_revenue) as revenue,
       AVG(avg_order_value) as aov
FROM v_google_keyword_performance 
GROUP BY google_campaign, google_keyword
ORDER BY revenue DESC
LIMIT 10;

-- Google ad group performance
SELECT google_campaign, google_ad_group,
       SUM(total_orders) as orders,
       SUM(total_revenue) as revenue
FROM v_google_ad_group_performance 
GROUP BY google_campaign, google_ad_group
ORDER BY revenue DESC;
```

### **Meta-Specific Queries:**
```sql
-- Top performing Meta ads
SELECT meta_campaign_name, meta_ad_name, meta_platform,
       SUM(total_orders) as orders,
       SUM(total_revenue) as revenue
FROM v_meta_ad_performance 
GROUP BY meta_campaign_name, meta_ad_name, meta_platform
ORDER BY revenue DESC
LIMIT 10;

-- Meta platform comparison
SELECT meta_platform,
       SUM(total_orders) as orders,
       SUM(total_revenue) as revenue,
       AVG(avg_order_value) as aov
FROM v_meta_campaign_performance 
GROUP BY meta_platform
ORDER BY revenue DESC;
```

### **Cross-Platform Queries:**
```sql
-- Platform performance comparison
SELECT platform_group,
       SUM(total_orders) as orders,
       SUM(total_revenue) as revenue,
       AVG(attribution_rate) as avg_attribution_rate
FROM v_platform_performance_comparison 
GROUP BY platform_group
ORDER BY revenue DESC;

-- Attribution source effectiveness
SELECT platform_group, attribution_source,
       SUM(total_orders) as orders,
       SUM(total_revenue) as revenue
FROM v_attribution_source_effectiveness_by_platform 
GROUP BY platform_group, attribution_source
ORDER BY platform_group, revenue DESC;
```

## 🎯 **Business Questions You Can Answer**

### **Google-Specific Questions:**
- "Which Google keywords have the highest ROAS?"
- "What's the performance difference between exact and broad match keywords?"
- "Which ad groups are most efficient for our product categories?"
- "How does quality score impact our campaign performance?"

### **Meta-Specific Questions:**
- "Which Facebook ad creatives drive the highest conversion rates?"
- "How do Instagram campaigns compare to Facebook campaigns?"
- "Which audience targeting methods are most effective?"
- "What's the optimal attribution window for our Meta campaigns?"

### **Cross-Platform Questions:**
- "Should we allocate more budget to Google or Meta?"
- "Which platform has better attribution data quality?"
- "How do customer acquisition costs compare across platforms?"
- "What's the lifetime value difference between Google and Meta customers?"

## 🔧 **Configuration**

### **Platform-Specific Settings:**
```python
# Google-specific configuration
GOOGLE_CONFIG = {
    'utm_mapping': {
        'utm_source': 'google',
        'utm_medium': 'cpc',
        'utm_campaign': 'campaign_name',
        'utm_content': 'ad_group',
        'utm_term': 'keyword'
    },
    'attribution_priority': ['ad_group', 'campaign', 'keyword']
}

# Meta-specific configuration
META_CONFIG = {
    'utm_mapping': {
        'utm_source': 'facebook',  # or instagram
        'utm_medium': 'social',
        'utm_campaign': 'campaign_name',
        'utm_content': 'ad_id',
        'utm_term': 'adset_name'
    },
    'attribution_priority': ['ad', 'campaign', 'adset']
}
```

## 📋 **Next Steps**

1. **Install Dependencies**: `pip install -r requirements_etl.txt`
2. **Configure Platform Detection**: Update platform detection logic
3. **Run Platform-Specific ETL**: Process data with platform-specific logic
4. **Query Platform Views**: Use specialized views for each platform
5. **Compare Performance**: Use cross-platform views for optimization

This platform-specific approach gives you granular insights into each platform's performance while maintaining the ability to compare across platforms for strategic decision-making.
