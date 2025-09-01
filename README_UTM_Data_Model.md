# UTM Data Model for Attribution Analysis

## Overview

This UTM data model is designed to store and analyze UTM tracking data from customer journeys, enabling you to join UTM data with ad insights (`ads_insights_hourly`) to get comprehensive hourly purchase/sale analysis. The model provides a complete attribution framework that tracks customer sessions, maps them to orders, and creates hourly aggregations for performance analysis.

## Key Features

- **Session-level UTM tracking** with full customer journey data
- **Order mapping** for revenue attribution and conversion tracking
- **Hourly aggregation** optimized for joining with `ads_insights_hourly`
- **Configurable channel mapping** system for consistent classification
- **Pre-built analytical views** for common attribution analysis
- **Performance-optimized indexes** for fast queries
- **Multi-touch attribution** support (first-click, last-click, linear, time-decay)

## Database Schema

### 1. UTM Sessions Table (`utm_sessions`)

Stores individual UTM sessions/visits with timestamps and conversion data.

**Key Fields:**
- `session_id`: Unique session identifier
- `session_token`: Session token for tracking
- `utm_source`, `utm_medium`, `utm_campaign`, `utm_content`, `utm_term`: UTM parameters
- `campaign_id`, `ad_id`: Campaign and ad identifiers
- `session_start_time`, `session_end_time`: Session timestamps
- `is_conversion_session`: Boolean flag for conversion sessions
- `conversion_value`: Revenue value for conversion sessions

**Use Cases:**
- Track individual customer journeys
- Analyze session-level conversion data
- Understand customer behavior patterns

### 2. UTM Order Mapping Table (`utm_order_mapping`)

Maps UTM sessions to actual Shopify orders for revenue attribution.

**Key Fields:**
- `order_id`: Shopify order identifier
- `session_id`: Reference to UTM session
- `attribution_model`: Attribution method used
- `attribution_weight`: Weight for multi-touch attribution
- `channel`, `sub_channel`: Marketing channel classification

**Use Cases:**
- Link UTM data to actual revenue
- Implement different attribution models
- Track order-level performance by channel

### 3. UTM Hourly Metrics Table (`utm_hourly_metrics`)

Pre-aggregated UTM data by hour for fast joins with `ads_insights_hourly`.

**Key Fields:**
- `date_start`, `hour_start`: Time dimensions
- `hour_window`: Hour window format matching `ads_insights_hourly`
- `total_sessions`, `total_conversions`: Session and conversion metrics
- `total_revenue`, `total_orders`: Revenue and order metrics
- `attributed_revenue`, `attributed_orders`: Attribution metrics

**Use Cases:**
- Hourly performance analysis
- Fast joins with ad insights data
- Real-time dashboard data

### 4. UTM Channel Mapping Table (`utm_channel_mapping`)

Configurable mapping of UTM parameters to marketing channels.

**Key Fields:**
- `utm_source_pattern`, `utm_medium_pattern`: Regex patterns for matching
- `channel`, `sub_channel`: Channel classification
- `platform`: Marketing platform (Facebook, Google, etc.)
- `priority`: Priority for overlapping patterns

**Use Cases:**
- Consistent channel classification
- Easy channel mapping updates
- Platform-specific analysis

## Installation and Setup

### 1. Database Setup

```sql
-- Run the UTM data model schema
\i utm_data_model.sql
```

### 2. Python Dependencies

```bash
pip install psycopg2-binary pandas sqlalchemy
```

### 3. Configuration

Update the database configuration in `utm_data_processor.py`:

```python
db_config = {
    'host': 'your_host',
    'port': 5432,
    'database': 'your_database',
    'user': 'your_username',
    'password': 'your_password'
}
```

## Usage Examples

### 1. Process Shopify Orders to UTM Data

```python
from utm_data_processor import UTMDataProcessor

# Initialize processor
processor = UTMDataProcessor(db_config)

# Process orders for the last 30 days
start_date = '2024-01-01'
end_date = '2024-01-31'
processor.process_shopify_orders_to_utm(start_date, end_date)
```

### 2. Get Hourly Purchase Analysis

```python
# Retrieve hourly analysis by joining UTM data with ad insights
analysis_df = processor.get_hourly_purchase_analysis(start_date, end_date)

# Display results
print(analysis_df.head())
```

### 3. Direct SQL Queries

#### Hourly UTM + Ad Performance (Main View)

```sql
SELECT * FROM v_utm_ad_hourly_performance 
WHERE date_start = CURRENT_DATE - INTERVAL '1 day'
AND channel = 'Facebook'
ORDER BY hour_start DESC;
```

#### Daily UTM Performance Summary

```sql
SELECT * FROM v_utm_daily_summary 
WHERE date_start >= CURRENT_DATE - INTERVAL '7 days'
ORDER BY date_start DESC, total_revenue DESC;
```

#### Campaign Performance with UTM Attribution

```sql
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
```

## Data Flow

### 1. Data Ingestion

```
Shopify Orders → UTM Sessions → UTM Order Mapping → Hourly Metrics
```

### 2. Analysis Pipeline

```
UTM Hourly Metrics + ads_insights_hourly → Hourly Purchase Analysis
```

### 3. Attribution Models

- **Last Click**: Attributes conversion to the last UTM source
- **First Click**: Attributes conversion to the first UTM source
- **Linear**: Distributes attribution equally across all touchpoints
- **Time Decay**: Gives more weight to recent touchpoints

## Performance Optimization

### Indexes

The schema includes comprehensive indexes for:
- Date and time-based queries
- Campaign and ad ID lookups
- UTM parameter filtering
- Channel-based analysis

### Partitioning

For large datasets, consider partitioning the `utm_hourly_metrics` table by date:

```sql
-- Example partitioning (PostgreSQL 10+)
CREATE TABLE utm_hourly_metrics_2024_01 PARTITION OF utm_hourly_metrics
FOR VALUES FROM ('2024-01-01') TO ('2024-02-01');
```

### Materialized Views

For frequently accessed aggregations, create materialized views:

```sql
CREATE MATERIALIZED VIEW mv_utm_daily_summary AS
SELECT * FROM v_utm_daily_summary;

-- Refresh periodically
REFRESH MATERIALIZED VIEW mv_utm_daily_summary;
```

## Data Quality and Validation

### Completeness Score

The system calculates a data completeness score (0-100) based on:
- Required field completion
- Attribution logic consistency
- Financial data accuracy

### Data Validation

```sql
-- Check for data quality issues
SELECT 
    batch_id,
    COUNT(*) as total_records,
    calculate_attribution_data_quality_score(batch_id) as quality_score
FROM utm_hourly_metrics
WHERE created_at >= CURRENT_DATE - INTERVAL '1 day'
GROUP BY batch_id
ORDER BY created_at DESC;
```

## Integration with Existing Systems

### 1. AttributionEngine.py

The UTM data model complements your existing `AttributionEngine.py` by:
- Providing structured UTM data storage
- Enabling hourly analysis capabilities
- Supporting multiple attribution models

### 2. ads_insights_hourly

The model is designed to seamlessly join with your existing `ads_insights_hourly` table:
- Matching on date, hour, campaign_id, and ad_id
- Providing UTM context for ad performance
- Enabling comprehensive ROI analysis

### 3. Shopify Data

Integrates with your existing Shopify order data:
- Extracts UTM parameters from `customer_utm_*` fields
- Maps orders to UTM sessions
- Tracks conversion attribution

## Monitoring and Maintenance

### 1. Data Processing Logs

Monitor the `utm_data_processor.log` file for:
- Processing errors
- Data quality issues
- Performance metrics

### 2. Database Monitoring

```sql
-- Check table sizes
SELECT 
    schemaname,
    tablename,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as size
FROM pg_tables
WHERE tablename LIKE 'utm_%'
ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC;

-- Check index usage
SELECT 
    schemaname,
    tablename,
    indexname,
    idx_scan,
    idx_tup_read,
    idx_tup_fetch
FROM pg_stat_user_indexes
WHERE tablename LIKE 'utm_%'
ORDER BY idx_scan DESC;
```

### 3. Regular Maintenance

```sql
-- Update statistics
ANALYZE utm_sessions;
ANALYZE utm_order_mapping;
ANALYZE utm_hourly_metrics;

-- Clean up old data (if needed)
DELETE FROM utm_sessions 
WHERE session_start_time < CURRENT_DATE - INTERVAL '90 days';
```

## Troubleshooting

### Common Issues

1. **Data Mismatch**: Ensure campaign_id and ad_id values match between UTM data and ad insights
2. **Performance Issues**: Check index usage and consider partitioning for large datasets
3. **Data Quality**: Monitor completeness scores and validate attribution logic

### Debug Queries

```sql
-- Check for orphaned records
SELECT COUNT(*) FROM utm_order_mapping uom
LEFT JOIN utm_sessions us ON uom.session_id = us.session_id
WHERE us.session_id IS NULL;

-- Verify hourly aggregation
SELECT 
    date_start,
    hour_start,
    COUNT(*) as record_count,
    SUM(total_revenue) as total_revenue
FROM utm_hourly_metrics
WHERE date_start >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY date_start, hour_start
ORDER BY date_start DESC, hour_start;
```

## Future Enhancements

### 1. Real-time Processing

- Implement streaming data processing
- Add real-time attribution updates
- Support for live dashboards

### 2. Advanced Attribution

- Machine learning-based attribution models
- Customer lifetime value integration
- Cross-device attribution

### 3. API Integration

- REST API for data access
- Webhook support for real-time updates
- Third-party platform integrations

## Support and Documentation

For additional support:
- Check the logs in `utm_data_processor.log`
- Review the sample queries in the schema file
- Monitor database performance metrics
- Validate data quality scores

## License

This UTM data model is designed to work with your existing attribution analysis system and follows PostgreSQL best practices for data warehousing and analytics.
