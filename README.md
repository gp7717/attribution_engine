# Attribution Analysis Engine

This tool performs attribution analysis by mapping Shopify orders to their originating marketing channels using PostgreSQL database tables.

## Database Schema

The tool works with two main tables:

### 1. `shopify_orders` Table
Contains order data with attribution information:
- **Primary Key**: `order_id` (text)
- **Key Attribution Columns**:
  - `customer_journey` (JSONB) - Customer journey data with UTM parameters
  - `custom_attributes` (JSONB) - Custom attributes with UTM data
  - `customer_utm_source`, `customer_utm_medium`, `customer_utm_campaign`, `customer_utm_content`, `customer_utm_term` (text)
- **Order Data**: `order_name`, `created_at`, `total_price_amount`, etc.

### 2. `ads_insights_hourly` Table
Contains hourly ad performance data:
- **Campaign Structure**:
  - `campaign_id` (bigint) - Campaign identifier
  - `campaign_name` (text) - Campaign name
  - `adset_id` (bigint) - Ad set identifier
  - `adset_name` (text) - Ad set name
  - `ad_id` (bigint) - Ad identifier
  - `ad_name` (text) - Ad name
- **Time & Performance**:
  - `date_start`, `date_stop` (date) - Date range
  - `hourly_window` (text) - Hour window
  - `impressions`, `clicks`, `spend` (bigint/numeric) - Core metrics
  - `cpm`, `cpc`, `ctr` (numeric) - Calculated metrics
- **Conversion Actions**:
  - `action_onsite_web_purchase` (bigint) - Purchase actions
  - `value_onsite_web_purchase` (numeric) - Purchase value
  - `action_onsite_web_add_to_cart` (bigint) - Add to cart actions
  - `action_onsite_web_initiate_checkout` (bigint) - Checkout actions
  - `action_onsite_web_view_content` (bigint) - Content view actions
  - `action_link_click` (bigint) - Link click actions

## Attribution Logic

The engine uses a hierarchical attribution approach:

1. **Primary Source**: `customer_journey` JSONB column
   - Parses the JSON structure to find UTM parameters
   - Uses last-click attribution (last valid content ID)

2. **Secondary Source**: `custom_attributes` JSONB column
   - Extracts UTM parameters from custom attributes
   - Falls back to campaign/medium if content ID not available

3. **Fallback**: Direct UTM columns
   - Uses `customer_utm_*` columns as final fallback

4. **Default**: "Direct/Unknown" for unattributed orders

## Setup Instructions

### 1. Install Dependencies
```bash
pip install pandas sqlalchemy psycopg2-binary
```

### 2. Configure Database Connection
Edit `config.py` with your PostgreSQL credentials:
```python
POSTGRESQL_CONFIG = {
    'host': 'your_host',
    'port': 5432,
    'database': 'your_database',
    'user': 'your_username',
    'password': 'your_password'
}
```

### 3. Configure Date Range (Optional)
You can specify a custom date range for analysis:
```python
# Use specific date range
USE_DATE_RANGE = True
START_DATE = "2025-01-01"  # Start date (YYYY-MM-DD)
END_DATE = "2025-12-31"    # End date (YYYY-MM-DD)

# Or use relative days (default)
USE_DATE_RANGE = False
TIME_RANGE_DAYS = 30  # Look back 30 days from today
```

### 3. Run the Analysis
```bash
python AttributionEngine.py
```

## Output Files

The tool generates a comprehensive Excel file with multiple sheets:

### **Main Excel File: `attribution_analysis_YYYYMMDD_HHMMSS.xlsx`**

**Sheets included:**

1. **`Raw_Attribution_Data`** - Complete attribution data for every order with:
   - Order details (ID, name, date, value, shipping info)
   - Attribution information (channel, source, type)
   - UTM parameters (parsed and raw)
   - Campaign data (campaign, adset, ad details)
   - Ad performance metrics (impressions, clicks, spend, conversions)
   - Investigation flags (has customer journey, JSON lengths)

2. **`Attributed_Orders_Only`** - Filtered view of only attributed orders

3. **`Unattributed_Orders`** - Orders that couldn't be attributed for investigation

4. **`Raw_Data_Investigation`** - Sample of orders with raw JSON data for debugging

5. **`Channel_Breakdown`** - Performance summary by channel

6. **`Attribution_Source_Breakdown`** - Breakdown by attribution source (customer_journey, custom_attributes, direct_utm)

7. **`Campaign_Performance`** - Detailed campaign-level metrics with ROAS, CTR, conversion rates (includes ad-level details)

8. **`Campaign_Summary`** - High-level campaign summary with total sales, orders, and performance metrics

9. **`Ad_Level_Performance`** - Detailed ad-level performance breakdown with individual ad metrics

10. **`Summary_Statistics`** - Overall statistics and key metrics

### **Additional CSV File:**
- **`attribution_analysis_YYYYMMDD_HHMMSS.csv`** - Same as Raw_Attribution_Data sheet for compatibility

## Key Features

- **Robust JSON Parsing**: Handles malformed JSON in customer journey and custom attributes
- **Flexible Date Range**: Use specific date range or relative days lookback
- **Channel Mapping**: Maps UTM sources to readable channel names
- **Comprehensive Logging**: Detailed logs in `attribution_analysis.log`
- **Error Handling**: Graceful handling of missing data and connection issues

## Channel Mapping

The tool maps UTM sources to channels:
- `google` → Google
- `facebook`, `fb` → Facebook
- `instagram`, `ig` → Instagram
- `shopify` → Direct
- `duckduckgo` → DuckDuckGo
- Others → Source name as-is

## Usage Example

```python
from AttributionEngine import AttributionEngine

# Initialize with database config and date range
engine = AttributionEngine(
    time_range_days=30,
    db_config=POSTGRESQL_CONFIG,
    start_date="2025-01-01",  # Optional: specific start date
    end_date="2025-12-31"     # Optional: specific end date
)

# Run analysis
results_df, summary = engine.run_attribution_analysis()

# Access results
print(f"Total Orders: {summary['total_orders']}")
print(f"Attribution Rate: {summary['attribution_rate']:.1f}%")

## Troubleshooting

1. **Database Connection Issues**: Check your PostgreSQL credentials in `config.py`
2. **Missing Tables**: Ensure `shopify_orders` and `ads_insights_hourly` tables exist
3. **JSON Parsing Errors**: Check the log file for specific parsing issues
4. **Memory Issues**: Reduce `TIME_RANGE_DAYS` for large datasets
