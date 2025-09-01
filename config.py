# PostgreSQL Database Configuration
# Update these values with your actual database credentials

POSTGRESQL_CONFIG = {
    'host': 'selericdb.postgres.database.azure.com',           # Your PostgreSQL host
    'port': 5432,                  # PostgreSQL default port
    'database': 'postgres',  # Your database name
    'user': 'admin_seleric',       # Your database username
    'password': 'SelericDB246'    # Your database password
}

# Analysis Configuration
TIME_RANGE_DAYS = 30  # Number of days to look back for attribution analysis

# Date Range Configuration (Global)
# Use specific date range instead of relative days
USE_DATE_RANGE = True  # Set to False to use TIME_RANGE_DAYS instead
START_DATE = "2025-07-01"  # Start date for analysis (YYYY-MM-DD)
END_DATE = "2025-07-30"    # End date for analysis (YYYY-MM-DD)

# Output Configuration
OUTPUT_PREFIX = "attribution_analysis"  # Prefix for output files

# Database Schema Information
# Tables:
# - shopify_orders (with customer_journey and custom_attributes JSONB columns)
# - ads_insights_hourly (hourly ad performance data)
