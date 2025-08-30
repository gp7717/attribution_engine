# Attribution ETL Pipeline

A comprehensive ETL (Extract, Transform, Load) pipeline for attribution data processing, optimized for performance and scalability.

## 🚀 Features

- **Scalable Processing**: Uses PySpark for large-scale data processing
- **Optimized Storage**: Proper database schema with indexing for fast queries
- **Materialized Views**: Pre-aggregated views for instant analytics
- **Incremental Processing**: Support for incremental data updates
- **Data Quality**: Built-in validation and quality checks
- **Performance Monitoring**: Comprehensive monitoring and alerting
- **Flexible Configuration**: Environment-specific configurations

## 📊 Architecture

### ETL Flow
```
Source Tables → Extract → Transform (PySpark/Pandas) → Load → Views
     ↓              ↓           ↓                    ↓        ↓
shopify_orders   Raw Data   Attribution Logic   Optimized   Analytics
ads_insights     Validation  Channel Mapping    Tables     Views
```

### Database Schema
- **`attribution_raw`**: Raw processed attribution data
- **`campaign_performance`**: Campaign-level performance metrics
- **`channel_performance`**: Channel-level performance metrics
- **Materialized Views**: Pre-aggregated analytics views

## 🛠️ Installation

### 1. Install Dependencies
```bash
pip install -r requirements_etl.txt
```

### 2. Configure Database
Update `attribution_etl_config.py` with your PostgreSQL credentials:
```python
POSTGRESQL_CONFIG = {
    'host': 'your_host',
    'port': 5432,
    'database': 'your_database',
    'user': 'your_username',
    'password': 'your_password'
}
```

### 3. Setup PySpark (Optional)
If you want to use PySpark for large-scale processing:
```bash
# Install Java (required for PySpark)
# On macOS:
brew install openjdk@11

# On Ubuntu:
sudo apt-get install openjdk-11-jdk

# Set JAVA_HOME
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
```

## 🚀 Usage

### Command Line Interface

#### Basic Usage
```bash
# Run ETL for last 30 days
python attribution_etl_runner.py --days 30

# Run ETL for specific date range
python attribution_etl_runner.py --start-date 2025-01-01 --end-date 2025-01-31

# Run in development mode
python attribution_etl_runner.py --environment development --days 7
```

#### Advanced Options
```bash
# Run without PySpark (Pandas only)
python attribution_etl_runner.py --no-spark --days 30

# Dry run (no data processing)
python attribution_etl_runner.py --dry-run --days 30

# Validate data only
python attribution_etl_runner.py --validate-only --days 30

# Refresh materialized views only
python attribution_etl_runner.py --refresh-views-only

# Custom configuration
python attribution_etl_runner.py --config-file custom_config.py --days 30
```

### Programmatic Usage

```python
from attribution_etl_architecture import AttributionETL
from attribution_etl_config import DEFAULT_CONFIG

# Initialize ETL pipeline
etl = AttributionETL(
    db_config=DEFAULT_CONFIG['postgresql'],
    spark_config=DEFAULT_CONFIG['spark']
)

# Run ETL pipeline
etl.run_etl_pipeline(
    start_date='2025-01-01',
    end_date='2025-01-31',
    use_spark=True
)

# Get analytics
analytics = etl.get_attribution_analytics('2025-01-01', '2025-01-31')
```

## 📈 Performance Optimization

### PySpark Configuration
The pipeline includes optimized PySpark settings:
- Adaptive query execution
- Dynamic partition pruning
- Columnar data processing
- Memory optimization

### Database Optimization
- Proper indexing on key columns
- Partitioned tables by date
- Materialized views for fast queries
- Connection pooling

### Processing Optimization
- Incremental processing support
- Batch processing with configurable sizes
- Parallel processing where possible
- Memory-efficient data structures

## 📊 Analytics Views

The pipeline creates several materialized views for fast analytics:

### 1. Daily Attribution Summary
```sql
SELECT * FROM v_daily_attribution_summary 
WHERE date_start >= '2025-01-01' AND date_start <= '2025-01-31';
```

### 2. Channel Performance
```sql
SELECT * FROM v_channel_performance 
ORDER BY total_revenue DESC;
```

### 3. Campaign Performance
```sql
SELECT * FROM v_campaign_performance 
WHERE channel = 'Facebook'
ORDER BY total_revenue DESC;
```

### 4. SKU Performance
```sql
SELECT * FROM v_sku_performance 
ORDER BY revenue DESC
LIMIT 100;
```

### 5. Attribution Quality Analysis
```sql
SELECT * FROM v_attribution_quality_analysis 
WHERE date_start >= '2025-01-01';
```

## 🔧 Configuration

### Environment-Specific Configs
```python
# Development
config = get_config_for_environment('development')

# Staging
config = get_config_for_environment('staging')

# Production
config = get_config_for_environment('production')
```

### Custom Configuration
Create a custom config file:
```python
# custom_config.py
POSTGRESQL_CONFIG = {
    'host': 'custom_host',
    'port': 5432,
    'database': 'custom_db',
    'user': 'custom_user',
    'password': 'custom_password'
}

SPARK_CONFIG = {
    'spark.master': 'yarn',
    'spark.executor.memory': '4g',
    'spark.executor.cores': '2'
}
```

## 📋 Data Quality

### Validation Rules
- Required field validation
- Data type validation
- Range validation (e.g., order values)
- Referential integrity checks

### Quality Metrics
- Attribution rate monitoring
- Data completeness tracking
- Anomaly detection
- Performance monitoring

### Error Handling
- Retry mechanisms with exponential backoff
- Dead letter queue for failed records
- Comprehensive error logging
- Alerting on critical failures

## 🔄 Incremental Processing

### Watermark-Based Processing
```python
# Enable incremental processing
INCREMENTAL_CONFIG = {
    'enable_incremental': True,
    'watermark_column': 'updated_at',
    'watermark_lag_hours': 1
}
```

### Checkpoint Management
- Automatic checkpoint creation
- State store for processing state
- Recovery from failures
- Backfill support

## 📊 Monitoring

### Performance Metrics
- Processing time tracking
- Memory usage monitoring
- Throughput measurement
- Error rate tracking

### Alerting
- Email alerts on failures
- Performance degradation alerts
- Data quality alerts
- Custom alert rules

### Logging
- Structured logging with JSON format
- Log rotation and retention
- Context-aware logging
- Multiple log destinations

## 🚀 Deployment

### Local Development
```bash
# Install dependencies
pip install -r requirements_etl.txt

# Run ETL
python attribution_etl_runner.py --environment development --days 7
```

### Production Deployment
```bash
# Install dependencies
pip install -r requirements_etl.txt

# Run ETL with monitoring
python attribution_etl_runner.py --environment production --days 30
```

### Airflow Integration
```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from attribution_etl_architecture import AttributionETL

def run_attribution_etl():
    etl = AttributionETL(db_config=POSTGRESQL_CONFIG)
    etl.run_etl_pipeline(
        start_date='{{ ds }}',
        end_date='{{ ds }}',
        use_spark=True
    )

dag = DAG('attribution_etl', schedule_interval='@daily')
etl_task = PythonOperator(
    task_id='run_attribution_etl',
    python_callable=run_attribution_etl,
    dag=dag
)
```

## 🔍 Troubleshooting

### Common Issues

#### 1. PySpark Not Available
```bash
# Install Java first
brew install openjdk@11  # macOS
sudo apt-get install openjdk-11-jdk  # Ubuntu

# Set JAVA_HOME
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
```

#### 2. Memory Issues
```python
# Reduce batch size
SPARK_CONFIG = {
    'spark.executor.memory': '2g',
    'spark.driver.memory': '2g'
}
```

#### 3. Database Connection Issues
```python
# Check connection string
connection_string = f"postgresql://{user}:{password}@{host}:{port}/{database}"
```

#### 4. Performance Issues
```python
# Enable adaptive query execution
SPARK_CONFIG = {
    'spark.sql.adaptive.enabled': 'true',
    'spark.sql.adaptive.coalescePartitions.enabled': 'true'
}
```

### Debug Mode
```bash
# Run with debug logging
python attribution_etl_runner.py --log-level DEBUG --days 7
```

### Validation Mode
```bash
# Validate data without processing
python attribution_etl_runner.py --validate-only --days 30
```

## 📚 API Reference

### AttributionETL Class
```python
class AttributionETL:
    def __init__(self, db_config: Dict, spark_config: Dict = None)
    def run_etl_pipeline(self, start_date: str, end_date: str, use_spark: bool = True)
    def get_attribution_analytics(self, start_date: str, end_date: str) -> Dict
    def create_attribution_schema(self) -> None
    def refresh_materialized_views(self) -> None
```

### Configuration Functions
```python
def get_config_for_environment(env: str = 'production') -> Dict
```

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests
5. Submit a pull request

## 📄 License

This project is licensed under the MIT License.

## 🆘 Support

For support and questions:
- Create an issue in the repository
- Check the troubleshooting section
- Review the logs for error details
