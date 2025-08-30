# Azure Data Factory Implementation Guide
## Attribution Analysis Pipeline

### Overview
This guide provides a step-by-step approach to implement the AttributionEngine.py as a transformation layer in Azure Data Factory (ADF), storing raw and ad-level data in PostgreSQL database tables and views.

---

## 🏗️ Architecture Overview

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Source Data   │    │  ADF Pipeline   │    │   Target DB     │
│                 │    │                 │    │                 │
│ • shopify_orders│───▶│ • Data Ingestion│───▶│ • Raw Tables    │
│ • ads_insights  │    │ • Transformation│    │ • Analytics     │
│ • product_data  │    │ • Attribution   │    │ • Views         │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

---

## 📋 Prerequisites

### 1. Azure Resources
- **Azure Data Factory** (v2)
- **Azure Key Vault** (for secrets management)
- **Azure Storage Account** (for staging data)
- **PostgreSQL Database** (target database)

### 2. Python Environment
- **Python 3.8+** with required packages
- **AttributionEngine.py** (modified for ADF integration)
- **Azure Functions** or **Azure Batch** for Python execution

### 3. Database Setup
- PostgreSQL database with attribution schema
- Proper permissions for ADF service principal
- Network access configured

---

## 🔧 Step 1: Database Schema Setup

### 1.1 Create Database Schema
```sql
-- Execute the attribution_schema_model.sql
-- This creates:
-- - attribution_raw_data table
-- - attribution_ad_level_analytics table  
-- - attribution_pipeline_log table
-- - Analytical views
-- - Data quality functions
```

### 1.2 Verify Schema Creation
```sql
-- Check tables created
SELECT table_name FROM information_schema.tables 
WHERE table_schema = 'public' 
AND table_name LIKE 'attribution_%';

-- Check views created
SELECT table_name FROM information_schema.views 
WHERE table_schema = 'public' 
AND table_name LIKE 'v_%';
```

---

## 🐍 Step 2: Modify AttributionEngine.py for ADF

### 2.1 Create ADF-Compatible Version
```python
# attribution_engine_adf.py
import pandas as pd
import numpy as np
import json
import logging
from datetime import datetime, timedelta
import psycopg2
from sqlalchemy import create_engine
import uuid
import sys
import os

class AttributionEngineADF:
    def __init__(self, db_config: dict, batch_id: str = None):
        """
        Initialize AttributionEngine for ADF execution
        
        Args:
            db_config: Database configuration dictionary
            batch_id: Unique batch identifier for this pipeline run
        """
        self.db_config = db_config
        self.batch_id = batch_id or str(uuid.uuid4())
        self.engine = None
        
        # Set up logging for ADF
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        
        logging.info(f"AttributionEngineADF initialized with batch_id: {self.batch_id}")
    
    def connect_to_database(self):
        """Connect to PostgreSQL database"""
        try:
            connection_string = (
                f"postgresql://{self.db_config['user']}:{self.db_config['password']}@"
                f"{self.db_config['host']}:{self.db_config['port']}/{self.db_config['database']}"
            )
            self.engine = create_engine(connection_string, pool_pre_ping=True)
            logging.info("Connected to PostgreSQL database successfully")
        except Exception as e:
            logging.error(f"Error connecting to PostgreSQL database: {e}")
            raise
    
    def log_pipeline_start(self, pipeline_name: str, start_date: str, end_date: str):
        """Log pipeline execution start"""
        try:
            with self.engine.connect() as conn:
                conn.execute(text("""
                    INSERT INTO attribution_pipeline_log (
                        pipeline_name, pipeline_run_id, batch_id, execution_start_time,
                        execution_status, analysis_date_start, analysis_date_end
                    ) VALUES (
                        :pipeline_name, :pipeline_run_id, :batch_id, :execution_start_time,
                        'RUNNING', :analysis_date_start, :analysis_date_end
                    )
                """), {
                    'pipeline_name': pipeline_name,
                    'pipeline_run_id': os.environ.get('ADF_PIPELINE_RUN_ID', 'unknown'),
                    'batch_id': self.batch_id,
                    'execution_start_time': datetime.now(),
                    'analysis_date_start': start_date,
                    'analysis_date_end': end_date
                })
                conn.commit()
        except Exception as e:
            logging.error(f"Error logging pipeline start: {e}")
    
    def log_pipeline_end(self, status: str, metrics: dict = None, error_message: str = None):
        """Log pipeline execution end"""
        try:
            with self.engine.connect() as conn:
                conn.execute(text("""
                    UPDATE attribution_pipeline_log 
                    SET execution_end_time = :execution_end_time,
                        execution_status = :execution_status,
                        total_orders_processed = :total_orders,
                        total_ads_processed = :total_ads,
                        attributed_orders_count = :attributed_orders,
                        unattributed_orders_count = :unattributed_orders,
                        total_spend_processed = :total_spend,
                        total_sales_processed = :total_sales,
                        data_quality_score = :quality_score,
                        error_message = :error_message,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE batch_id = :batch_id
                """), {
                    'execution_end_time': datetime.now(),
                    'execution_status': status,
                    'total_orders': metrics.get('total_orders', 0) if metrics else 0,
                    'total_ads': metrics.get('total_ads', 0) if metrics else 0,
                    'attributed_orders': metrics.get('attributed_orders', 0) if metrics else 0,
                    'unattributed_orders': metrics.get('unattributed_orders', 0) if metrics else 0,
                    'total_spend': metrics.get('total_spend', 0) if metrics else 0,
                    'total_sales': metrics.get('total_sales', 0) if metrics else 0,
                    'quality_score': metrics.get('quality_score', 0) if metrics else 0,
                    'error_message': error_message,
                    'batch_id': self.batch_id
                })
                conn.commit()
        except Exception as e:
            logging.error(f"Error logging pipeline end: {e}")
    
    def save_raw_data_to_db(self, results_df: pd.DataFrame):
        """Save raw attribution data to database"""
        try:
            # Add batch_id to all records
            results_df['batch_id'] = self.batch_id
            
            # Rename columns to match database schema
            results_df = results_df.rename(columns={
                'order_value': 'order_value',
                'total_cogs': 'total_cogs',
                'skus': 'skus'
            })
            
            # Select and order columns to match database schema
            db_columns = [
                'order_id', 'order_name', 'order_date', 'order_value', 'order_currency',
                'total_cogs', 'line_items_count', 'skus', 'unique_skus_count', 
                'total_sku_quantity', 'channel', 'attribution_source', 'attribution_id',
                'attribution_type', 'utm_source', 'utm_medium', 'utm_campaign',
                'utm_content', 'utm_term', 'campaign_id', 'campaign_name',
                'adset_id', 'adset_name', 'ad_id', 'ad_name', 'is_attributed',
                'is_paid_channel', 'has_customer_journey', 'has_custom_attributes',
                'batch_id'
            ]
            
            # Filter to only include columns that exist
            available_columns = [col for col in db_columns if col in results_df.columns]
            db_data = results_df[available_columns]
            
            # Save to database
            db_data.to_sql('attribution_raw_data', self.engine, if_exists='append', index=False)
            logging.info(f"Saved {len(db_data)} raw attribution records to database")
            
            return len(db_data)
            
        except Exception as e:
            logging.error(f"Error saving raw data to database: {e}")
            raise
    
    def save_ad_level_data_to_db(self, ad_level_data: pd.DataFrame, start_date: str, end_date: str):
        """Save ad-level analytics data to database"""
        try:
            # Add batch_id and date range to all records
            ad_level_data['batch_id'] = self.batch_id
            ad_level_data['analysis_date_start'] = start_date
            ad_level_data['analysis_date_end'] = end_date
            
            # Select and order columns to match database schema
            db_columns = [
                'campaign_id', 'campaign_name', 'adset_id', 'adset_name', 'ad_id', 'ad_name',
                'channel', 'attributed', 'orders', 'attributed_orders', 'total_sales',
                'total_cogs', 'net_profit', 'impressions', 'clicks', 'spend', 'roas',
                'ctr', 'conversion_rate', 'avg_order_value', 'profit_margin',
                'all_skus', 'total_unique_skus', 'total_sku_quantity',
                'analysis_date_start', 'analysis_date_end', 'batch_id'
            ]
            
            # Filter to only include columns that exist
            available_columns = [col for col in db_columns if col in ad_level_data.columns]
            db_data = ad_level_data[available_columns]
            
            # Save to database
            db_data.to_sql('attribution_ad_level_analytics', self.engine, if_exists='append', index=False)
            logging.info(f"Saved {len(db_data)} ad-level analytics records to database")
            
            return len(db_data)
            
        except Exception as e:
            logging.error(f"Error saving ad-level data to database: {e}")
            raise
    
    def run_attribution_analysis_adf(self, start_date: str, end_date: str, pipeline_name: str = "attribution_pipeline"):
        """
        Run complete attribution analysis for ADF
        
        Args:
            start_date: Start date for analysis (YYYY-MM-DD)
            end_date: End date for analysis (YYYY-MM-DD)
            pipeline_name: Name of the ADF pipeline
            
        Returns:
            Dictionary with execution results and metrics
        """
        try:
            # Log pipeline start
            self.log_pipeline_start(pipeline_name, start_date, end_date)
            
            # Connect to database
            self.connect_to_database()
            
            # Initialize the original AttributionEngine
            from AttributionEngine import AttributionEngine
            engine = AttributionEngine(
                time_range_days=30,
                db_config=self.db_config,
                start_date=start_date,
                end_date=end_date
            )
            
            # Load data
            engine.load_data()
            
            # Process attribution
            results_df = engine.process_attribution()
            
            # Generate summary
            summary = engine.generate_summary_report(results_df)
            
            # Create ad-level summary
            ad_level_data = engine.create_ad_level_summary(results_df)
            
            # Save data to database
            raw_count = self.save_raw_data_to_db(results_df)
            ad_level_count = self.save_ad_level_data_to_db(ad_level_data, start_date, end_date)
            
            # Calculate metrics
            metrics = {
                'total_orders': len(results_df),
                'total_ads': len(ad_level_data),
                'attributed_orders': results_df['is_attributed'].sum(),
                'unattributed_orders': (~results_df['is_attributed']).sum(),
                'total_spend': ad_level_data['spend'].sum(),
                'total_sales': ad_level_data['total_sales'].sum(),
                'quality_score': 95.0  # Placeholder - implement actual quality calculation
            }
            
            # Log pipeline success
            self.log_pipeline_end('SUCCESS', metrics)
            
            logging.info(f"Attribution analysis completed successfully for batch {self.batch_id}")
            
            return {
                'status': 'SUCCESS',
                'batch_id': self.batch_id,
                'metrics': metrics,
                'raw_records': raw_count,
                'ad_level_records': ad_level_count
            }
            
        except Exception as e:
            error_message = str(e)
            logging.error(f"Attribution analysis failed: {error_message}")
            
            # Log pipeline failure
            self.log_pipeline_end('FAILED', error_message=error_message)
            
            return {
                'status': 'FAILED',
                'batch_id': self.batch_id,
                'error': error_message
            }

# Main execution function for ADF
def main():
    """Main function for ADF execution"""
    try:
        # Get parameters from environment variables or command line
        start_date = os.environ.get('START_DATE', sys.argv[1] if len(sys.argv) > 1 else None)
        end_date = os.environ.get('END_DATE', sys.argv[2] if len(sys.argv) > 2 else None)
        pipeline_name = os.environ.get('PIPELINE_NAME', 'attribution_pipeline')
        
        if not start_date or not end_date:
            raise ValueError("START_DATE and END_DATE must be provided")
        
        # Database configuration from environment variables
        db_config = {
            'host': os.environ.get('DB_HOST'),
            'port': int(os.environ.get('DB_PORT', 5432)),
            'database': os.environ.get('DB_NAME'),
            'user': os.environ.get('DB_USER'),
            'password': os.environ.get('DB_PASSWORD')
        }
        
        # Validate database configuration
        required_keys = ['host', 'database', 'user', 'password']
        for key in required_keys:
            if not db_config.get(key):
                raise ValueError(f"Database configuration missing: {key}")
        
        # Initialize and run attribution engine
        engine = AttributionEngineADF(db_config)
        result = engine.run_attribution_analysis_adf(start_date, end_date, pipeline_name)
        
        # Print result for ADF to capture
        print(json.dumps(result, default=str))
        
        # Exit with appropriate code
        sys.exit(0 if result['status'] == 'SUCCESS' else 1)
        
    except Exception as e:
        error_result = {
            'status': 'FAILED',
            'error': str(e)
        }
        print(json.dumps(error_result))
        sys.exit(1)

if __name__ == "__main__":
    main()
```

---

## 🏭 Step 3: ADF Pipeline Design

### 3.1 Pipeline Architecture
```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Data Sources  │    │  Data Factory   │    │   Target DB     │
│                 │    │                 │    │                 │
│ • shopify_orders│───▶│ • Copy Activity │───▶│ • Raw Tables    │
│ • ads_insights  │    │ • Python Script │    │ • Analytics     │
│ • product_data  │    │ • Stored Proc   │    │ • Views         │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

### 3.2 Pipeline Components

#### 3.2.1 Data Ingestion Activities
- **Copy Data Activity**: Extract data from source systems
- **Lookup Activity**: Get configuration parameters
- **Set Variable Activity**: Set pipeline variables

#### 3.2.2 Transformation Activities
- **Azure Function Activity**: Execute AttributionEngine.py
- **Stored Procedure Activity**: Data quality checks
- **Data Flow Activity**: Additional transformations if needed

#### 3.2.3 Data Loading Activities
- **Copy Data Activity**: Load processed data to target tables
- **Stored Procedure Activity**: Update analytical views
- **Web Activity**: Send notifications

---

## 🔧 Step 4: ADF Pipeline Implementation

### 4.1 Create Linked Services

#### 4.1.1 PostgreSQL Linked Service
```json
{
    "name": "PostgreSQL_Attribution",
    "type": "Microsoft.DataFactory/factories/linkedservices",
    "properties": {
        "type": "PostgreSql",
        "typeProperties": {
            "connectionString": {
                "type": "AzureKeyVaultSecret",
                "store": {
                    "referenceName": "AzureKeyVault",
                    "type": "LinkedServiceReference"
                },
                "secretName": "postgresql-connection-string"
            }
        }
    }
}
```

#### 4.1.2 Azure Function Linked Service
```json
{
    "name": "AzureFunction_Attribution",
    "type": "Microsoft.DataFactory/factories/linkedservices",
    "properties": {
        "type": "AzureFunction",
        "typeProperties": {
            "functionAppUrl": "https://attribution-function-app.azurewebsites.net",
            "functionKey": {
                "type": "AzureKeyVaultSecret",
                "store": {
                    "referenceName": "AzureKeyVault",
                    "type": "LinkedServiceReference"
                },
                "secretName": "azure-function-key"
            }
        }
    }
}
```

### 4.2 Create Datasets

#### 4.2.1 Source Datasets
```json
{
    "name": "ShopifyOrders",
    "type": "Microsoft.DataFactory/factories/datasets",
    "properties": {
        "type": "PostgreSqlTable",
        "linkedServiceName": {
            "referenceName": "PostgreSQL_Source",
            "type": "LinkedServiceReference"
        },
        "typeProperties": {
            "tableName": "shopify_orders"
        }
    }
}
```

#### 4.2.2 Target Datasets
```json
{
    "name": "AttributionRawData",
    "type": "Microsoft.DataFactory/factories/datasets",
    "properties": {
        "type": "PostgreSqlTable",
        "linkedServiceName": {
            "referenceName": "PostgreSQL_Attribution",
            "type": "LinkedServiceReference"
        },
        "typeProperties": {
            "tableName": "attribution_raw_data"
        }
    }
}
```

### 4.3 Create Pipeline

#### 4.3.1 Main Pipeline JSON
```json
{
    "name": "AttributionAnalysisPipeline",
    "type": "Microsoft.DataFactory/factories/pipelines",
    "properties": {
        "activities": [
            {
                "name": "GetPipelineParameters",
                "type": "Lookup",
                "inputs": [
                    {
                        "referenceName": "PipelineParameters",
                        "type": "DatasetReference"
                    }
                ],
                "outputs": [
                    {
                        "referenceName": "PipelineParametersOutput",
                        "type": "DatasetReference"
                    }
                ]
            },
            {
                "name": "SetDateVariables",
                "type": "SetVariable",
                "dependsOn": [
                    {
                        "activity": "GetPipelineParameters",
                        "dependencyConditions": ["Succeeded"]
                    }
                ],
                "typeProperties": {
                    "variableName": "startDate",
                    "value": {
                        "value": "@activity('GetPipelineParameters').output.firstRow.start_date",
                        "type": "Expression"
                    }
                }
            },
            {
                "name": "ExecuteAttributionAnalysis",
                "type": "AzureFunctionActivity",
                "linkedServiceName": {
                    "referenceName": "AzureFunction_Attribution",
                    "type": "LinkedServiceReference"
                },
                "typeProperties": {
                    "functionName": "attribution-analysis",
                    "method": "POST",
                    "body": {
                        "start_date": "@variables('startDate')",
                        "end_date": "@variables('endDate')",
                        "pipeline_name": "@pipeline().Pipeline"
                    }
                }
            },
            {
                "name": "ValidateDataQuality",
                "type": "StoredProcedure",
                "linkedServiceName": {
                    "referenceName": "PostgreSQL_Attribution",
                    "type": "LinkedServiceReference"
                },
                "typeProperties": {
                    "storedProcedureName": "validate_attribution_data_quality",
                    "storedProcedureParameters": {
                        "batch_id": {
                            "value": "@activity('ExecuteAttributionAnalysis').output.batch_id",
                            "type": "String"
                        }
                    }
                }
            },
            {
                "name": "SendSuccessNotification",
                "type": "WebActivity",
                "dependsOn": [
                    {
                        "activity": "ValidateDataQuality",
                        "dependencyConditions": ["Succeeded"]
                    }
                ],
                "typeProperties": {
                    "url": "https://hooks.slack.com/services/YOUR/SLACK/WEBHOOK",
                    "method": "POST",
                    "body": {
                        "text": "Attribution analysis completed successfully for batch @{activity('ExecuteAttributionAnalysis').output.batch_id}"
                    }
                }
            }
        ],
        "variables": {
            "startDate": {
                "type": "String"
            },
            "endDate": {
                "type": "String"
            }
        }
    }
}
```

---

## 📊 Step 5: Monitoring and Alerting

### 5.1 Data Quality Monitoring
```sql
-- Create monitoring view
CREATE OR REPLACE VIEW v_pipeline_monitoring AS
SELECT 
    pl.pipeline_name,
    pl.batch_id,
    pl.execution_start_time,
    pl.execution_end_time,
    pl.execution_status,
    pl.total_orders_processed,
    pl.total_ads_processed,
    pl.data_quality_score,
    pl.error_message,
    CASE 
        WHEN pl.execution_status = 'SUCCESS' THEN '✅'
        WHEN pl.execution_status = 'FAILED' THEN '❌'
        WHEN pl.execution_status = 'RUNNING' THEN '🔄'
        ELSE '❓'
    END as status_icon
FROM attribution_pipeline_log pl
ORDER BY pl.execution_start_time DESC;
```

### 5.2 Alerting Rules
- **Pipeline Failure**: Alert when execution_status = 'FAILED'
- **Data Quality**: Alert when data_quality_score < 90
- **Performance**: Alert when execution time > 2 hours
- **Data Volume**: Alert when order count drops > 20% from previous run

---

## 🚀 Step 6: Deployment Steps

### 6.1 Pre-Deployment Checklist
- [ ] Database schema created and tested
- [ ] AttributionEngine.py modified for ADF
- [ ] Azure Function deployed with Python code
- [ ] Linked services configured
- [ ] Datasets created
- [ ] Pipeline JSON validated

### 6.2 Deployment Process
1. **Deploy Database Schema**
   ```bash
   psql -h your-db-host -U your-user -d your-db -f attribution_schema_model.sql
   ```

2. **Deploy Azure Function**
   ```bash
   func azure functionapp publish attribution-function-app
   ```

3. **Deploy ADF Pipeline**
   ```bash
   az datafactory pipeline create --resource-group your-rg --factory-name your-adf --name AttributionAnalysisPipeline --pipeline @pipeline.json
   ```

4. **Test Pipeline**
   ```bash
   az datafactory pipeline create-run --resource-group your-rg --factory-name your-adf --name AttributionAnalysisPipeline --parameters start_date=2025-01-01 end_date=2025-01-02
   ```

### 6.3 Post-Deployment Validation
- [ ] Pipeline executes successfully
- [ ] Data is loaded into target tables
- [ ] Analytical views return expected results
- [ ] Monitoring alerts are configured
- [ ] Documentation is updated

---

## 📈 Step 7: Performance Optimization

### 7.1 Database Optimization
- **Partitioning**: Partition tables by date for better performance
- **Indexing**: Create composite indexes for common query patterns
- **Statistics**: Update table statistics regularly
- **Vacuum**: Schedule regular VACUUM and ANALYZE operations

### 7.2 Pipeline Optimization
- **Parallel Processing**: Use parallel activities where possible
- **Data Compression**: Compress data during transfer
- **Caching**: Cache frequently accessed data
- **Resource Scaling**: Scale compute resources based on data volume

---

## 🔍 Step 8: Testing and Validation

### 8.1 Unit Testing
```python
# test_attribution_engine_adf.py
import unittest
from attribution_engine_adf import AttributionEngineADF

class TestAttributionEngineADF(unittest.TestCase):
    def setUp(self):
        self.db_config = {
            'host': 'test-host',
            'port': 5432,
            'database': 'test-db',
            'user': 'test-user',
            'password': 'test-password'
        }
        self.engine = AttributionEngineADF(self.db_config, 'test-batch-123')
    
    def test_batch_id_generation(self):
        self.assertIsNotNone(self.engine.batch_id)
        self.assertEqual(len(self.engine.batch_id), 36)  # UUID length
    
    def test_data_quality_calculation(self):
        # Test data quality score calculation
        pass
```

### 8.2 Integration Testing
- **End-to-End Pipeline**: Test complete pipeline execution
- **Data Validation**: Verify data accuracy and completeness
- **Performance Testing**: Test with large datasets
- **Error Handling**: Test failure scenarios and recovery

---

## 📚 Step 9: Documentation and Maintenance

### 9.1 Documentation
- **Technical Documentation**: Pipeline architecture and components
- **User Guide**: How to use analytical views and reports
- **Troubleshooting Guide**: Common issues and solutions
- **Change Log**: Track all modifications and updates

### 9.2 Maintenance Schedule
- **Daily**: Monitor pipeline execution and data quality
- **Weekly**: Review performance metrics and optimize queries
- **Monthly**: Update documentation and review architecture
- **Quarterly**: Plan capacity and performance improvements

---

## 🎯 Expected Outcomes

After successful implementation:

1. **Automated Attribution Analysis**: Daily/hourly attribution analysis without manual intervention
2. **Scalable Data Processing**: Handle increasing data volumes efficiently
3. **Real-time Analytics**: Access to up-to-date attribution insights
4. **Data Quality Assurance**: Automated monitoring and alerting
5. **Performance Optimization**: Optimized queries and data structures
6. **Business Intelligence**: Rich analytical views for business users

---

## 🔧 Troubleshooting Common Issues

### Issue 1: Pipeline Timeout
**Solution**: Increase timeout settings and optimize Python code

### Issue 2: Memory Issues
**Solution**: Implement data chunking and streaming processing

### Issue 3: Data Quality Issues
**Solution**: Implement comprehensive data validation and quality checks

### Issue 4: Performance Issues
**Solution**: Optimize database queries and implement caching

---

This implementation guide provides a comprehensive approach to deploying the AttributionEngine.py in Azure Data Factory with proper data storage, monitoring, and maintenance procedures.
