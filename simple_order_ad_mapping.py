"""
Simple Order-to-Ad Mapping
=========================

This script creates a direct mapping between orders and ads by:
1. Rolling up hourly ad data to daily level
2. Mapping order SKUs to UTM content (ad ID)
3. Creating a simple order-to-ad mapping table

This gives you a direct relationship: Order → SKU → Ad ID → Ad Performance
"""

import pandas as pd
import numpy as np
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from sqlalchemy import create_engine, text
import json

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class SimpleOrderAdMapping:
    """
    Simple order-to-ad mapping engine.
    
    Creates direct mapping between orders and ads by:
    1. Rolling up hourly ads to daily
    2. Mapping order SKUs to ad IDs via UTM content
    3. Creating order-ad performance mapping
    """
    
    def __init__(self, db_config: Dict):
        """Initialize the mapping engine."""
        self.db_config = db_config
        self.engine = None
        self.daily_ads_df = None
        self.orders_df = None
        
    def connect_database(self) -> None:
        """Connect to PostgreSQL database."""
        connection_string = (
            f"postgresql://{self.db_config['user']}:{self.db_config['password']}@"
            f"{self.db_config['host']}:{self.db_config['port']}/{self.db_config['database']}"
        )
        self.engine = create_engine(connection_string, pool_pre_ping=True)
        logger.info("Database connection established")
    
    def rollup_ads_to_daily(self, start_date: str, end_date: str) -> pd.DataFrame:
        """
        Roll up hourly ad data to daily level.
        
        Args:
            start_date: Start date for ad data (YYYY-MM-DD)
            end_date: End date for ad data (YYYY-MM-DD)
            
        Returns:
            DataFrame with daily aggregated ad data
        """
        logger.info(f"Rolling up hourly ads to daily from {start_date} to {end_date}")
        
        ads_query = f"""
            SELECT 
                campaign_id, campaign_name, adset_id, adset_name, ad_id, ad_name,
                date_start,
                SUM(impressions) as daily_impressions,
                SUM(clicks) as daily_clicks,
                SUM(spend) as daily_spend,
                SUM(action_onsite_web_purchase) as daily_purchases,
                SUM(value_onsite_web_purchase) as daily_revenue,
                SUM(action_onsite_web_add_to_cart) as daily_add_to_cart,
                SUM(action_onsite_web_initiate_checkout) as daily_checkout,
                SUM(action_onsite_web_view_content) as daily_content_views,
                SUM(action_link_click) as daily_link_clicks
            FROM ads_insights_hourly 
            WHERE date_start >= '{start_date}'::date AND date_start <= '{end_date}'::date
            GROUP BY campaign_id, campaign_name, adset_id, adset_name, ad_id, ad_name, date_start
            ORDER BY date_start DESC, daily_spend DESC
        """
        
        self.daily_ads_df = pd.read_sql(ads_query, self.engine)
        logger.info(f"Rolled up to {len(self.daily_ads_df)} daily ad records")
        
        # Calculate daily metrics
        self.daily_ads_df['daily_cpm'] = np.where(
            self.daily_ads_df['daily_impressions'] > 0,
            (self.daily_ads_df['daily_spend'] / self.daily_ads_df['daily_impressions'] * 1000),
            0
        )
        self.daily_ads_df['daily_cpc'] = np.where(
            self.daily_ads_df['daily_clicks'] > 0,
            (self.daily_ads_df['daily_spend'] / self.daily_ads_df['daily_clicks']),
            0
        )
        self.daily_ads_df['daily_ctr'] = np.where(
            self.daily_ads_df['daily_impressions'] > 0,
            (self.daily_ads_df['daily_clicks'] / self.daily_ads_df['daily_impressions'] * 100),
            0
        )
        
        return self.daily_ads_df
    
    def load_orders_with_skus(self, start_date: str, end_date: str) -> pd.DataFrame:
        """
        Load orders with SKU information and UTM data.
        
        Args:
            start_date: Start date for orders (YYYY-MM-DD)
            end_date: End date for orders (YYYY-MM-DD)
            
        Returns:
            DataFrame with orders and SKU information
        """
        logger.info(f"Loading orders with SKUs from {start_date} to {end_date}")
        
        orders_query = f"""
            SELECT 
                o.order_id, o.order_name, o.created_at_ist as order_date,
                o.total_price_amount as order_value, o.total_price_currency,
                o.customer_utm_source, o.customer_utm_medium, o.customer_utm_campaign,
                o.customer_utm_content, o.customer_utm_term,
                o.customer_journey, o.custom_attributes,
                -- Line item details
                li.item_id, li.title as product_title, li.quantity, 
                li.original_unit_price_amount, li.discounted_unit_price_amount,
                li.variant_id,
                -- Product variant details
                pv.sku, pv.variant_title, pv.unit_cost_amount,
                pv.product_id, pv.product_title as variant_product_title, pv.vendor
            FROM shopify_orders o
            LEFT JOIN shopify_order_line_items li ON o.order_id = li.order_id
            LEFT JOIN shopify_product_variants pv ON li.variant_id = pv.variant_id
            WHERE o.created_at_ist::timestamp >= '{start_date} 00:00:00+05:30'::timestamp 
              AND o.created_at_ist::timestamp <= '{end_date} 23:59:59+05:30'::timestamp
              AND o.cancelled_at_ist IS NULL
            ORDER BY o.created_at_ist::timestamp DESC
        """
        
        self.orders_df = pd.read_sql(orders_query, self.engine)
        logger.info(f"Loaded {len(self.orders_df)} order line items")
        
        return self.orders_df
    
    def extract_utm_content_from_journey(self, journey_str: str) -> Optional[str]:
        """
        Extract UTM content (ad ID) from customer journey JSON.
        
        Args:
            journey_str: Customer journey JSON string
            
        Returns:
            UTM content (ad ID) or None
        """
        if not journey_str or journey_str == '' or str(journey_str).strip() == '':
            return None
            
        try:
            journey_data = json.loads(journey_str)
            if not journey_data or 'moments' not in journey_data:
                return None
                
            moments = journey_data['moments']
            if not moments:
                return None
                
            # Find the last valid moment with UTM content
            for moment in reversed(moments):
                if moment and 'utmParameters' in moment and moment['utmParameters']:
                    utm_params = moment['utmParameters']
                    content = utm_params.get('content')
                    if content and content != 'null' and not content.startswith('{{'):
                        return content
            
            return None
            
        except (json.JSONDecodeError, KeyError, TypeError):
            return None
    
    def extract_utm_content_from_attributes(self, attributes_str: str) -> Optional[str]:
        """
        Extract UTM content (ad ID) from custom attributes JSON.
        
        Args:
            attributes_str: Custom attributes JSON string
            
        Returns:
            UTM content (ad ID) or None
        """
        if not attributes_str or attributes_str == '' or str(attributes_str).strip() == '':
            return None
            
        try:
            attributes = json.loads(attributes_str)
            if not attributes:
                return None
                
            # Extract UTM content from custom attributes
            for attr in attributes:
                if isinstance(attr, dict) and 'key' in attr and 'value' in attr:
                    if attr['key'] == 'utm_content':
                        return attr['value']
            
            return None
            
        except (json.JSONDecodeError, KeyError, TypeError):
            return None
    
    def get_ad_id_for_order(self, row: pd.Series) -> Optional[str]:
        """
        Get ad ID (UTM content) for an order using priority order.
        
        Priority:
        1. Customer journey
        2. Custom attributes
        3. Direct UTM content column
        
        Args:
            row: Order row from DataFrame
            
        Returns:
            Ad ID (UTM content) or None
        """
        # 1. Try customer journey first
        customer_journey = row.get('customer_journey')
        if customer_journey:
            ad_id = self.extract_utm_content_from_journey(customer_journey)
            if ad_id:
                return ad_id
        
        # 2. Try custom attributes
        custom_attributes = row.get('custom_attributes')
        if custom_attributes:
            ad_id = self.extract_utm_content_from_attributes(custom_attributes)
            if ad_id:
                return ad_id
        
        # 3. Try direct UTM content column
        utm_content = row.get('customer_utm_content')
        if utm_content and utm_content != '' and str(utm_content).strip() != '':
            return str(utm_content).strip()
        
        return None
    
    def create_order_ad_mapping(self) -> pd.DataFrame:
        """
        Create order-to-ad mapping by matching SKUs to ad IDs.
        
        Returns:
            DataFrame with order-ad mapping
        """
        logger.info("Creating order-to-ad mapping")
        
        if self.orders_df is None or self.daily_ads_df is None:
            raise ValueError("Orders and ads data must be loaded first")
        
        mapping_results = []
        
        # Group orders by order_id to handle multiple line items
        orders_grouped = self.orders_df.groupby('order_id')
        total_orders = len(orders_grouped)
        
        logger.info(f"Processing {total_orders} orders for ad mapping...")
        
        for order_idx, (order_id, order_group) in enumerate(orders_grouped):
            try:
                # Get the first row for order-level data
                order = order_group.iloc[0]
                order_date = pd.to_datetime(order['order_date']).date()
                
                # Get ad ID for this order
                ad_id = self.get_ad_id_for_order(order)
                
                if not ad_id:
                    logger.debug(f"No ad ID found for order {order_id}")
                    continue
                
                # Find matching ad data for this date and ad ID
                matching_ads = self.daily_ads_df[
                    (self.daily_ads_df['date_start'] == order_date) &
                    (self.daily_ads_df['ad_id'].astype(str) == str(ad_id))
                ]
                
                if matching_ads.empty:
                    logger.debug(f"No matching ad found for order {order_id}, ad_id {ad_id}, date {order_date}")
                    continue
                
                # Take the first matching ad (should be unique)
                ad_data = matching_ads.iloc[0]
                
                # Process each line item (SKU) in the order
                for _, line_item in order_group.iterrows():
                    sku = line_item.get('sku')
                    if not sku:
                        continue
                    
                    # Calculate line item metrics
                    quantity = line_item.get('quantity', 0) or 0
                    unit_price = line_item.get('discounted_unit_price_amount', 0) or line_item.get('original_unit_price_amount', 0) or 0
                    unit_cost = line_item.get('unit_cost_amount', 0) or 0
                    
                    line_item_revenue = quantity * unit_price
                    line_item_cogs = quantity * unit_cost
                    line_item_profit = line_item_revenue - line_item_cogs
                    
                    # Create mapping record
                    mapping_record = {
                        # Order information
                        'order_id': order_id,
                        'order_name': order['order_name'],
                        'order_date': order_date,
                        'order_value': order['order_value'],
                        'order_currency': order['total_price_currency'],
                        
                        # SKU information
                        'sku': sku,
                        'product_title': line_item.get('product_title'),
                        'variant_title': line_item.get('variant_title'),
                        'quantity': quantity,
                        'unit_price': unit_price,
                        'unit_cost': unit_cost,
                        'line_item_revenue': line_item_revenue,
                        'line_item_cogs': line_item_cogs,
                        'line_item_profit': line_item_profit,
                        'profit_margin': (line_item_profit / line_item_revenue * 100) if line_item_revenue > 0 else 0,
                        
                        # Ad information
                        'ad_id': ad_id,
                        'campaign_id': ad_data['campaign_id'],
                        'campaign_name': ad_data['campaign_name'],
                        'adset_id': ad_data['adset_id'],
                        'adset_name': ad_data['adset_name'],
                        'ad_name': ad_data['ad_name'],
                        
                        # Ad performance metrics (daily)
                        'ad_impressions': ad_data['daily_impressions'],
                        'ad_clicks': ad_data['daily_clicks'],
                        'ad_spend': ad_data['daily_spend'],
                        'ad_purchases': ad_data['daily_purchases'],
                        'ad_revenue': ad_data['daily_revenue'],
                        'ad_cpm': ad_data['daily_cpm'],
                        'ad_cpc': ad_data['daily_cpc'],
                        'ad_ctr': ad_data['daily_ctr'],
                        
                        # Calculated metrics
                        'roas': (line_item_revenue / ad_data['daily_spend']) if ad_data['daily_spend'] > 0 else 0,
                        'cost_per_order': (ad_data['daily_spend'] / ad_data['daily_purchases']) if ad_data['daily_purchases'] > 0 else 0,
                        
                        # UTM information
                        'utm_source': order.get('customer_utm_source'),
                        'utm_medium': order.get('customer_utm_medium'),
                        'utm_campaign': order.get('customer_utm_campaign'),
                        'utm_content': order.get('customer_utm_content'),
                        'utm_term': order.get('customer_utm_term'),
                    }
                    
                    mapping_results.append(mapping_record)
                
                # Log progress
                if (order_idx + 1) % 100 == 0:
                    logger.info(f"Processed {order_idx + 1}/{total_orders} orders...")
                
            except Exception as e:
                logger.error(f"Error processing order {order_id}: {e}")
                continue
        
        result_df = pd.DataFrame(mapping_results)
        logger.info(f"Created {len(result_df)} order-ad mappings")
        
        return result_df
    
    def create_mapping_table(self, mapping_df: pd.DataFrame) -> None:
        """
        Create order-ad mapping table in database.
        
        Args:
            mapping_df: DataFrame with order-ad mappings
        """
        logger.info("Creating order-ad mapping table in database")
        
        # Create table
        create_table_sql = """
            CREATE TABLE IF NOT EXISTS order_ad_mapping (
                id SERIAL PRIMARY KEY,
                order_id VARCHAR(50) NOT NULL,
                order_name VARCHAR(100),
                order_date DATE NOT NULL,
                order_value DECIMAL(10,2),
                order_currency VARCHAR(3),
                
                -- SKU information
                sku VARCHAR(100) NOT NULL,
                product_title VARCHAR(500),
                variant_title VARCHAR(500),
                quantity INTEGER,
                unit_price DECIMAL(10,2),
                unit_cost DECIMAL(10,2),
                line_item_revenue DECIMAL(10,2),
                line_item_cogs DECIMAL(10,2),
                line_item_profit DECIMAL(10,2),
                profit_margin DECIMAL(5,2),
                
                -- Ad information
                ad_id VARCHAR(50) NOT NULL,
                campaign_id VARCHAR(50),
                campaign_name VARCHAR(200),
                adset_id VARCHAR(50),
                adset_name VARCHAR(200),
                ad_name VARCHAR(200),
                
                -- Ad performance metrics
                ad_impressions BIGINT,
                ad_clicks BIGINT,
                ad_spend DECIMAL(10,2),
                ad_purchases BIGINT,
                ad_revenue DECIMAL(10,2),
                ad_cpm DECIMAL(10,2),
                ad_cpc DECIMAL(10,2),
                ad_ctr DECIMAL(5,2),
                
                -- Calculated metrics
                roas DECIMAL(10,2),
                cost_per_order DECIMAL(10,2),
                
                -- UTM information
                utm_source VARCHAR(100),
                utm_medium VARCHAR(100),
                utm_campaign VARCHAR(100),
                utm_content VARCHAR(100),
                utm_term VARCHAR(100),
                
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """
        
        with self.engine.connect() as conn:
            conn.execute(text(create_table_sql))
            conn.commit()
        
        # Insert data
        mapping_df.to_sql(
            'order_ad_mapping',
            self.engine,
            if_exists='append',
            index=False,
            method='multi',
            chunksize=1000
        )
        
        logger.info("Order-ad mapping table created and populated")
    
    def create_analysis_views(self) -> None:
        """
        Create analysis views for the order-ad mapping.
        """
        views_sql = [
            # SKU performance by ad
            """
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
                ROUND(SUM(line_item_revenue) / SUM(quantity), 2) as revenue_per_unit
            FROM order_ad_mapping
            GROUP BY sku, product_title, ad_id, ad_name, campaign_name
            ORDER BY total_revenue DESC;
            """,
            
            # Ad performance by SKU
            """
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
                ROUND(SUM(ad_spend) / COUNT(*), 2) as cost_per_order
            FROM order_ad_mapping
            GROUP BY ad_id, ad_name, campaign_name
            ORDER BY total_revenue DESC;
            """,
            
            # Daily performance summary
            """
            CREATE OR REPLACE VIEW v_daily_order_ad_performance AS
            SELECT 
                order_date,
                COUNT(DISTINCT order_id) as total_orders,
                COUNT(DISTINCT sku) as unique_skus,
                COUNT(DISTINCT ad_id) as unique_ads,
                SUM(line_item_revenue) as total_revenue,
                SUM(ad_spend) as total_spend,
                ROUND(SUM(line_item_revenue) / SUM(ad_spend), 2) as daily_roas,
                ROUND(SUM(ad_spend) / COUNT(DISTINCT order_id), 2) as cost_per_order
            FROM order_ad_mapping
            GROUP BY order_date
            ORDER BY order_date DESC;
            """,
            
            # Campaign performance with SKU breakdown
            """
            CREATE OR REPLACE VIEW v_campaign_sku_breakdown AS
            SELECT 
                campaign_name,
                COUNT(DISTINCT sku) as unique_skus,
                COUNT(*) as total_orders,
                SUM(line_item_revenue) as total_revenue,
                SUM(ad_spend) as total_spend,
                ROUND(SUM(line_item_revenue) / SUM(ad_spend), 2) as campaign_roas,
                ROUND(SUM(line_item_revenue) / COUNT(DISTINCT sku), 2) as revenue_per_sku
            FROM order_ad_mapping
            GROUP BY campaign_name
            ORDER BY total_revenue DESC;
            """
        ]
        
        with self.engine.connect() as conn:
            for view_sql in views_sql:
                conn.execute(text(view_sql))
                conn.commit()
        
        logger.info("Analysis views created successfully")
    
    def run_simple_mapping(self, start_date: str, end_date: str) -> pd.DataFrame:
        """
        Run the complete simple order-ad mapping process.
        
        Args:
            start_date: Start date for processing (YYYY-MM-DD)
            end_date: End date for processing (YYYY-MM-DD)
            
        Returns:
            DataFrame with order-ad mappings
        """
        try:
            logger.info(f"Starting simple order-ad mapping for {start_date} to {end_date}")
            
            # Connect to database
            self.connect_database()
            
            # Roll up ads to daily
            self.rollup_ads_to_daily(start_date, end_date)
            
            # Load orders with SKUs
            self.load_orders_with_skus(start_date, end_date)
            
            # Create order-ad mapping
            mapping_df = self.create_order_ad_mapping()
            
            # Create database table and views
            self.create_mapping_table(mapping_df)
            self.create_analysis_views()
            
            logger.info("Simple order-ad mapping completed successfully")
            
            return mapping_df
            
        except Exception as e:
            logger.error(f"Simple order-ad mapping failed: {e}")
            raise


def main():
    """Example usage of the Simple Order-Ad Mapping."""
    from config import POSTGRESQL_CONFIG
    
    # Initialize mapping engine
    mapper = SimpleOrderAdMapping(POSTGRESQL_CONFIG)
    
    # Run mapping for a specific date range
    start_date = "2025-01-01"
    end_date = "2025-01-31"
    
    try:
        # Run simple mapping
        mapping_df = mapper.run_simple_mapping(start_date, end_date)
        
        print("Simple Order-Ad Mapping completed successfully!")
        print(f"Total mappings created: {len(mapping_df)}")
        print(f"Unique orders: {mapping_df['order_id'].nunique()}")
        print(f"Unique SKUs: {mapping_df['sku'].nunique()}")
        print(f"Unique ads: {mapping_df['ad_id'].nunique()}")
        
        # Show sample results
        print("\nSample mappings:")
        print(mapping_df[['order_id', 'sku', 'ad_id', 'ad_name', 'line_item_revenue', 'roas']].head(10))
        
    except Exception as e:
        print(f"Simple Order-Ad Mapping failed: {e}")


if __name__ == "__main__":
    main()
