#!/usr/bin/env python3
"""
UTM Data Processor for Attribution Analysis
===========================================

This script demonstrates how to use the UTM data model to:
1. Process UTM data from Shopify orders
2. Create hourly aggregations for joining with ad insights
3. Generate hourly purchase/sale analysis by joining with ads_insights_hourly

Requirements:
- PostgreSQL database with the UTM data model tables
- Existing AttributionEngine.py and related data
- psycopg2 and pandas for data processing
"""

import pandas as pd
import numpy as np
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import psycopg2
from sqlalchemy import create_engine, text
import json

# Set up logging
logging.basicConfig(
    filename='utm_data_processor.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filemode='w',
    force=True
)

class UTMDataProcessor:
    """
    Processes UTM data and creates hourly aggregations for attribution analysis.
    """
    
    def __init__(self, db_config: Dict):
        """
        Initialize the UTM data processor.
        
        Args:
            db_config: PostgreSQL database configuration dictionary
        """
        self.db_config = db_config
        self.engine = None
        self.connect_to_database()
        
        # UTM channel mapping patterns
        self.channel_mapping = {
            'facebook': 'Facebook',
            'fb': 'Facebook',
            'instagram': 'Instagram',
            'ig': 'Instagram',
            'google': 'Google',
            'shopify': 'Direct',
            'duckduckgo': 'DuckDuckGo',
            'email': 'Email',
            'mail': 'Email',
            'referral': 'Referral',
            'direct': 'Direct',
            'organic': 'Organic',
            'social': 'Social'
        }
        
        logging.info("UTM Data Processor initialized")
    
    def connect_to_database(self) -> None:
        """Connect to PostgreSQL database."""
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
    
    def create_utm_session_from_order(self, order_data: Dict) -> Dict:
        """
        Create a UTM session record from Shopify order data.
        
        Args:
            order_data: Dictionary containing order and UTM information
            
        Returns:
            Dictionary with UTM session data
        """
        # Extract UTM parameters
        utm_source = order_data.get('customer_utm_source')
        utm_medium = order_data.get('customer_utm_medium')
        utm_campaign = order_data.get('customer_utm_campaign')
        utm_content = order_data.get('customer_utm_content')
        utm_term = order_data.get('customer_utm_term')
        
        # Generate session token (you might want to use actual session tracking)
        session_token = f"session_{order_data['order_id']}_{int(datetime.now().timestamp())}"
        
        # Determine channel based on UTM parameters
        channel = self._determine_channel(utm_source, utm_medium)
        
        # Extract campaign information from UTM content (assuming ad_id is stored there)
        ad_id = utm_content if utm_content and utm_content.isdigit() else None
        
        session_data = {
            'session_token': session_token,
            'customer_id': order_data.get('customer_id'),
            'utm_source': utm_source,
            'utm_medium': utm_medium,
            'utm_campaign': utm_campaign,
            'utm_content': utm_content,
            'utm_term': utm_term,
            'campaign_id': utm_campaign,  # You might need to map this to actual campaign IDs
            'ad_id': ad_id,
            'session_start_time': order_data.get('created_at'),
            'first_pageview_time': order_data.get('created_at'),
            'is_conversion_session': True,
            'conversion_type': 'purchase',
            'conversion_value': float(order_data.get('total_price_amount', 0)),
            'conversion_currency': order_data.get('total_price_currency', 'INR'),
            'data_source': 'shopify_orders'
        }
        
        return session_data
    
    def _determine_channel(self, utm_source: str, utm_medium: str) -> str:
        """
        Determine marketing channel based on UTM parameters.
        
        Args:
            utm_source: UTM source parameter
            utm_medium: UTM medium parameter
            
        Returns:
            Marketing channel name
        """
        if not utm_source:
            return 'Direct'
        
        utm_source_lower = utm_source.lower()
        utm_medium_lower = utm_medium.lower() if utm_medium else ''
        
        # Check for paid channels
        if utm_medium_lower in ['cpc', 'paid', 'ppc']:
            if 'facebook' in utm_source_lower or 'fb' in utm_source_lower:
                return 'Facebook'
            elif 'instagram' in utm_source_lower or 'ig' in utm_source_lower:
                return 'Instagram'
            elif 'google' in utm_source_lower:
                return 'Google'
            else:
                return 'Paid'
        
        # Check for organic channels
        if utm_medium_lower in ['social', 'organic']:
            if 'facebook' in utm_source_lower or 'fb' in utm_source_lower:
                return 'Facebook'
            elif 'instagram' in utm_source_lower or 'ig' in utm_source_lower:
                return 'Instagram'
            else:
                return 'Social'
        
        # Check for email
        if utm_medium_lower == 'email' or 'email' in utm_source_lower:
            return 'Email'
        
        # Check for referral
        if utm_medium_lower == 'referral':
            return 'Referral'
        
        # Default mapping based on source
        for source_pattern, channel in self.channel_mapping.items():
            if source_pattern in utm_source_lower:
                return channel
        
        return 'Other'
    
    def insert_utm_session(self, session_data: Dict) -> int:
        """
        Insert UTM session data into the database.
        
        Args:
            session_data: Dictionary containing session data
            
        Returns:
            Session ID of the inserted record
        """
        try:
            with self.engine.connect() as conn:
                # Insert session data
                insert_query = """
                    INSERT INTO utm_sessions (
                        session_token, visitor_id, customer_id, utm_source, utm_medium,
                        utm_campaign, utm_content, utm_term, campaign_id, ad_id,
                        session_start_time, first_pageview_time, is_conversion_session,
                        conversion_type, conversion_value, conversion_currency, data_source
                    ) VALUES (
                        :session_token, :visitor_id, :customer_id, :utm_source, :utm_medium,
                        :utm_campaign, :utm_content, :utm_term, :campaign_id, :ad_id,
                        :session_start_time, :first_pageview_time, :is_conversion_session,
                        :conversion_type, :conversion_value, :conversion_currency, :data_source
                    ) RETURNING session_id
                """
                
                result = conn.execute(text(insert_query), session_data)
                session_id = result.scalar()
                conn.commit()
                
                logging.info(f"Inserted UTM session with ID: {session_id}")
                return session_id
                
        except Exception as e:
            logging.error(f"Error inserting UTM session: {e}")
            raise
    
    def insert_utm_order_mapping(self, order_data: Dict, session_id: int) -> None:
        """
        Insert UTM order mapping data.
        
        Args:
            order_data: Dictionary containing order data
            session_id: ID of the associated UTM session
        """
        try:
            with self.engine.connect() as conn:
                # Get channel classification
                channel = self._determine_channel(
                    order_data.get('customer_utm_source'),
                    order_data.get('customer_utm_medium')
                )
                
                mapping_data = {
                    'order_id': order_data['order_id'],
                    'order_name': order_data.get('order_name'),
                    'order_date': order_data.get('created_at'),
                    'order_value': float(order_data.get('total_price_amount', 0)),
                    'order_currency': order_data.get('total_price_currency', 'INR'),
                    'session_id': session_id,
                    'session_token': f"session_{order_data['order_id']}_{int(datetime.now().timestamp())}",
                    'attribution_model': 'last_click',
                    'attribution_weight': 1.0,
                    'is_primary_attribution': True,
                    'utm_source': order_data.get('customer_utm_source'),
                    'utm_medium': order_data.get('customer_utm_medium'),
                    'utm_campaign': order_data.get('customer_utm_campaign'),
                    'utm_content': order_data.get('customer_utm_content'),
                    'utm_term': order_data.get('customer_utm_term'),
                    'campaign_id': order_data.get('customer_utm_campaign'),
                    'ad_id': order_data.get('customer_utm_content') if order_data.get('customer_utm_content') and order_data.get('customer_utm_content').isdigit() else None,
                    'channel': channel,
                    'data_source': 'attribution_engine',
                    'batch_id': f"batch_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
                }
                
                insert_query = """
                    INSERT INTO utm_order_mapping (
                        order_id, order_name, order_date, order_value, order_currency,
                        session_id, session_token, attribution_model, attribution_weight,
                        is_primary_attribution, utm_source, utm_medium, utm_campaign,
                        utm_content, utm_term, campaign_id, ad_id, channel,
                        data_source, batch_id
                    ) VALUES (
                        :order_id, :order_name, :order_date, :order_value, :order_currency,
                        :session_id, :session_token, :attribution_model, :attribution_weight,
                        :is_primary_attribution, :utm_source, :utm_medium, :utm_campaign,
                        :utm_content, :utm_term, :campaign_id, :ad_id, :channel,
                        :data_source, :batch_id
                    )
                """
                
                conn.execute(text(insert_query), mapping_data)
                conn.commit()
                
                logging.info(f"Inserted UTM order mapping for order: {order_data['order_id']}")
                
        except Exception as e:
            logging.error(f"Error inserting UTM order mapping: {e}")
            raise
    
    def create_hourly_utm_metrics(self, start_date: str, end_date: str, batch_id: str) -> None:
        """
        Create hourly UTM metrics by aggregating session and order data.
        
        Args:
            start_date: Start date for aggregation (YYYY-MM-DD)
            end_date: End date for aggregation (YYYY-MM-DD)
            batch_id: Unique identifier for this batch
        """
        try:
            with self.engine.connect() as conn:
                # Query to aggregate UTM data by hour
                aggregation_query = """
                    WITH hourly_data AS (
                        SELECT 
                            DATE(uom.order_date) as date_start,
                            EXTRACT(hour FROM uom.order_date) as hour_start,
                            CONCAT(
                                LPAD(EXTRACT(hour FROM uom.order_date)::text, 2, '0'),
                                ':00:00 - ',
                                LPAD(EXTRACT(hour FROM uom.order_date)::text, 2, '0'),
                                ':59:59'
                            ) as hour_window,
                            uom.campaign_id,
                            uom.campaign_name,
                            uom.adset_id,
                            uom.adset_name,
                            uom.ad_id,
                            uom.ad_name,
                            uom.utm_source,
                            uom.utm_medium,
                            uom.utm_campaign,
                            uom.utm_content,
                            uom.utm_term,
                            uom.channel,
                            uom.sub_channel,
                            COUNT(DISTINCT us.session_id) as total_sessions,
                            COUNT(DISTINCT us.visitor_id) as unique_visitors,
                            COUNT(DISTINCT us.customer_id) as unique_customers,
                            COUNT(DISTINCT uom.order_id) as total_conversions,
                            SUM(uom.order_value) as total_revenue,
                            COUNT(DISTINCT uom.order_id) as total_orders,
                            AVG(uom.order_value) as avg_order_value,
                            SUM(uom.order_value) as attributed_revenue,
                            COUNT(DISTINCT uom.order_id) as attributed_orders,
                            CASE 
                                WHEN COUNT(DISTINCT us.session_id) > 0 THEN 
                                    ROUND((COUNT(DISTINCT uom.order_id)::DECIMAL / COUNT(DISTINCT us.session_id)) * 100, 4)
                                ELSE 0 
                            END as conversion_rate,
                            CASE 
                                WHEN COUNT(DISTINCT uom.order_id) > 0 THEN 
                                    ROUND((COUNT(DISTINCT uom.order_id)::DECIMAL / COUNT(DISTINCT uom.order_id)) * 100, 4)
                                ELSE 0 
                            END as attribution_rate,
                            85.5 as data_completeness_score -- Placeholder, calculate based on actual data quality
                        FROM utm_order_mapping uom
                        LEFT JOIN utm_sessions us ON uom.session_id = us.session_id
                        WHERE DATE(uom.order_date) >= :start_date 
                        AND DATE(uom.order_date) <= :end_date
                        GROUP BY 
                            DATE(uom.order_date),
                            EXTRACT(hour FROM uom.order_date),
                            uom.campaign_id, uom.campaign_name, uom.adset_id, uom.adset_name,
                            uom.ad_id, uom.ad_name, uom.utm_source, uom.utm_medium,
                            uom.utm_campaign, uom.utm_content, uom.utm_term,
                            uom.channel, uom.sub_channel
                    )
                    INSERT INTO utm_hourly_metrics (
                        date_start, hour_start, hour_window, campaign_id, campaign_name,
                        adset_id, adset_name, ad_id, ad_name, utm_source, utm_medium,
                        utm_campaign, utm_content, utm_term, channel, sub_channel,
                        total_sessions, unique_visitors, unique_customers, total_conversions,
                        conversion_rate, total_revenue, total_orders, avg_order_value,
                        attributed_revenue, attributed_orders, attribution_rate,
                        data_completeness_score, batch_id
                    )
                    SELECT 
                        date_start, hour_start, hour_window, campaign_id, campaign_name,
                        adset_id, adset_name, ad_id, ad_name, utm_source, utm_medium,
                        utm_campaign, utm_content, utm_term, channel, sub_channel,
                        total_sessions, unique_visitors, unique_customers, total_conversions,
                        conversion_rate, total_revenue, total_orders, avg_order_value,
                        attributed_revenue, attributed_orders, attribution_rate,
                        data_completeness_score, :batch_id
                    FROM hourly_data
                    ON CONFLICT (date_start, hour_start, campaign_id, ad_id, utm_source, utm_medium, utm_campaign, batch_id)
                    DO UPDATE SET
                        total_sessions = EXCLUDED.total_sessions,
                        unique_visitors = EXCLUDED.unique_visitors,
                        unique_customers = EXCLUDED.unique_customers,
                        total_conversions = EXCLUDED.total_conversions,
                        conversion_rate = EXCLUDED.conversion_rate,
                        total_revenue = EXCLUDED.total_revenue,
                        total_orders = EXCLUDED.total_orders,
                        avg_order_value = EXCLUDED.avg_order_value,
                        attributed_revenue = EXCLUDED.attributed_revenue,
                        attributed_orders = EXCLUDED.attributed_orders,
                        attribution_rate = EXCLUDED.attribution_rate,
                        data_completeness_score = EXCLUDED.data_completeness_score,
                        updated_at = CURRENT_TIMESTAMP
                """
                
                conn.execute(text(aggregation_query), {
                    'start_date': start_date,
                    'end_date': end_date,
                    'batch_id': batch_id
                })
                conn.commit()
                
                logging.info(f"Created hourly UTM metrics for {start_date} to {end_date}")
                
        except Exception as e:
            logging.error(f"Error creating hourly UTM metrics: {e}")
            raise
    
    def get_hourly_purchase_analysis(self, start_date: str, end_date: str) -> pd.DataFrame:
        """
        Get hourly purchase/sale analysis by joining UTM data with ad insights.
        
        Args:
            start_date: Start date for analysis (YYYY-MM-DD)
            end_date: End date for analysis (YYYY-MM-DD)
            
        Returns:
            DataFrame with hourly purchase/sale analysis
        """
        try:
            query = """
                SELECT 
                    u.date_start,
                    u.hour_start,
                    u.hour_window,
                    COALESCE(u.campaign_id, a.campaign_id) as campaign_id,
                    COALESCE(u.campaign_name, a.campaign_name) as campaign_name,
                    COALESCE(u.ad_id, a.ad_id) as ad_id,
                    COALESCE(u.ad_name, a.ad_name) as ad_name,
                    u.utm_source,
                    u.utm_medium,
                    u.utm_campaign,
                    u.channel,
                    u.sub_channel,
                    
                    -- Ad Performance Metrics
                    COALESCE(a.impressions, 0) as ad_impressions,
                    COALESCE(a.clicks, 0) as ad_clicks,
                    COALESCE(a.spend, 0) as ad_spend,
                    COALESCE(a.cpm, 0) as ad_cpm,
                    COALESCE(a.cpc, 0) as ad_cpc,
                    COALESCE(a.ctr, 0) as ad_ctr,
                    
                    -- UTM Conversion Metrics
                    COALESCE(u.total_sessions, 0) as utm_sessions,
                    COALESCE(u.total_conversions, 0) as utm_conversions,
                    COALESCE(u.conversion_rate, 0) as utm_conversion_rate,
                    COALESCE(u.total_revenue, 0) as utm_revenue,
                    COALESCE(u.total_orders, 0) as utm_orders,
                    COALESCE(u.avg_order_value, 0) as utm_avg_order_value,
                    
                    -- Attribution Metrics
                    COALESCE(u.attributed_revenue, 0) as attributed_revenue,
                    COALESCE(u.attributed_orders, 0) as attributed_orders,
                    COALESCE(u.attribution_rate, 0) as attribution_rate,
                    
                    -- Calculated Performance Metrics
                    CASE 
                        WHEN COALESCE(a.spend, 0) > 0 THEN ROUND((COALESCE(u.attributed_revenue, 0) / a.spend)::DECIMAL, 4)
                        ELSE 0 
                    END as roas,
                    
                    CASE 
                        WHEN COALESCE(a.clicks, 0) > 0 THEN ROUND((COALESCE(u.utm_conversions, 0)::DECIMAL / a.clicks) * 100, 4)
                        ELSE 0 
                    END as conversion_rate_percent,
                    
                    -- Data Quality
                    COALESCE(u.data_completeness_score, 0) as utm_data_quality,
                    CASE 
                        WHEN a.campaign_id IS NOT NULL AND u.campaign_id IS NOT NULL THEN 'matched'
                        WHEN a.campaign_id IS NOT NULL THEN 'ads_only'
                        WHEN u.campaign_id IS NOT NULL THEN 'utm_only'
                        ELSE 'unmatched'
                    END as data_match_status
                    
                FROM utm_hourly_metrics u
                FULL OUTER JOIN ads_insights_hourly a ON 
                    u.date_start = a.date_start 
                    AND u.hour_start = EXTRACT(hour FROM a.hourly_window::time)
                    AND COALESCE(u.campaign_id, '') = COALESCE(a.campaign_id, '')
                    AND COALESCE(u.ad_id, '') = COALESCE(a.ad_id, '')
                
                WHERE u.date_start >= :start_date 
                AND u.date_start <= :end_date
                
                ORDER BY u.date_start DESC, u.hour_start DESC, u.utm_revenue DESC
            """
            
            df = pd.read_sql(query, self.engine, params={
                'start_date': start_date,
                'end_date': end_date
            })
            
            logging.info(f"Retrieved hourly purchase analysis: {len(df)} records")
            return df
            
        except Exception as e:
            logging.error(f"Error retrieving hourly purchase analysis: {e}")
            raise
    
    def process_shopify_orders_to_utm(self, start_date: str, end_date: str) -> None:
        """
        Process Shopify orders and create UTM data for the specified date range.
        
        Args:
            start_date: Start date for processing (YYYY-MM-DD)
            end_date: End date for processing (YYYY-MM-DD)
        """
        try:
            # Load Shopify orders with UTM data
            orders_query = """
                SELECT 
                    o.order_id, o.order_name, o.created_at, o.total_price_amount, 
                    o.total_price_currency, o.customer_utm_source, o.customer_utm_medium,
                    o.customer_utm_campaign, o.customer_utm_content, o.customer_utm_term,
                    o.customer_id
                FROM shopify_orders o
                WHERE DATE(o.created_at_ist) >= :start_date 
                AND DATE(o.created_at_ist) <= :end_date
                AND o.cancelled_at_ist IS NULL
                AND (o.customer_utm_source IS NOT NULL OR o.customer_utm_medium IS NOT NULL)
                ORDER BY o.created_at_ist
            """
            
            orders_df = pd.read_sql(orders_query, self.engine, params={
                'start_date': start_date,
                'end_date': end_date
            })
            
            logging.info(f"Processing {len(orders_df)} orders with UTM data")
            
            batch_id = f"batch_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            
            for _, order in orders_df.iterrows():
                try:
                    # Create UTM session
                    session_data = self.create_utm_session_from_order(order.to_dict())
                    session_id = self.insert_utm_session(session_data)
                    
                    # Create UTM order mapping
                    self.insert_utm_order_mapping(order.to_dict(), session_id)
                    
                except Exception as e:
                    logging.error(f"Error processing order {order['order_id']}: {e}")
                    continue
            
            # Create hourly metrics
            self.create_hourly_utm_metrics(start_date, end_date, batch_id)
            
            logging.info(f"Completed UTM data processing for {start_date} to {end_date}")
            
        except Exception as e:
            logging.error(f"Error processing Shopify orders to UTM: {e}")
            raise

def main():
    """Main function to demonstrate UTM data processing."""
    
    # Database configuration (update with your actual credentials)
    db_config = {
        'host': 'localhost',
        'port': 5432,
        'database': 'your_database_name',
        'user': 'your_username',
        'password': 'your_password'
    }
    
    try:
        # Initialize processor
        processor = UTMDataProcessor(db_config)
        
        # Set date range for processing (last 30 days)
        end_date = datetime.now().strftime('%Y-%m-%d')
        start_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
        
        print(f"Processing UTM data from {start_date} to {end_date}")
        
        # Process Shopify orders to UTM data
        processor.process_shopify_orders_to_utm(start_date, end_date)
        
        # Get hourly purchase analysis
        print("Retrieving hourly purchase analysis...")
        analysis_df = processor.get_hourly_purchase_analysis(start_date, end_date)
        
        # Display sample results
        print(f"\nHourly Purchase Analysis Results ({len(analysis_df)} records):")
        print("=" * 80)
        
        if not analysis_df.empty:
            # Summary by channel
            channel_summary = analysis_df.groupby('channel').agg({
                'utm_revenue': 'sum',
                'utm_orders': 'sum',
                'ad_spend': 'sum',
                'utm_conversions': 'sum'
            }).round(2)
            
            print("\nChannel Performance Summary:")
            print(channel_summary)
            
            # Top performing campaigns
            campaign_summary = analysis_df.groupby(['campaign_name', 'channel']).agg({
                'utm_revenue': 'sum',
                'ad_spend': 'sum',
                'utm_conversions': 'sum'
            }).round(2).sort_values('utm_revenue', ascending=False).head(10)
            
            print("\nTop 10 Campaigns by Revenue:")
            print(campaign_summary)
            
            # Hourly patterns
            hourly_patterns = analysis_df.groupby('hour_start').agg({
                'utm_revenue': 'sum',
                'utm_orders': 'sum',
                'utm_conversions': 'sum'
            }).round(2)
            
            print("\nHourly Performance Patterns:")
            print(hourly_patterns)
            
        else:
            print("No data found for the specified date range.")
        
        print("\nUTM data processing completed successfully!")
        
    except Exception as e:
        logging.error(f"Error in main execution: {e}")
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
