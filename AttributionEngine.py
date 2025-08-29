import pandas as pd
import numpy as np
import json
import logging
from datetime import datetime, timedelta
import re
from typing import Dict, List, Optional, Tuple
import psycopg2
from sqlalchemy import create_engine, text

# Set up logging
logging.basicConfig(
    filename='attribution_analysis.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filemode='w',
    force=True
)

class AttributionEngine:
    def __init__(self, time_range_days: int = 30, db_config: Dict = None, 
                 start_date: str = None, end_date: str = None):
        """
        Initialize the attribution engine.
        
        Args:
            time_range_days: Number of days to look back for attribution analysis (used if start_date/end_date not provided)
            db_config: PostgreSQL database configuration dictionary
            start_date: Start date for analysis (YYYY-MM-DD format)
            end_date: End date for analysis (YYYY-MM-DD format)
        """
        self.time_range_days = time_range_days
        self.db_config = db_config or {}
        self.start_date = start_date
        self.end_date = end_date
        self.orders_df = None
        self.ads_df = None
        self.attribution_results = []
        self.engine = None
        
        # Channel mapping for UTM sources
        self.channel_mapping = {
            'google': 'Google',
            'facebook': 'Facebook',
            'fb': 'Facebook', 
            'instagram': 'Instagram',
            'ig': 'Instagram',
            'shopify': 'Direct',
            'duckduckgo': 'DuckDuckGo',
            '{{site_source_name}}': 'Unknown',
            'null': 'Direct/Unknown',
            '': 'Direct/Unknown'
        }
        
        logging.info(f"Attribution Engine initialized with {time_range_days} days lookback")
    
    def connect_to_database(self) -> None:
        """
        Connect to PostgreSQL database using the provided configuration.
        """
        try:
            # PostgreSQL connection
            connection_string = (
                f"postgresql://{self.db_config['user']}:{self.db_config['password']}@"
                f"{self.db_config['host']}:{self.db_config['port']}/{self.db_config['database']}"
            )
            self.engine = create_engine(connection_string, pool_pre_ping=True)
            
            logging.info("Connected to PostgreSQL database successfully")
            
        except Exception as e:
            logging.error(f"Error connecting to PostgreSQL database: {e}")
            raise
    
    def load_data_from_db(self) -> None:
        """
        Load data from PostgreSQL database tables.
        """
        try:
            if not self.engine:
                self.connect_to_database()
            
            # Determine date range for filtering
            if self.start_date and self.end_date:
                start_date_str = self.start_date
                end_date_str = self.end_date
                logging.info(f"Using date range: {start_date_str} to {end_date_str}")
            else:
                # Calculate cutoff date based on time_range_days
                cutoff_date = datetime.now() - timedelta(days=self.time_range_days)
                start_date_str = cutoff_date.strftime('%Y-%m-%d')
                end_date_str = datetime.now().strftime('%Y-%m-%d')
                logging.info(f"Using {self.time_range_days} days lookback: {start_date_str} to {end_date_str}")
            
            logging.info("Loading Shopify orders data from PostgreSQL...")
            
            # Load orders data with line items and product variants
            orders_query = f"""
                SELECT 
                    o.order_id, o.order_name, o.created_at, o.processed_at, o.updated_at,
                    o.closed_at, o.cancelled_at, o.total_price_amount, o.total_price_currency,
                    o.total_shipping_amount, o.total_shipping_currency, o.display_fulfillment_status,
                    o.ship_city, o.ship_province, o.ship_country, o.customer_referrer_url,
                    o.customer_utm_source, o.customer_utm_medium, o.customer_utm_campaign,
                    o.customer_utm_content, o.customer_utm_term, o.created_at_ist,
                    o.processed_at_ist, o.updated_at_ist, o.closed_at_ist, o.cancelled_at_ist,
                    o.customer_journey, o.custom_attributes,
                    -- Line item details
                    li.item_id, li.title as product_title, li.quantity, 
                    li.original_unit_price_amount, li.original_unit_price_currency,
                    li.discounted_unit_price_amount, li.discounted_unit_price_currency,
                    li.variant_id,
                    -- Product variant details
                    pv.sku, pv.variant_title, pv.unit_cost_amount, pv.unit_cost_currency,
                    pv.product_id, pv.product_title as variant_product_title, pv.vendor,
                    pv.selling_price
                FROM shopify_orders o
                LEFT JOIN shopify_order_line_items li ON o.order_id = li.order_id
                LEFT JOIN shopify_product_variants pv ON li.variant_id = pv.variant_id
                WHERE o.created_at_ist::timestamp >= '{start_date_str} 00:00:00+05:30'::timestamp 
                  AND o.created_at_ist::timestamp <= '{end_date_str} 23:59:59+05:30'::timestamp
                  AND o.cancelled_at_ist IS NULL
                ORDER BY o.created_at_ist::timestamp DESC
            """
            
            self.orders_df = pd.read_sql(orders_query, self.engine)
            logging.info(f"Loaded {len(self.orders_df)} orders from PostgreSQL")
            
            # Debug: Check if orders data is empty
            if len(self.orders_df) == 0:
                logging.warning("No orders data loaded! Check date range and database connection.")
            else:
                logging.info(f"Orders date range (IST): {self.orders_df['created_at_ist'].min()} to {self.orders_df['created_at_ist'].max()}")
                logging.info(f"Sample order columns: {list(self.orders_df.columns)}")
            
            logging.info("Loading ad insights data from PostgreSQL...")
            
            # Load ads data with optimized query
            ads_query = f"""
                SELECT 
                    campaign_id, campaign_name, adset_id, adset_name, ad_id, ad_name,
                    date_start, date_stop, hourly_window,
                    impressions, clicks, spend, cpm, cpc, ctr,
                    action_onsite_web_purchase, value_onsite_web_purchase,
                    action_onsite_web_add_to_cart, action_onsite_web_initiate_checkout,
                    action_onsite_web_view_content, action_link_click
                FROM ads_insights_hourly 
                WHERE date_start >= '{start_date_str}'::date AND date_start <= '{end_date_str}'::date
                ORDER BY date_start DESC
            """
            
            self.ads_df = pd.read_sql(ads_query, self.engine)
            logging.info(f"Loaded {len(self.ads_df)} ad records from PostgreSQL")
            
            # Debug: Check if ads data is empty
            if len(self.ads_df) == 0:
                logging.warning("No ads data loaded! Check date range and database connection.")
            else:
                logging.info(f"Ads date range: {self.ads_df['date_start'].min()} to {self.ads_df['date_start'].max()}")
                logging.info(f"Sample ads columns: {list(self.ads_df.columns)}")
            
            # Convert date columns
            self.orders_df['created_at'] = pd.to_datetime(self.orders_df['created_at'])
            self.ads_df['date_start'] = pd.to_datetime(self.ads_df['date_start'])
            
            logging.info(f"Data loaded successfully: {len(self.orders_df)} orders and {len(self.ads_df)} ad records")
            
        except Exception as e:
            logging.error(f"Error loading data from PostgreSQL: {e}")
            raise
    
    def load_data(self) -> None:
        """
        Load and prepare the data from PostgreSQL database.
        """
        # Load from database
        self.load_data_from_db()
    
    def parse_customer_journey(self, journey_str: str) -> Optional[Dict]:
        """
        Parse customer journey JSON string and extract attribution data.
        
        Args:
            journey_str: JSON string containing customer journey data
            
        Returns:
            Dictionary with attribution data or None if parsing fails
        """
        if (journey_str is None or 
            journey_str == '' or 
            str(journey_str).strip() == '' or 
            str(journey_str).strip() == 'null' or
            str(journey_str).strip() == 'None'):
            return None
            
        try:
            # Handle different JSON formats
            if isinstance(journey_str, str):
                journey_data = json.loads(journey_str)
            else:
                journey_data = journey_str
                
            if not journey_data or 'moments' not in journey_data:
                return None
                
            moments = journey_data['moments']
            if not moments:
                return None
                
            # Find the last valid moment with UTM parameters
            for moment in reversed(moments):
                if moment and 'utmParameters' in moment and moment['utmParameters']:
                    utm_params = moment['utmParameters']
                    
                    # Look for content ID first, then campaign, then medium
                    content_id = utm_params.get('content')
                    campaign_id = utm_params.get('campaign')
                    medium_id = utm_params.get('medium')
                    
                    # Return the first valid ID found
                    if content_id and content_id != 'null' and not content_id.startswith('{{'):
                        return {
                            'source': utm_params.get('source'),
                            'medium': utm_params.get('medium'),
                            'campaign': utm_params.get('campaign'),
                            'content': content_id,
                            'term': utm_params.get('term'),
                            'attribution_id': content_id,
                            'attribution_type': 'content'
                        }
                    elif campaign_id and campaign_id != 'null' and not campaign_id.startswith('{{'):
                        return {
                            'source': utm_params.get('source'),
                            'medium': utm_params.get('medium'),
                            'campaign': campaign_id,
                            'content': utm_params.get('content'),
                            'term': utm_params.get('term'),
                            'attribution_id': campaign_id,
                            'attribution_type': 'campaign'
                        }
                    elif medium_id and medium_id != 'null' and not medium_id.startswith('{{'):
                        return {
                            'source': utm_params.get('source'),
                            'medium': medium_id,
                            'campaign': utm_params.get('campaign'),
                            'content': utm_params.get('content'),
                            'term': utm_params.get('term'),
                            'attribution_id': medium_id,
                            'attribution_type': 'medium'
                        }
            
            return None
            
        except (json.JSONDecodeError, KeyError, TypeError) as e:
            logging.warning(f"Error parsing customer journey: {e}")
            return None
    
    def parse_custom_attributes(self, attributes_str: str) -> Optional[Dict]:
        """
        Parse custom attributes JSON string and extract UTM data.
        
        Args:
            attributes_str: JSON string containing custom attributes
            
        Returns:
            Dictionary with UTM data or None if parsing fails
        """
        if (attributes_str is None or 
            attributes_str == '' or 
            str(attributes_str).strip() == '' or 
            str(attributes_str).strip() == 'null' or
            str(attributes_str).strip() == 'None' or
            str(attributes_str).strip() == '[]'):
            return None
            
        try:
            if isinstance(attributes_str, str):
                attributes = json.loads(attributes_str)
            else:
                attributes = attributes_str
                
            if not attributes:
                return None
                
            # Extract UTM parameters from custom attributes
            utm_data = {}
            for attr in attributes:
                if isinstance(attr, dict) and 'key' in attr and 'value' in attr:
                    key = attr['key']
                    value = attr['value']
                    
                    if key.startswith('utm_'):
                        utm_data[key] = value
                    elif key == 'fbclid':
                        utm_data['fbclid'] = value
                    elif key == 'gclid':
                        utm_data['gclid'] = value
            
            if not utm_data:
                return None
                
            # Determine attribution ID (content > campaign > medium)
            attribution_id = None
            attribution_type = None
            
            if 'utm_content' in utm_data and utm_data['utm_content']:
                attribution_id = utm_data['utm_content']
                attribution_type = 'content'
            elif 'utm_campaign' in utm_data and utm_data['utm_campaign']:
                attribution_id = utm_data['utm_campaign']
                attribution_type = 'campaign'
            elif 'utm_medium' in utm_data and utm_data['utm_medium']:
                attribution_id = utm_data['utm_medium']
                attribution_type = 'medium'
            
            if attribution_id:
                return {
                    'source': utm_data.get('utm_source'),
                    'medium': utm_data.get('utm_medium'),
                    'campaign': utm_data.get('utm_campaign'),
                    'content': utm_data.get('utm_content'),
                    'term': utm_data.get('utm_term'),
                    'attribution_id': attribution_id,
                    'attribution_type': attribution_type
                }
            
            return None
            
        except (json.JSONDecodeError, KeyError, TypeError) as e:
            logging.warning(f"Error parsing custom attributes: {e}")
            return None
    
    def get_direct_utm_data(self, row: pd.Series) -> Optional[Dict]:
        """
        Extract UTM data from direct columns in the order data.
        
        Args:
            row: Order row from DataFrame
            
        Returns:
            Dictionary with UTM data or None
        """
        utm_data = {}
        
        # Check direct UTM columns
        utm_source = row.get('customer_utm_source')
        if utm_source is not None and utm_source != '' and str(utm_source).strip() != '':
            utm_data['source'] = utm_source
            
        utm_medium = row.get('customer_utm_medium')
        if utm_medium is not None and utm_medium != '' and str(utm_medium).strip() != '':
            utm_data['medium'] = utm_medium
            
        utm_campaign = row.get('customer_utm_campaign')
        if utm_campaign is not None and utm_campaign != '' and str(utm_campaign).strip() != '':
            utm_data['campaign'] = utm_campaign
            
        utm_content = row.get('customer_utm_content')
        if utm_content is not None and utm_content != '' and str(utm_content).strip() != '':
            utm_data['content'] = utm_content
            
        utm_term = row.get('customer_utm_term')
        if utm_term is not None and utm_term != '' and str(utm_term).strip() != '':
            utm_data['term'] = utm_term
        
        if not utm_data:
            return None
        
        # Determine attribution ID
        attribution_id = None
        attribution_type = None
        
        if 'content' in utm_data and utm_data['content']:
            attribution_id = utm_data['content']
            attribution_type = 'content'
        elif 'campaign' in utm_data and utm_data['campaign']:
            attribution_id = utm_data['campaign']
            attribution_type = 'campaign'
        elif 'medium' in utm_data and utm_data['medium']:
            attribution_id = utm_data['medium']
            attribution_type = 'medium'
        
        if attribution_id:
            return {
                'source': utm_data.get('source'),
                'medium': utm_data.get('medium'),
                'campaign': utm_data.get('campaign'),
                'content': utm_data.get('content'),
                'term': utm_data.get('term'),
                'attribution_id': attribution_id,
                'attribution_type': attribution_type
            }
        
        return None
    
    def rollup_ads_to_daily(self) -> pd.DataFrame:
        """
        Roll up hourly ad data to daily level.
        
        Returns:
            DataFrame with daily aggregated ad data
        """
        logging.info("Rolling up hourly ad data to daily level...")
        
        # Group by date and aggregate metrics
        daily_ads = self.ads_df.groupby([
            'date_start', 'campaign_id', 'campaign_name', 'adset_id', 'adset_name', 'ad_id', 'ad_name'
        ]).agg({
            'impressions': 'sum',
            'clicks': 'sum',
            'spend': 'sum',
            'action_onsite_web_purchase': 'sum',
            'value_onsite_web_purchase': 'sum',
            'action_onsite_web_add_to_cart': 'sum',
            'action_onsite_web_initiate_checkout': 'sum',
            'action_onsite_web_view_content': 'sum',
            'action_link_click': 'sum'
        }).reset_index()
        
        # Calculate daily metrics with zero division protection
        daily_ads['cpm'] = np.where(daily_ads['impressions'] > 0, 
                                   (daily_ads['spend'] / daily_ads['impressions'] * 1000), 0)
        daily_ads['cpc'] = np.where(daily_ads['clicks'] > 0, 
                                   (daily_ads['spend'] / daily_ads['clicks']), 0)
        daily_ads['ctr'] = np.where(daily_ads['impressions'] > 0, 
                                   (daily_ads['clicks'] / daily_ads['impressions'] * 100), 0)
        
        logging.info(f"Rolled up to {len(daily_ads)} daily ad records")
        return daily_ads
    
    def map_attribution_to_campaign(self, attribution_data: Dict, daily_ads: pd.DataFrame, channel: str = None) -> Optional[Dict]:
        """
        Map attribution data to campaign information.
        If no match found but channel is known, return "Unknown Campaign" data
        
        Args:
            attribution_data: Attribution data from order
            daily_ads: Daily aggregated ad data
            channel: Channel name (for creating "Unknown Campaign" entries)
            
        Returns:
            Dictionary with campaign mapping or None
        """
        if attribution_data is None or 'attribution_id' not in attribution_data:
            # If we have a channel but no attribution data, create "Unknown Campaign" entry
            if channel and channel in ['Facebook', 'Instagram', 'Google', 'TikTok', 'YouTube']:
                logging.debug(f"Creating 'Unknown Campaign' entry for {channel} with no attribution data")
                return {
                    'campaign_id': None,
                    'campaign_name': f"Unknown Campaign - {channel}",
                    'adset_id': None,
                    'adset_name': f"Unknown Adset - {channel}",
                    'ad_id': None,
                    'ad_name': f"Unknown Ad - {channel}",
                    'impressions': 0,
                    'clicks': 0,
                    'spend': 0,
                    'ad_purchases': 0,
                    'ad_revenue': 0
                }
            return None
        
        attribution_id = attribution_data['attribution_id']
        attribution_type = attribution_data.get('attribution_type', 'content')
        
        # Debug: Log matching attempt
        logging.debug(f"Trying to match attribution_id: {attribution_id} (type: {attribution_type})")
        
        # Try to match by different ID types based on attribution type
        matches = []
        
        # Convert attribution_id to string for comparison
        attribution_id_str = str(attribution_id)
        
        if attribution_type == 'content':
            # Match by ad_id (content) - try both string and integer matching
            ad_matches = daily_ads[
                (daily_ads['ad_id'].astype(str) == attribution_id_str) |
                (daily_ads['ad_id'] == attribution_id)
            ]
            if not ad_matches.empty:
                matches.extend(ad_matches.to_dict('records'))
                logging.debug(f"Found {len(ad_matches)} ad matches for content_id: {attribution_id}")
        
        elif attribution_type == 'campaign':
            # Match by campaign_id - try both string and integer matching
            campaign_matches = daily_ads[
                (daily_ads['campaign_id'].astype(str) == attribution_id_str) |
                (daily_ads['campaign_id'] == attribution_id)
            ]
            if not campaign_matches.empty:
                matches.extend(campaign_matches.to_dict('records'))
                logging.debug(f"Found {len(campaign_matches)} campaign matches for campaign_id: {attribution_id}")
        
        elif attribution_type == 'medium':
            # Match by adset_id (medium level) - try both string and integer matching
            adset_matches = daily_ads[
                (daily_ads['adset_id'].astype(str) == attribution_id_str) |
                (daily_ads['adset_id'] == attribution_id)
            ]
            if not adset_matches.empty:
                matches.extend(adset_matches.to_dict('records'))
                logging.debug(f"Found {len(adset_matches)} adset matches for adset_id: {attribution_id}")
        
        if not matches:
            # If no match found but we have a channel, create "Unknown Campaign" entry
            if channel and channel in ['Facebook', 'Instagram', 'Google', 'TikTok', 'YouTube']:
                logging.debug(f"No match found for attribution_id {attribution_id} ({attribution_type}), creating 'Unknown Campaign' for {channel}")
                return {
                    'campaign_id': None,
                    'campaign_name': f"Unknown Campaign - {channel}",
                    'adset_id': None,
                    'adset_name': f"Unknown Adset - {channel}",
                    'ad_id': None,
                    'ad_name': f"Unknown Ad - {channel}",
                    'impressions': 0,
                    'clicks': 0,
                    'spend': 0,
                    'ad_purchases': 0,
                    'ad_revenue': 0
                }
            else:
                logging.debug(f"No matches found for attribution_id: {attribution_id}")
                return None
        
        # Take the first match (most recent or highest spend)
        match = matches[0]
        
        return {
            'campaign_id': match['campaign_id'],
            'campaign_name': match['campaign_name'],
            'adset_id': match['adset_id'],
            'adset_name': match['adset_name'],
            'ad_id': match['ad_id'],
            'ad_name': match['ad_name'],
            'impressions': match['impressions'],
            'clicks': match['clicks'],
            'spend': match['spend'],
            'ad_purchases': match['action_onsite_web_purchase'],
            'ad_revenue': match['value_onsite_web_purchase']
        }
    
    def determine_channel(self, source: str) -> str:
        """
        Map UTM source to channel name.
        
        Args:
            source: UTM source value
            
        Returns:
            Channel name
        """
        if pd.isna(source) or source == '' or source == 'null':
            return 'Direct/Unknown'
        
        source_lower = str(source).lower().strip()
        return self.channel_mapping.get(source_lower, source)
    
    def process_attribution(self) -> pd.DataFrame:
        """
        Process attribution for all orders.
        
        Returns:
            DataFrame with attribution results
        """
        logging.info("Starting attribution processing...")
        
        # Roll up ads to daily level
        daily_ads = self.rollup_ads_to_daily()
        
        # Debug: Log available ad data
        logging.info(f"Available ad data: {len(daily_ads)} records")
        if not daily_ads.empty:
            logging.info(f"Ad ID range: {daily_ads['ad_id'].min()} to {daily_ads['ad_id'].max()}")
            logging.info(f"Campaign ID range: {daily_ads['campaign_id'].min()} to {daily_ads['campaign_id'].max()}")
            logging.info(f"Adset ID range: {daily_ads['adset_id'].min()} to {daily_ads['adset_id'].max()}")
            logging.info(f"Sample ad names: {daily_ads['ad_name'].head().tolist()}")
            logging.info(f"Sample campaign names: {daily_ads['campaign_name'].head().tolist()}")
        
        results = []
        
        # Group orders by order_id to handle multiple line items per order
        orders_grouped = self.orders_df.groupby('order_id')
        total_orders = len(orders_grouped)
        
        logging.info(f"Processing {total_orders} unique orders with line items...")
        
        for order_idx, (order_id, order_group) in enumerate(orders_grouped):
            try:
                # Get the first row for order-level data (all rows have same order data)
                order = order_group.iloc[0]
                order_date = order['created_at_ist']
                order_value = float(order['total_price_amount'])
                
                # Debug: Log first few orders to see data structure
                if order_idx < 3:
                    logging.info(f"Processing order {order_id}: {len(order_group)} line items, customer_journey type={type(order.get('customer_journey'))}, "
                               f"custom_attributes type={type(order.get('custom_attributes'))}")
                
                # Initialize attribution data
                attribution_data = None
                attribution_source = None
                campaign_data = None
                
                # 1. Try customer journey first (highest priority)
                customer_journey = order.get('customer_journey')
                if (customer_journey is not None and 
                    customer_journey != '' and 
                    str(customer_journey).strip() != '' and 
                    str(customer_journey).strip() != 'null' and
                    str(customer_journey).strip() != 'None'):
                    attribution_data = self.parse_customer_journey(customer_journey)
                    if attribution_data:
                        attribution_source = 'customer_journey'
                
                # 2. Try custom attributes if no customer journey
                if attribution_data is None:
                    custom_attributes = order.get('custom_attributes')
                    if (custom_attributes is not None and 
                        custom_attributes != '' and 
                        str(custom_attributes).strip() != '' and 
                        str(custom_attributes).strip() != 'null' and
                        str(custom_attributes).strip() != '[]' and
                        str(custom_attributes).strip() != 'None'):
                        attribution_data = self.parse_custom_attributes(custom_attributes)
                        if attribution_data:
                            attribution_source = 'custom_attributes'
                
                # 3. Try direct UTM columns as fallback
                if attribution_data is None:
                    attribution_data = self.get_direct_utm_data(order)
                    if attribution_data:
                        attribution_source = 'direct_utm'
                
                # Determine channel first (needed for "Unknown Campaign" logic)
                source = attribution_data.get('source') if attribution_data else None
                channel = self.determine_channel(source)
                
                # Map to campaign data if attribution found
                if attribution_data:
                    # Debug: Log attribution data for first few orders
                    if order_idx < 5:
                        logging.info(f"Order {order_id} attribution: {attribution_data}")
                    campaign_data = self.map_attribution_to_campaign(attribution_data, daily_ads, channel)
                    if campaign_data and order_idx < 5:
                        logging.info(f"Order {order_id} campaign match: {campaign_data.get('campaign_name', 'N/A')} - {campaign_data.get('ad_name', 'N/A')}")
                else:
                    # If no attribution data but we have a channel, create "Unknown Campaign" entry
                    if channel and channel in ['Facebook', 'Instagram', 'Google', 'TikTok', 'YouTube']:
                        campaign_data = self.map_attribution_to_campaign(None, daily_ads, channel)
                    else:
                        campaign_data = None
                
                # Calculate order-level financial metrics and collect SKU information
                total_cogs = 0
                total_net_profit = 0
                line_items_info = []
                skus_in_order = []
                sku_quantities = {}
                
                for _, line_item in order_group.iterrows():
                    # Calculate COGS for this line item
                    quantity = line_item.get('quantity', 0) or 0
                    unit_cost = line_item.get('unit_cost_amount', 0) or 0
                    line_item_cogs = quantity * unit_cost
                    total_cogs += line_item_cogs
                    
                    # Calculate net profit for this line item
                    unit_price = line_item.get('discounted_unit_price_amount', 0) or line_item.get('original_unit_price_amount', 0) or 0
                    line_item_revenue = quantity * unit_price
                    line_item_profit = line_item_revenue - line_item_cogs
                    total_net_profit += line_item_profit
                    
                    # Collect SKU information
                    sku = line_item.get('sku')
                    if sku:
                        skus_in_order.append(sku)
                        sku_quantities[sku] = sku_quantities.get(sku, 0) + quantity
                    
                    # Store line item info
                    line_items_info.append({
                        'sku': sku,
                        'product_title': line_item.get('product_title'),
                        'quantity': quantity,
                        'unit_price': unit_price,
                        'unit_cost': unit_cost,
                        'line_item_revenue': line_item_revenue,
                        'line_item_cogs': line_item_cogs,
                        'line_item_profit': line_item_profit
                    })
                
                # Create result record with order-level data and financial metrics
                result = {
                    # Order Information (essential for analysis)
                    'order_id': order_id,
                    'order_name': order['order_name'],
                    'order_date': order_date,
                    'order_value': order_value,
                    'order_currency': order.get('total_price_currency'),
                    'ship_city': order.get('ship_city'),
                    'ship_province': order.get('ship_province'),
                    'ship_country': order.get('ship_country'),
                    
                    # Financial Metrics
                    'total_cogs': total_cogs,
                    'total_net_profit': total_net_profit,
                    'profit_margin': (total_net_profit / order_value * 100) if order_value > 0 else 0,
                    'line_items_count': len(order_group),
                    
                    # SKU Information
                    'skus': ', '.join(set(skus_in_order)) if skus_in_order else None,
                    'unique_skus_count': len(set(skus_in_order)) if skus_in_order else 0,
                    'total_sku_quantity': sum(sku_quantities.values()) if sku_quantities else 0,
                    
                    # Attribution Information (derived/calculated)
                    'channel': channel,
                    'attribution_source': attribution_source,
                    'attribution_id': attribution_data.get('attribution_id') if attribution_data else None,
                    'attribution_type': attribution_data.get('attribution_type') if attribution_data else None,
                    
                    # UTM Parameters (parsed from JSON)
                    'utm_source': attribution_data.get('source') if attribution_data else None,
                    'utm_medium': attribution_data.get('medium') if attribution_data else None,
                    'utm_campaign': attribution_data.get('campaign') if attribution_data else None,
                    'utm_content': attribution_data.get('content') if attribution_data else None,
                    'utm_term': attribution_data.get('term') if attribution_data else None,
                    
                    # Campaign Data (essential for analysis)
                    'campaign_id': campaign_data.get('campaign_id') if campaign_data else None,
                    'campaign_name': campaign_data.get('campaign_name') if campaign_data else None,
                    'adset_id': campaign_data.get('adset_id') if campaign_data else None,
                    'adset_name': campaign_data.get('adset_name') if campaign_data else None,
                    'ad_id': campaign_data.get('ad_id') if campaign_data else None,
                    'ad_name': campaign_data.get('ad_name') if campaign_data else None,
                    
                    # Investigation flags (derived)
                    'has_customer_journey': pd.notna(order.get('customer_journey')),
                    'has_custom_attributes': pd.notna(order.get('custom_attributes'))
                }
                
                results.append(result)
                
                # Log progress
                if (order_idx + 1) % 100 == 0:
                    logging.info(f"Processed {order_idx + 1}/{total_orders} orders...")
                
            except Exception as e:
                logging.error(f"Error processing order {order_id}: {e}")
                continue
        
        logging.info(f"Completed attribution processing for {len(results)} orders")
        
        # Debug: Count successful matches
        if results:
            results_df_temp = pd.DataFrame(results)
            matched_orders = results_df_temp[results_df_temp['campaign_name'].notna()].shape[0]
            total_orders = len(results_df_temp)
            logging.info(f"Attribution matching summary: {matched_orders}/{total_orders} orders matched to campaigns ({matched_orders/total_orders*100:.1f}%)")
        
        # Debug: Check if we have any results
        if len(results) == 0:
            logging.warning("No attribution results generated! Check if orders data is loaded properly.")
            # Create empty DataFrame with expected columns
            results_df = pd.DataFrame(columns=[
                'order_id', 'order_name', 'order_date', 'order_value', 'order_currency',
                'ship_city', 'ship_province', 'ship_country', 'total_cogs', 'total_net_profit',
                'profit_margin', 'line_items_count', 'skus', 'unique_skus_count', 'total_sku_quantity',
                'channel', 'attribution_source', 'attribution_id', 'attribution_type', 'utm_source', 
                'utm_medium', 'utm_campaign', 'utm_content', 'utm_term', 'campaign_id', 'campaign_name', 
                'adset_id', 'adset_name', 'ad_id', 'ad_name', 'has_customer_journey', 'has_custom_attributes',
                'is_attributed', 'is_paid_channel'
            ])
        else:
            # Create DataFrame and add summary statistics
            results_df = pd.DataFrame(results)
        
        # Add summary columns
        results_df['is_attributed'] = results_df['attribution_source'].notna()
        results_df['is_paid_channel'] = results_df['channel'].isin(['Facebook', 'Instagram', 'Google'])
        
        # Convert timezone-aware datetimes to timezone-naive for Excel compatibility
        if 'order_date' in results_df.columns:
            results_df['order_date'] = pd.to_datetime(results_df['order_date']).dt.tz_localize(None)
        
        return results_df
    
    def create_investigation_data(self) -> pd.DataFrame:
        """
        Create investigation data with raw JSON samples for debugging.
        
        Returns:
            DataFrame with raw data samples
        """
        logging.info("Creating investigation data with raw JSON samples...")
        
        investigation_data = []
        
        # Sample orders for investigation (mix of attributed and unattributed)
        if len(self.orders_df) == 0:
            logging.info("No orders data available for investigation")
            return pd.DataFrame()
            
        sample_size = min(50, len(self.orders_df))  # Max 50 samples
        if sample_size == 0:
            return pd.DataFrame()
            
        step = max(1, len(self.orders_df) // sample_size) if sample_size > 0 else 1
        sample_indices = list(range(0, len(self.orders_df), step))[:sample_size]
        
        for idx in sample_indices:
            order = self.orders_df.iloc[idx]
            
            # Get raw JSON data
            customer_journey_raw = order.get('customer_journey', '')
            custom_attributes_raw = order.get('custom_attributes', '')
            
            # Try to parse and format JSON for readability
            try:
                if (customer_journey_raw is not None and 
                    customer_journey_raw != '' and 
                    str(customer_journey_raw).strip() != '' and
                    str(customer_journey_raw).strip() != 'null' and
                    str(customer_journey_raw).strip() != 'None'):
                    customer_journey_formatted = json.dumps(json.loads(customer_journey_raw), indent=2)
                else:
                    customer_journey_formatted = "No data"
            except:
                customer_journey_formatted = str(customer_journey_raw)[:500] + "..." if len(str(customer_journey_raw)) > 500 else str(customer_journey_raw)
            
            try:
                if (custom_attributes_raw is not None and 
                    custom_attributes_raw != '' and 
                    str(custom_attributes_raw).strip() != '' and
                    str(custom_attributes_raw).strip() != 'null' and
                    str(custom_attributes_raw).strip() != 'None' and
                    str(custom_attributes_raw).strip() != '[]'):
                    custom_attributes_formatted = json.dumps(json.loads(custom_attributes_raw), indent=2)
                else:
                    custom_attributes_formatted = "No data"
            except:
                custom_attributes_formatted = str(custom_attributes_raw)[:500] + "..." if len(str(custom_attributes_raw)) > 500 else str(custom_attributes_raw)
            
            investigation_record = {
                'order_id': order['order_id'],
                'order_name': order['order_name'],
                'order_date': pd.to_datetime(order['created_at']).tz_localize(None) if pd.notna(order['created_at']) else None,
                'order_value': order['total_price_amount'],
                'customer_journey_raw': customer_journey_formatted,
                'custom_attributes_raw': custom_attributes_formatted,
                'customer_utm_source': order.get('customer_utm_source'),
                'customer_utm_medium': order.get('customer_utm_medium'),
                'customer_utm_campaign': order.get('customer_utm_campaign'),
                'customer_utm_content': order.get('customer_utm_content'),
                'customer_utm_term': order.get('customer_utm_term'),
                'customer_referrer_url': order.get('customer_referrer_url'),
                'has_customer_journey': (customer_journey_raw is not None and 
                                       customer_journey_raw != '' and 
                                       str(customer_journey_raw).strip() != '' and
                                       str(customer_journey_raw).strip() != 'null' and
                                       str(customer_journey_raw).strip() != 'None'),
                'has_custom_attributes': (custom_attributes_raw is not None and 
                                        custom_attributes_raw != '' and 
                                        str(custom_attributes_raw).strip() != '' and
                                        str(custom_attributes_raw).strip() != 'null' and
                                        str(custom_attributes_raw).strip() != 'None' and
                                        str(custom_attributes_raw).strip() != '[]'),
                'journey_length': len(str(customer_journey_raw)),
                'attributes_length': len(str(custom_attributes_raw))
            }
            
            investigation_data.append(investigation_record)
        
        return pd.DataFrame(investigation_data)
    
    def generate_summary_report(self, results_df: pd.DataFrame) -> Dict:
        """
        Generate summary statistics for the attribution analysis.
        
        Args:
            results_df: DataFrame with attribution results
            
        Returns:
            Dictionary with summary statistics
        """
        logging.info("Generating summary report...")
        
        total_orders = len(results_df)
        total_value = results_df['order_value'].sum()
        
        # Attribution rates
        attributed_orders = results_df['is_attributed'].sum() if not results_df.empty else 0
        attribution_rate = (attributed_orders / total_orders * 100) if total_orders > 0 else 0
        
        # Channel breakdown
        if not results_df.empty:
            channel_breakdown = results_df.groupby('channel').agg({
                'order_id': 'count',
                'order_value': 'sum'
            }).rename(columns={'order_id': 'orders', 'order_value': 'revenue'})
            channel_breakdown['percentage'] = (channel_breakdown['orders'] / total_orders * 100)
        else:
            channel_breakdown = pd.DataFrame(columns=['orders', 'revenue', 'percentage'])
        
        # Attribution source breakdown
        if not results_df.empty:
            source_breakdown = results_df.groupby('attribution_source').agg({
                'order_id': 'count',
                'order_value': 'sum'
            }).rename(columns={'order_id': 'orders', 'order_value': 'revenue'})
            source_breakdown['percentage'] = (source_breakdown['orders'] / total_orders * 100)
        else:
            source_breakdown = pd.DataFrame(columns=['orders', 'revenue', 'percentage'])
        
        # Campaign performance (for attributed orders)
        attributed_df = results_df[results_df['is_attributed']] if not results_df.empty else pd.DataFrame()
        campaign_performance = None
        if not attributed_df.empty:
            # Get daily ads data for performance metrics
            daily_ads = self.rollup_ads_to_daily()
            
            # Group by campaign level for total sales and profit summary
            campaign_performance = attributed_df.groupby(['campaign_name', 'adset_name', 'ad_name', 'channel']).agg({
                'order_id': 'count',
                'order_value': 'sum',
                'total_cogs': 'sum',
                'total_net_profit': 'sum'
            }).rename(columns={'order_id': 'orders', 'order_value': 'total_sales'})
            
                        # Join with ads data to get ad performance metrics (impressions, clicks, spend only)
            if not daily_ads.empty:
                # Create a key for joining - AGGREGATE ads data first to avoid duplicates
                campaign_performance = campaign_performance.reset_index()
                
                # Debug: Log before aggregation
                logging.info(f"Before aggregation - daily_ads shape: {daily_ads.shape}")
                logging.info(f"Before aggregation - campaign_performance shape: {campaign_performance.shape}")
                
                # Aggregate ads data by campaign, adset, and ad names to avoid duplicates
                # NOTE: We only use ad performance metrics, NOT Facebook's purchase/revenue data
                daily_ads_aggregated = daily_ads.groupby(['campaign_name', 'adset_name', 'ad_name']).agg({
                    'impressions': 'sum',
                    'clicks': 'sum', 
                    'spend': 'sum'
                }).reset_index()
                
                # Debug: Log after aggregation
                logging.info(f"After aggregation - daily_ads_aggregated shape: {daily_ads_aggregated.shape}")
                
                # Join on campaign, adset, and ad names
                campaign_performance = campaign_performance.merge(
                    daily_ads_aggregated, 
                    on=['campaign_name', 'adset_name', 'ad_name'], 
                    how='left'
                )
                
                # Debug: Log after join
                logging.info(f"After join - campaign_performance shape: {campaign_performance.shape}")
                
                # Rename columns to match expected format
                campaign_performance = campaign_performance.rename(columns={
                    'impressions': 'ad_impressions',
                    'clicks': 'ad_clicks', 
                    'spend': 'ad_spend'
                })
                
                # Fill NaN values with 0 for performance metrics
                performance_cols = ['ad_impressions', 'ad_clicks', 'ad_spend']
                campaign_performance[performance_cols] = campaign_performance[performance_cols].fillna(0)
                
                # Add Shopify-based purchase and revenue data (from our attribution results)
                campaign_performance['shopify_purchases'] = campaign_performance['orders']  # Use actual order count
                campaign_performance['shopify_revenue'] = campaign_performance['total_sales']  # Use actual Shopify revenue
            else:
                # If no ads data, add empty performance columns
                campaign_performance = campaign_performance.reset_index()
                campaign_performance['ad_impressions'] = 0
                campaign_performance['ad_clicks'] = 0
                campaign_performance['ad_spend'] = 0
                campaign_performance['ad_purchases'] = 0
                campaign_performance['ad_revenue'] = 0
            
            # Calculate performance metrics with zero division protection
            # Use Shopify data for sales metrics, Facebook data only for ad performance
            campaign_performance['roas'] = np.where(campaign_performance['ad_spend'] > 0, 
                                                   (campaign_performance['shopify_revenue'] / campaign_performance['ad_spend']), 0)
            campaign_performance['profit_roas'] = np.where(campaign_performance['ad_spend'] > 0, 
                                                          (campaign_performance['total_net_profit'] / campaign_performance['ad_spend']), 0)
            campaign_performance['ctr'] = np.where(campaign_performance['ad_impressions'] > 0, 
                                                  (campaign_performance['ad_clicks'] / campaign_performance['ad_impressions'] * 100), 0)
            campaign_performance['conversion_rate'] = np.where(campaign_performance['ad_clicks'] > 0, 
                                                              (campaign_performance['shopify_purchases'] / campaign_performance['ad_clicks'] * 100), 0)
            campaign_performance['avg_order_value'] = np.where(campaign_performance['shopify_purchases'] > 0, 
                                                              (campaign_performance['shopify_revenue'] / campaign_performance['shopify_purchases']), 0)
            campaign_performance['avg_order_profit'] = np.where(campaign_performance['shopify_purchases'] > 0, 
                                                               (campaign_performance['total_net_profit'] / campaign_performance['shopify_purchases']), 0)
            campaign_performance['profit_margin'] = np.where(campaign_performance['shopify_revenue'] > 0, 
                                                            (campaign_performance['total_net_profit'] / campaign_performance['shopify_revenue'] * 100), 0)
            
            # Sort by total sales descending
            campaign_performance = campaign_performance.sort_values('total_sales', ascending=False)
        
        summary = {
            'total_orders': total_orders,
            'total_revenue': total_value,
            'attributed_orders': attributed_orders,
            'attribution_rate': attribution_rate,
            'channel_breakdown': channel_breakdown,
            'source_breakdown': source_breakdown,
            'campaign_performance': campaign_performance
        }
        
        logging.info(f"Summary: {total_orders} orders, {attribution_rate:.1f}% attribution rate")
        
        return summary
    
    def save_results(self, results_df: pd.DataFrame, summary: Dict, output_prefix: str = "attribution_results") -> None:
        """
        Save results to Excel file with multiple sheets.
        
        Args:
            results_df: DataFrame with attribution results
            summary: Summary statistics
            output_prefix: Prefix for output filename
        """
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        excel_file = f"{output_prefix}_{timestamp}.xlsx"
        
        # Create Excel writer
        try:
            with pd.ExcelWriter(excel_file, engine='openpyxl') as writer:
                
                # Sheet 1: Raw Attribution Data (All Orders)
                results_df.to_excel(writer, sheet_name='Raw_Attribution_Data', index=False)
                logging.info(f"Saved raw attribution data to sheet 'Raw_Attribution_Data'")
                
                # Sheet 2: Channel Breakdown
                summary['channel_breakdown'].to_excel(writer, sheet_name='Channel_Breakdown')
                logging.info(f"Saved channel breakdown to sheet 'Channel_Breakdown'")
                
                # Sheet 3: Attribution Source Breakdown
                summary['source_breakdown'].to_excel(writer, sheet_name='Attribution_Source_Breakdown')
                logging.info(f"Saved attribution source breakdown to sheet 'Attribution_Source_Breakdown'")
                
                                # Sheet 4: Campaign Performance (if available)
                if summary['campaign_performance'] is not None and not summary['campaign_performance'].empty:
                    # Campaign Performance includes ad-level details (campaign_name, adset_name, ad_name)
                    summary['campaign_performance'].to_excel(writer, sheet_name='Campaign_Performance')
                    logging.info(f"Saved campaign performance (with ad-level details) to sheet 'Campaign_Performance'")
                    
                    # Sheet 4b: Campaign Summary (aggregated by campaign only)
                    campaign_summary = summary['campaign_performance'].groupby(['campaign_name', 'channel']).agg({
                        'orders': 'sum',
                        'total_sales': 'sum',
                        'total_cogs': 'sum',
                        'total_net_profit': 'sum',
                        'ad_spend': 'sum',
                        'ad_impressions': 'sum',
                        'ad_clicks': 'sum',
                        'shopify_purchases': 'sum',
                        'shopify_revenue': 'sum'
                    }).reset_index()
                    campaign_summary['roas'] = np.where(campaign_summary['ad_spend'] > 0, 
                                                   (campaign_summary['shopify_revenue'] / campaign_summary['ad_spend']), 0)
                    campaign_summary['profit_roas'] = np.where(campaign_summary['ad_spend'] > 0, 
                                                          (campaign_summary['total_net_profit'] / campaign_summary['ad_spend']), 0)
                    campaign_summary['ctr'] = np.where(campaign_summary['ad_impressions'] > 0, 
                                                  (campaign_summary['ad_clicks'] / campaign_summary['ad_impressions'] * 100), 0)
                    campaign_summary['conversion_rate'] = np.where(campaign_summary['ad_clicks'] > 0, 
                                                              (campaign_summary['shopify_purchases'] / campaign_summary['ad_clicks'] * 100), 0)
                    campaign_summary['avg_order_value'] = np.where(campaign_summary['shopify_purchases'] > 0, 
                                                              (campaign_summary['shopify_revenue'] / campaign_summary['shopify_purchases']), 0)
                    campaign_summary['avg_order_profit'] = np.where(campaign_summary['shopify_purchases'] > 0, 
                                                               (campaign_summary['total_net_profit'] / campaign_summary['shopify_purchases']), 0)
                    campaign_summary['profit_margin'] = np.where(campaign_summary['shopify_revenue'] > 0, 
                                                            (campaign_summary['total_net_profit'] / campaign_summary['shopify_revenue'] * 100), 0)
                    campaign_summary = campaign_summary.sort_values('total_sales', ascending=False)
                    campaign_summary.to_excel(writer, sheet_name='Campaign_Summary', index=False)
                    logging.info(f"Saved campaign summary to sheet 'Campaign_Summary'")
                    
                    # Sheet 4c: Ad-Level Performance (detailed ad breakdown)
                    ad_performance = summary['campaign_performance'].groupby(['campaign_name', 'adset_name', 'ad_name', 'channel']).agg({
                        'orders': 'sum',
                        'total_sales': 'sum',
                        'total_cogs': 'sum',
                        'total_net_profit': 'sum',
                        'ad_spend': 'first',
                        'ad_impressions': 'first',
                        'ad_clicks': 'first',
                        'shopify_purchases': 'sum',
                        'shopify_revenue': 'sum'
                    }).reset_index()
                    ad_performance['roas'] = np.where(ad_performance['ad_spend'] > 0, 
                                                     (ad_performance['shopify_revenue'] / ad_performance['ad_spend']), 0)
                    ad_performance['profit_roas'] = np.where(ad_performance['ad_spend'] > 0, 
                                                            (ad_performance['total_net_profit'] / ad_performance['ad_spend']), 0)
                    ad_performance['ctr'] = np.where(ad_performance['ad_impressions'] > 0, 
                                                    (ad_performance['ad_clicks'] / ad_performance['ad_impressions'] * 100), 0)
                    ad_performance['conversion_rate'] = np.where(ad_performance['ad_clicks'] > 0, 
                                                                (ad_performance['shopify_purchases'] / ad_performance['ad_clicks'] * 100), 0)
                    ad_performance['avg_order_value'] = np.where(ad_performance['shopify_purchases'] > 0, 
                                                                (ad_performance['shopify_revenue'] / ad_performance['shopify_purchases']), 0)
                    ad_performance['avg_order_profit'] = np.where(ad_performance['shopify_purchases'] > 0, 
                                                                 (ad_performance['total_net_profit'] / ad_performance['shopify_purchases']), 0)
                    ad_performance['profit_margin'] = np.where(ad_performance['shopify_revenue'] > 0, 
                                                              (ad_performance['total_net_profit'] / ad_performance['shopify_revenue'] * 100), 0)
                    ad_performance = ad_performance.sort_values('total_sales', ascending=False)
                    ad_performance.to_excel(writer, sheet_name='Ad_Level_Performance', index=False)
                    logging.info(f"Saved ad-level performance to sheet 'Ad_Level_Performance'")
                    
                    # Sheet 4d: SKU-Level Performance (SKU breakdown by channel/campaign)
                    if not results_df.empty:
                        # Create SKU-level analysis by expanding the SKU lists
                        sku_data = []
                        for _, row in results_df.iterrows():
                            if row['skus'] and row['is_attributed']:
                                sku_list = [sku.strip() for sku in row['skus'].split(',')]
                                for sku in sku_list:
                                    sku_data.append({
                                        'sku': sku,
                                        'order_id': row['order_id'],
                                        'order_value': row['order_value'],
                                        'total_cogs': row['total_cogs'],
                                        'total_net_profit': row['total_net_profit'],
                                        'profit_margin': row['profit_margin'],
                                        'channel': row['channel'],
                                        'campaign_name': row['campaign_name'],
                                        'adset_name': row['adset_name'],
                                        'ad_name': row['ad_name'],
                                        'attribution_source': row['attribution_source']
                                    })
                        
                        if sku_data:
                            sku_df = pd.DataFrame(sku_data)
                            sku_performance = sku_df.groupby(['sku', 'channel', 'campaign_name']).agg({
                                'order_id': 'count',
                                'order_value': 'sum',
                                'total_cogs': 'sum',
                                'total_net_profit': 'sum'
                            }).reset_index()
                            sku_performance = sku_performance.rename(columns={'order_id': 'orders'})
                            sku_performance['profit_margin'] = np.where(sku_performance['order_value'] > 0, 
                                                                      (sku_performance['total_net_profit'] / sku_performance['order_value'] * 100), 0)
                            sku_performance = sku_performance.sort_values('order_value', ascending=False)
                            sku_performance.to_excel(writer, sheet_name='SKU_Performance', index=False)
                            logging.info(f"Saved SKU performance to sheet 'SKU_Performance'")
                        else:
                            logging.info("No SKU data available - skipping SKU_Performance sheet")
                    else:
                        logging.info("No orders data available - skipping SKU_Performance sheet")
                else:
                    logging.info("No campaign performance data available - skipping campaign sheets")
                
                # Sheet 5: Summary Statistics
                total_cogs = results_df['total_cogs'].sum() if not results_df.empty else 0
                total_net_profit = results_df['total_net_profit'].sum() if not results_df.empty else 0
                overall_profit_margin = (total_net_profit / summary['total_revenue'] * 100) if summary['total_revenue'] > 0 else 0
                
                # Calculate SKU statistics
                total_sku_quantity = results_df['total_sku_quantity'].sum() if not results_df.empty else 0
                unique_skus = set()
                if not results_df.empty:
                    for skus_str in results_df['skus'].dropna():
                        if skus_str:
                            sku_list = [sku.strip() for sku in skus_str.split(',')]
                            unique_skus.update(sku_list)
                total_unique_skus = len(unique_skus)
                
                summary_stats = {
                    'Metric': [
                        'Total Orders', 'Total Revenue', 'Total COGS', 'Total Net Profit', 
                        'Overall Profit Margin (%)', 'Total SKU Quantity', 'Total Unique SKUs',
                        'Attributed Orders', 'Attribution Rate (%)'
                    ],
                    'Value': [
                        summary['total_orders'],
                        summary['total_revenue'],
                        total_cogs,
                        total_net_profit,
                        overall_profit_margin,
                        total_sku_quantity,
                        total_unique_skus,
                        summary['attributed_orders'],
                        summary['attribution_rate']
                    ]
                }
                pd.DataFrame(summary_stats).to_excel(writer, sheet_name='Summary_Statistics', index=False)
                logging.info(f"Saved summary statistics to sheet 'Summary_Statistics'")
                
                # Sheet 6: Attributed Orders Only (for easier analysis)
                if not results_df.empty:
                    attributed_orders = results_df[results_df['is_attributed'] == True]
                    if not attributed_orders.empty:
                        attributed_orders.to_excel(writer, sheet_name='Attributed_Orders_Only', index=False)
                        logging.info(f"Saved attributed orders only to sheet 'Attributed_Orders_Only'")
                    else:
                        logging.info("No attributed orders available - skipping Attributed_Orders_Only sheet")
                else:
                    logging.info("No orders data available - skipping Attributed_Orders_Only sheet")
                
                # Sheet 7: Unattributed Orders (for investigation)
                if not results_df.empty:
                    unattributed_orders = results_df[results_df['is_attributed'] == False]
                    if not unattributed_orders.empty:
                        unattributed_orders.to_excel(writer, sheet_name='Unattributed_Orders', index=False)
                        logging.info(f"Saved unattributed orders to sheet 'Unattributed_Orders'")
                    else:
                        logging.info("No unattributed orders available - skipping Unattributed_Orders sheet")
                else:
                    logging.info("No orders data available - skipping Unattributed_Orders sheet")
                
                # Sheet 8: Raw Data Investigation (sample of orders with JSON data)
                investigation_data = self.create_investigation_data()
                if not investigation_data.empty:
                    investigation_data.to_excel(writer, sheet_name='Raw_Data_Investigation', index=False)
                    logging.info(f"Saved raw data investigation to sheet 'Raw_Data_Investigation'")
                else:
                    logging.info("No investigation data available - skipping Raw_Data_Investigation sheet")
            
            logging.info(f"Saved all results to Excel file: {excel_file}")
            
        except Exception as e:
            logging.error(f"Error saving Excel file: {e}")
            # Fallback to CSV only
            results_file = f"{output_prefix}_{timestamp}.csv"
            results_df.to_csv(results_file, index=False)
            logging.info(f"Fallback: Saved results to CSV file: {results_file}")
            raise
        
        # Also save individual CSV files for compatibility
        results_file = f"{output_prefix}_{timestamp}.csv"
        results_df.to_csv(results_file, index=False)
        logging.info(f"Also saved detailed results to CSV: {results_file}")
    
    def run_attribution_analysis(self, output_prefix: str = "attribution_results") -> Tuple[pd.DataFrame, Dict]:
        """
        Run complete attribution analysis.
        
        Args:
            output_prefix: Prefix for output filenames
            
        Returns:
            Tuple of (results DataFrame, summary dictionary)
        """
        logging.info("Starting complete attribution analysis...")
        
        # Load data
        self.load_data()
        
        # Process attribution
        results_df = self.process_attribution()
        
        # Generate summary
        summary = self.generate_summary_report(results_df)
        
        # Save results
        self.save_results(results_df, summary, output_prefix)
        
        logging.info("Attribution analysis completed successfully!")
        
        return results_df, summary


def main():
    """
    Main function to run the attribution analysis.
    """
    print("="*60)
    print("ATTRIBUTION ANALYSIS ENGINE")
    print("="*60)
    print("This script runs attribution analysis using PostgreSQL database tables:")
    print("- shopify_orders")
    print("- ads_insights_hourly")
    print()
    
    # Try to load configuration from config.py
    try:
        from config import POSTGRESQL_CONFIG, TIME_RANGE_DAYS, OUTPUT_PREFIX, USE_DATE_RANGE, START_DATE, END_DATE
        DB_CONFIG = POSTGRESQL_CONFIG
        print("✅ Loaded configuration from config.py")
        
        # Display date range configuration
        if USE_DATE_RANGE:
            print(f"📅 Using date range: {START_DATE} to {END_DATE}")
        else:
            print(f"📅 Using {TIME_RANGE_DAYS} days lookback")
            
    except ImportError:
        print("⚠️  config.py not found, using default configuration")
        print("💡 Create config.py with your PostgreSQL credentials for easier setup")
        
        # Default configuration
        TIME_RANGE_DAYS = 30
        OUTPUT_PREFIX = "attribution_analysis"
        USE_DATE_RANGE = False
        START_DATE = None
        END_DATE = None
        DB_CONFIG = {
            'host': 'selericdb.postgres.database.azure.com',
            'port': 5432,
            'database': 'postgres',
            'user': 'admin_seleric',
            'password': 'SelericDB246'
        }
    
    # Ask user for input
    print("Choose an option:")
    print("1. Run attribution analysis")
    print("2. Test database connection only")
    
    choice = input("\nEnter your choice (1 or 2): ").strip()
    
    try:
        if choice == "1":
            print("\n🔍 Testing database connection...")
            if test_database_connection(DB_CONFIG, START_DATE if USE_DATE_RANGE else None, END_DATE if USE_DATE_RANGE else None):
                print("\n🚀 Starting attribution analysis from PostgreSQL...")
                
                # Initialize attribution engine with database config and date range
                if USE_DATE_RANGE:
                    engine = AttributionEngine(
                        time_range_days=TIME_RANGE_DAYS, 
                        db_config=DB_CONFIG,
                        start_date=START_DATE,
                        end_date=END_DATE
                    )
                else:
                    engine = AttributionEngine(
                        time_range_days=TIME_RANGE_DAYS, 
                        db_config=DB_CONFIG
                    )
                
                # Run analysis from database
                results_df, summary = engine.run_attribution_analysis(
                    output_prefix=OUTPUT_PREFIX
                )
                
                # Print summary
                print("\n" + "="*50)
                print("ATTRIBUTION ANALYSIS SUMMARY")
                print("="*50)
                print(f"Total Orders: {summary['total_orders']:,}")
                print(f"Total Revenue: ₹{summary['total_revenue']:,.2f}")
                print(f"Attributed Orders: {summary['attributed_orders']:,}")
                print(f"Attribution Rate: {summary['attribution_rate']:.1f}%")
                
                print("\nChannel Breakdown:")
                print(summary['channel_breakdown'][['orders', 'revenue', 'percentage']].round(2))
                
                print("\nAttribution Source Breakdown:")
                print(summary['source_breakdown'][['orders', 'revenue', 'percentage']].round(2))
                
                print("\n" + "="*50)
                print("✅ Analysis complete! Check the generated CSV files for detailed results.")
                print("📄 Log file: attribution_analysis.log")
                
            else:
                print("❌ Database connection failed. Please check your configuration.")
        
        elif choice == "2":
            print("\n🔍 Testing database connection...")
            test_database_connection(DB_CONFIG, START_DATE if USE_DATE_RANGE else None, END_DATE if USE_DATE_RANGE else None)
        
        else:
            print("❌ Invalid choice. Please run the script again and choose 1 or 2.")
        
    except Exception as e:
        logging.error(f"Analysis failed: {e}")
        print(f"❌ Error: {e}")
        print("📄 Check attribution_analysis.log for details")
    
    print("\n" + "="*60)
    print("Thank you for using the Attribution Analysis Engine!")
    print("="*60)


def create_postgresql_config_example():
    """
    Example function showing PostgreSQL configuration.
    """
    # PostgreSQL configuration
    postgres_config = {
        'host': 'localhost',
        'port': 5432,
        'database': 'postgres',
        'user': 'admin_seleric',
        'password': 'SelericDB246'
    }
    
    return postgres_config


def test_database_connection(db_config: Dict, start_date: str = None, end_date: str = None) -> bool:
    """
    Test PostgreSQL connection and verify table structure.
    
    Args:
        db_config: PostgreSQL database configuration dictionary
        start_date: Start date for analysis (YYYY-MM-DD format)
        end_date: End date for analysis (YYYY-MM-DD format)
        
    Returns:
        True if connection successful, False otherwise
    """
    try:
        # Create PostgreSQL connection string
        connection_string = (
            f"postgresql://{db_config['user']}:{db_config['password']}@"
            f"{db_config['host']}:{db_config['port']}/{db_config['database']}"
        )
        
        engine = create_engine(connection_string, pool_pre_ping=True)
        
        # Test connection
        with engine.connect() as conn:
            # Check if tables exist
            tables_query = """
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name IN ('shopify_orders', 'ads_insights_hourly')
            """
            
            result = conn.execute(text(tables_query))
            tables = [row[0] for row in result]
            
            if 'shopify_orders' not in tables:
                print("❌ Table 'shopify_orders' not found in PostgreSQL database")
                return False
            
            if 'ads_insights_hourly' not in tables:
                print("❌ Table 'ads_insights_hourly' not found in PostgreSQL database")
                return False
            
            # Check table structure
            print("✅ PostgreSQL connection successful!")
            print(f"✅ Found tables: {', '.join(tables)}")
            
            # Get row counts
            orders_count = conn.execute(text("SELECT COUNT(*) FROM shopify_orders")).scalar()
            ads_count = conn.execute(text("SELECT COUNT(*) FROM ads_insights_hourly")).scalar()
            
            print(f"📊 shopify_orders: {orders_count:,} rows")
            print(f"📊 ads_insights_hourly: {ads_count:,} rows")
            
            # Get date ranges
            orders_date_range = conn.execute(text("""
                SELECT MIN(created_at_ist::timestamp), MAX(created_at_ist::timestamp) 
                FROM shopify_orders
            """)).fetchone()
            
            ads_date_range = conn.execute(text("""
                SELECT MIN(date_start), MAX(date_start) 
                FROM ads_insights_hourly
            """)).fetchone()
            
            print(f"📅 shopify_orders date range: {orders_date_range[0]} to {orders_date_range[1]}")
            print(f"📅 ads_insights_hourly date range: {ads_date_range[0]} to {ads_date_range[1]}")
            
            # If date range is specified, show filtered counts
            if start_date and end_date:
                filtered_orders_count = conn.execute(text(f"""
                    SELECT COUNT(*) FROM shopify_orders 
                    WHERE created_at_ist::timestamp >= '{start_date} 00:00:00+05:30'::timestamp 
                      AND created_at_ist::timestamp <= '{end_date} 23:59:59+05:30'::timestamp
                      AND cancelled_at_ist IS NULL
                """)).scalar()
                
                filtered_ads_count = conn.execute(text(f"""
                    SELECT COUNT(*) FROM ads_insights_hourly 
                    WHERE date_start >= '{start_date}'::date AND date_start <= '{end_date}'::date
                """)).scalar()
                
                print(f"📊 Filtered shopify_orders ({start_date} to {end_date}): {filtered_orders_count:,} rows")
                print(f"📊 Filtered ads_insights_hourly ({start_date} to {end_date}): {filtered_ads_count:,} rows")
            
            return True
            
    except Exception as e:
        print(f"❌ PostgreSQL connection failed: {e}")
        return False





if __name__ == "__main__":
    main()
