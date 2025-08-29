import pandas as pd
import logging
from datetime import datetime
import os

# Determine the directory where the script is located
script_dir = os.path.dirname(os.path.abspath(__file__))
logging.info(f"Script directory: {script_dir}")

# Set up logging to append to product_search.log in the script's directory
logging.basicConfig(
    filename=os.path.join(script_dir, 'product_search.log'),
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filemode='a',  # Append mode
    force=True
)
logging.info("Logging setup complete. Appending to product_search.log.")
logging.info(f"Script execution started at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}.")

try:
    # Define file paths relative to the script's directory
    book_csv_path = os.path.join(script_dir, "Book.csv")
    ad_data_csv_path = os.path.join(script_dir, "ad_data.csv")

    # Load Book.csv to get Price and Margin
    logging.info("Loading Book.csv...")
    book_df = pd.read_csv(book_csv_path)
    logging.info(f"Loaded Book.csv successfully. Found {len(book_df)} rows.")

    # Load the ad data
    logging.info("Loading ad_data.csv...")
    ad_df = pd.read_csv(ad_data_csv_path)
    logging.info(f"Loaded ad_data.csv successfully. Found {len(ad_df)} rows.")

    # Group by Product and calculate Total Ad Spent, Total Sales (Purchases), Total Revenue, and ROAS
    logging.info("Calculating product-wise metrics...")
    product_stats = ad_df.groupby('Product').agg({
        'Amount spent (INR)': 'sum',  # Total Ad Spent
        'Purchases': 'sum',           # Total Sales (Purchases)
        'Purchase Value': 'sum'       # Total Revenue
    }).reset_index()

    # Rename columns for clarity
    product_stats = product_stats.rename(columns={
        'Amount spent (INR)': 'Total Ad Spent (INR)',
        'Purchases': 'Total Sales (Purchases)',
        'Purchase Value': 'Total Revenue (INR)'
    })

    # Calculate ROAS (Total Revenue / Total Ad Spent)
    product_stats['ROAS'] = product_stats['Total Revenue (INR)'] / product_stats['Total Ad Spent (INR)']
    # Handle division by zero or NaN by replacing with 0
    product_stats['ROAS'] = product_stats['ROAS'].replace([float('inf'), -float('inf')], 0).fillna(0)

    # Merge with Book.csv to include Price and Margin
    logging.info("Merging with Book.csv to include Price and Margin...")
    book_subset = book_df[['Product', 'Price', 'Margin']]
    product_stats = product_stats.merge(book_subset, on='Product', how='left')

    # Validate Revenue: Expected Revenue = Price * Total Sales (Purchases)
    product_stats['Expected Revenue (INR)'] = product_stats['Price'] * product_stats['Total Sales (Purchases)']
    product_stats['Revenue Validation'] = product_stats.apply(
        lambda row: 'Matches' if pd.isna(row['Price']) or abs(row['Total Revenue (INR)'] - row['Expected Revenue (INR)']) < 1e-6
        else f"Mismatch (Actual: {row['Total Revenue (INR)']}, Expected: {row['Expected Revenue (INR)']})",
        axis=1
    )

    # Save the results to a new CSV file
    output_filename = f"product_metrics_summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    output_path = os.path.join(script_dir, output_filename)
    product_stats.to_csv(output_path, index=False)
    logging.info(f"Results saved to '{output_path}'.")

    print(f"Analysis complete. Results saved to '{output_path}'. Check 'product_search.log' in the script directory for details.")

except Exception as e:
    logging.error(f"Script failed: {e}")
    print(f"An error occurred: {e}. Check 'product_search.log' in the script directory for details.")

finally:
    logging.info(f"Script execution completed at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}.")