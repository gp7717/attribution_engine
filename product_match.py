import pandas as pd
import logging
from datetime import datetime

# Set up logging to create product_search.log if it doesn't exist
logging.basicConfig(
    filename='product_search.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filemode='a',  # Append mode to create the file if it doesn't exist
    force=True  # Ensure the logger is reset to use this configuration
)
logging.info("Logging setup complete. product_search.log created or opened for appending.")
logging.info(f"Script execution started at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}.")

def search_product(row, product_mappings, row_index):
    """
    Search for a product based on search terms in the 'Ad name' column.
    Args:
        row: A row from the DataFrame.
        product_mappings: A dictionary mapping products to their search terms.
        row_index: The index of the row being processed (for logging).
    Returns:
        The matched product name or "Brand Awareness" if no match is found.
    """
    try:
        # Get the value of the 'Ad name' column (handle NaN and non-string values)
        ad_name = str(row.get('Ad name', '')) if pd.notna(row.get('Ad name', '')) else ""
        
        # Iterate through the product mappings (product -> search terms)
        for product, search_terms in product_mappings.items():
            # Skip if search_terms is empty or invalid
            if not search_terms or pd.isna(search_terms):
                logging.warning(f"Row {row_index}: Empty or invalid search terms for product '{product}'.")
                continue

            # Split the search terms (e.g., "FMB, MISTBRU, FLUFFMIST" -> ["FMB", "MISTBRU", "FLUFFMIST"])
            terms = [term.strip() for term in str(search_terms).split(",")]

            # Check if any search term matches in the 'Ad name' column
            for term in terms:
                if term and term.lower() in ad_name.lower():
                    logging.info(f"Row {row_index}: Match found for product '{product}' in Ad name '{ad_name}' with term '{term}'.")
                    return product

        # If no match is found, return default
        logging.debug(f"Row {row_index}: No match found for Ad name '{ad_name}'.")
        return "Brand Awareness"

    except Exception as e:
        logging.error(f"Row {row_index}: Error processing row: {e}")
        return "Brand Awareness"

try:
    # Load the product mapping from Book.csv
    logging.info("Loading Book.csv...")
    book_df = pd.read_csv("analysis/Book.csv")
    logging.info(f"Loaded Book.csv successfully. Found {len(book_df)} rows.")

    # Create a mapping of products to their search terms
    product_mappings = dict(zip(book_df['Product'], book_df['Search']))
    logging.info(f"Created product mappings with {len(product_mappings)} entries.")

    # Load the ad data from ad_data.csv
    logging.info("Loading ad_data.csv...")
    ad_df = pd.read_csv("6month data/day-age-gender-Apr-May.csv")
    logging.info(f"Loaded ad_data.csv successfully. Found {len(ad_df)} rows.")

    # Apply the search logic to each row and store results in a new column
    logging.info("Starting product matching process...")
    ad_df['Matched Product'] = ad_df.apply(
        lambda row: search_product(row, product_mappings, row.name), axis=1
    )
    logging.info("Completed product matching for all rows.")

    # Merge with Book.csv to add Price and Margin columns
    logging.info("Merging with Book.csv to add Price and Margin...")
    book_subset = book_df[['Product', 'Price', 'Margin']].rename(columns={'Product': 'Matched Product'})
    ad_df = ad_df.merge(book_subset, on='Matched Product', how='left')
    logging.info("Price and Margin columns added successfully.")

    # Save the updated dataframe to a new CSV file
    output_filename = f"ad_data_with_matched_products_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    ad_df.to_csv(output_filename, index=False)
    logging.info(f"Results saved to '{output_filename}'.")

    print(f"Processing complete. Results saved to '{output_filename}'. Check 'product_search.log' for details.")

except Exception as e:
    logging.error(f"Script failed: {e}")
    print(f"An error occurred: {e}. Check 'product_search.log' for details.")

finally:
    logging.info(f"Script execution completed at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}.")