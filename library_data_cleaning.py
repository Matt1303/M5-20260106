"""
Library System Data Cleaning & Validation Script
Phase 1: Clean and validate CSV data files - using arparse for input/output paths
Phase 2: Save cleaned data to database using SQLAlchemy (not implemented yet)
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import argparse
import logging
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from database_models import Base, Customer, Book, Loan

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('library_cleaning.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def parse_arguments():
    parser = argparse.ArgumentParser(
        description='Library System Data Cleaning & Validation Script'
    )
    
    parser.add_argument(
        '--books-input',
        default='03_Library Systembook.csv',
        help='Path to books CSV file (default: 03_Library Systembook.csv)'
    )
    
    parser.add_argument(
        '--customers-input',
        default='03_Library SystemCustomers.csv',
        help='Path to customers CSV file (default: 03_Library SystemCustomers.csv)'
    )
    
    parser.add_argument(
        '--books-output',
        default='03_Library Systembook_cleaned.csv',
        help='Path for cleaned books output (default: 03_Library Systembook_cleaned.csv)'
    )
    
    parser.add_argument(
        '--customers-output',
        default='03_Library SystemCustomers_cleaned.csv',
        help='Path for cleaned customers output (default: 03_Library SystemCustomers_cleaned.csv)'
    )
    
    parser.add_argument(
        '--db-path',
        default='library_system.db',
        help='Path for SQLite database (default: library_system.db)'
    )
    
    parser.add_argument(
        '--save-to-db',
        action='store_true',
        help='Save cleaned data to SQLite database'
    )
    
    parser.add_argument(
        '--loan-period',
        type=int,
        default=14,
        help='Number of days allowed for borrowing (default: 14)'
    )
    
    return parser.parse_args()

def load_data(books_path, customers_path):
    
    books_df = pd.read_csv(books_path)
    customers_df = pd.read_csv(customers_path)
    
    return books_df, customers_df

def analyse_data_quality(books_df, customers_df):
    
    issues = []
    
    logger.info(f"Starting data quality analysis")
    logger.info(f"Books data: Total rows: {len(books_df)}")
    logger.warning(f"Books - Rows with NaN in Id: {books_df['Id'].isna().sum()}")
    logger.warning(f"Books - Rows with NaN in Books: {books_df['Books'].isna().sum()}")
    logger.warning(f"Books - Rows with NaN in Customer ID: {books_df['Customer ID'].isna().sum()}")
    logger.warning(f"Books - Completely empty rows: {books_df.isna().all(axis=1).sum()}")
    
    # check for invalid dates
    invalid_dates = []
    for idx, row in books_df.iterrows():
        if pd.notna(row['Book checkout']):
            checkout = str(row['Book checkout']).strip('"')
            # manual checks for known issues
            if '2063' in checkout:
                invalid_dates.append((idx, checkout, '2063 - too far in future'))
                logger.error(f"Invalid date found - Row {idx}: {checkout} (year 2063 in future)")
            if checkout.startswith('32/'):
                invalid_dates.append((idx, checkout, 'Not 32 days in month'))
                logger.error(f"Invalid date found - Row {idx}: {checkout} (day 32 invalid)")
    
    if invalid_dates:
        issues.extend(invalid_dates)
    
    logger.info(f"Customers data: Total rows: {len(customers_df)}")
    logger.warning(f"Customers - Rows with NaN in Customer ID: {customers_df['Customer ID'].isna().sum()}")
    logger.warning(f"Customers - Rows with NaN in Customer Name: {customers_df['Customer Name'].isna().sum()}")
    logger.warning(f"Customers - Completely empty rows: {customers_df.isna().all(axis=1).sum()}")
    
    books_customer_ids = books_df['Customer ID'].dropna().unique()
    customers_ids = customers_df['Customer ID'].dropna().unique()
    
    missing_customers = set(books_customer_ids) - set(customers_ids)
    if missing_customers:
        missing_customers_int = {int(x) for x in missing_customers}
        logger.error(f"Referential integrity issue - Customer IDs in books but not in customers table: {missing_customers_int}")
        issues.append(('referential_integrity', missing_customers_int))
    else:
        logger.info("All customer IDs are valid!")
    
    logger.info(f"Data quality analysis complete. Total issues found: {len(issues)}")
    return issues

def clean_books_data(books_df, loan_period=14):

    logger.info(f"Starting books data cleaning (loan period: {loan_period} days)")
    
    # remove completely empty rows
    original_count = len(books_df)
    books_df = books_df.dropna(how='all')
    removed = original_count - len(books_df)
    if removed > 0:
        logger.info(f"Removed {removed} completely empty rows from books data")
    
    # remove rows with NaN
    before_nan_removal = len(books_df)
    books_df = books_df.dropna(subset=['Id', 'Books'])
    removed_nan = before_nan_removal - len(books_df)
    if removed_nan > 0:
        logger.info(f"Removed {removed_nan} rows with missing Id or Books values")
    
    # clean date formats (remove extra quotes)
    def clean_date(date_str):
        if pd.isna(date_str):
            return None
        date_str = str(date_str).strip().strip('"')
        return date_str
    
    books_df['Book checkout'] = books_df['Book checkout'].apply(clean_date)
    books_df['Book Returned'] = books_df['Book Returned'].apply(clean_date)
    logger.debug("Cleaned date format (removed extra quotes)")
    
    def fix_invalid_date(date_str, row_id):
        if pd.isna(date_str) or date_str is None:
            return None
            
        # manually fix year 2063 to 2023
        if '2063' in date_str:
            fixed_date = date_str.replace('2063', '2023')
            logger.warning(f"Fixed invalid year in row {row_id}: {date_str} -> {fixed_date}")
            return fixed_date
        
        # manually fix day 32 to day 31
        if date_str.startswith('32/'):
            fixed_date = '31/' + date_str[3:]
            logger.warning(f"Fixed invalid day in row {row_id}: {date_str} -> {fixed_date}")
            return fixed_date
        
        return date_str
    
    books_df['Book checkout'] = books_df.apply(
        lambda row: fix_invalid_date(row['Book checkout'], row['Id']), axis=1
    )
    books_df['Book Returned'] = books_df.apply(
        lambda row: fix_invalid_date(row['Book Returned'], row['Id']), axis=1
    )
    
    books_df['checkout_date'] = pd.to_datetime(books_df['Book checkout'], format='%d/%m/%Y', errors='coerce')
    books_df['return_date'] = pd.to_datetime(books_df['Book Returned'], format='%d/%m/%Y', errors='coerce')
    
    # Log any dates that couldn't be parsed
    unparsed_checkout = books_df['checkout_date'].isna().sum()
    unparsed_return = books_df['return_date'].isna().sum()
    if unparsed_checkout > 0:
        logger.warning(f"{unparsed_checkout} checkout dates could not be parsed")
    if unparsed_return > 0:
        logger.warning(f"{unparsed_return} return dates could not be parsed")
    
    books_df['days_borrowed'] = (books_df['return_date'] - books_df['checkout_date']).dt.days
    
    books_df['days_allowed'] = loan_period
    books_df['is_overdue'] = books_df['days_borrowed'] > books_df['days_allowed']
    books_df['days_overdue'] = books_df.apply(
        lambda row: max(0, row['days_borrowed'] - row['days_allowed']) if pd.notna(row['days_borrowed']) else 0,
        axis=1
    )
    
    overdue_count = books_df['is_overdue'].sum()
    logger.info(f"Books cleaning complete. Found {overdue_count} overdue loans out of {len(books_df)} records")
    
    return books_df

def clean_customers_data(customers_df):

    logger.info("Starting customers data cleaning")
    
    # remove completely empty rows 
    original_count = len(customers_df)
    customers_df = customers_df.dropna(how='all')
    removed = original_count - len(customers_df)
    if removed > 0:
        logger.info(f"Removed {removed} completely empty rows from customers data")
    
    # remove rows with NaN in Customer ID or Name
    before_nan_removal = len(customers_df)
    customers_df = customers_df.dropna(subset=['Customer ID', 'Customer Name'])
    removed_nan = before_nan_removal - len(customers_df)
    if removed_nan > 0:
        logger.info(f"Removed {removed_nan} rows with missing Customer ID or Name")
    
    customers_df['Customer ID'] = customers_df['Customer ID'].astype(int)
    logger.info(f"Customers cleaning complete. {len(customers_df)} valid customer records")
    
    return customers_df

def add_missing_customers(customers_df, books_df):
    
    logger.info("Checking for missing customers in customer table")
    
    books_customer_ids = set(books_df['Customer ID'].dropna().astype(int).unique())
    existing_customer_ids = set(customers_df['Customer ID'].unique())
    
    missing_ids = books_customer_ids - existing_customer_ids
    
    if missing_ids:
        logger.warning(f"Found {len(missing_ids)} missing customer IDs: {sorted(missing_ids)}")
        # add missing customers with placeholder names        
        for cust_id in missing_ids:
            new_customer = pd.DataFrame({
                'Customer ID': [cust_id],
                'Customer Name': [f'Unknown Customer {cust_id}']
            })
            customers_df = pd.concat([customers_df, new_customer], ignore_index=True)
            logger.info(f"Added missing customer record: ID {cust_id}")
        
        customers_df = customers_df.sort_values('Customer ID').reset_index(drop=True)
        logger.info(f"All missing customers added. Total customers now: {len(customers_df)}")
    else:
        logger.info("No missing customers found")
    
    return customers_df

def save_cleaned_data(books_df, customers_df, books_output_path, customers_output_path):
    
    logger.info("Saving cleaned data to CSV files")
    
    # select columns for output
    books_output = books_df[[
        'Id', 'Books', 'Book checkout', 'Book Returned', 
        'Days allowed to borrow', 'Customer ID', 
        'days_borrowed', 'is_overdue', 'days_overdue'
    ]].copy()
    
    customers_output = customers_df[['Customer ID', 'Customer Name']].copy()
    
    books_output.to_csv(books_output_path, index=False)
    customers_output.to_csv(customers_output_path, index=False)
    
    logger.info(f"Cleaned books data saved: {books_output_path} ({len(books_output)} records)")
    logger.info(f"Cleaned customers data saved: {customers_output_path} ({len(customers_output)} records)")

def save_to_database(books_df, customers_df, db_path='library_system.db'):
    """saves cleaned data to SQLite database using SQLAlchemy"""

    logger.info(f"Starting database save to {db_path}")
    
    engine = create_engine(f'sqlite:///{db_path}', echo=False)
    Base.metadata.create_all(engine)
    
    Session = sessionmaker(bind=engine)
    session = Session()
    
    try:
        # delete existing data
        deleted_loans = session.query(Loan).delete()
        deleted_books = session.query(Book).delete()
        deleted_customers = session.query(Customer).delete()
        session.commit()
        logger.info(f"Cleared existing data: {deleted_customers} customers, {deleted_books} books, {deleted_loans} loans")
        
        # Insert customers
        for _, row in customers_df.iterrows():
            customer = Customer(
                customer_id=int(row['Customer ID']),
                customer_name=row['Customer Name']
            )
            session.add(customer)
        session.commit()
        logger.info(f"Inserted {len(customers_df)} customers into database")
        
        # Insert unique books
        unique_books = books_df['Books'].unique()
        book_map = {}
        
        for idx, book_title in enumerate(unique_books, start=1):
            book = Book(
                book_id=idx,
                title=book_title
            )
            session.add(book)
            book_map[book_title] = idx
        session.commit()
        logger.info(f"Inserted {len(unique_books)} unique books into database")
        
        # Insert loans
        for _, row in books_df.iterrows():
            checkout_date = pd.to_datetime(row['Book checkout'], format='%d/%m/%Y', errors='coerce')
            return_date = pd.to_datetime(row['Book Returned'], format='%d/%m/%Y', errors='coerce')
            
            loan = Loan(
                loan_id=int(row['Id']),
                book_id=book_map[row['Books']],
                customer_id=int(row['Customer ID']),
                checkout_date=checkout_date.date() if pd.notna(checkout_date) else None,
                return_date=return_date.date() if pd.notna(return_date) else None,
                days_allowed=int(row['days_allowed'])
            )
            session.add(loan)
        session.commit()
        logger.info(f"Inserted {len(books_df)} loans into database")
        logger.info(f"Database save completed successfully: {db_path}")
        
    except Exception as e:
        session.rollback()
        logger.error(f"Database error: {e}", exc_info=True)
        raise
    finally:
        session.close()

def main():
    args = parse_arguments()
    
    logger.info("=" * 60)
    logger.info("Library System Data Cleaning & Validation Started")
    logger.info("=" * 60)
    logger.info(f"Loading data from:")
    logger.info(f"  Books: {args.books_input}")
    logger.info(f"  Customers: {args.customers_input}")
    
    books_df, customers_df = load_data(args.books_input, args.customers_input)
    logger.info(f"Data loaded successfully: {len(books_df)} books records, {len(customers_df)} customers records")
    
    issues = analyse_data_quality(books_df, customers_df)
     
     
    books_df = clean_books_data(books_df, args.loan_period)
    customers_df = clean_customers_data(customers_df)

    customers_df = add_missing_customers(customers_df, books_df)
    
    save_cleaned_data(books_df, customers_df, args.books_output, args.customers_output)
    
    if args.save_to_db:
        save_to_database(books_df, customers_df, args.db_path)
    
    logger.info("=" * 60)
    logger.info("Library System Data Cleaning & Validation Complete")
    logger.info("=" * 60)

if __name__ == "__main__":
    main()
