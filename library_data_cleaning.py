"""
Library System Data Cleaning & Validation Script
Phase 1: Clean and validate CSV data files - using arparse for input/output paths
Phase 2: Save cleaned data to database using SQLAlchemy (not implemented yet)
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import argparse
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from database_models import Base, Customer, Book, Loan

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
    
    print(f"Total rows: {len(books_df)}")
    print(f"Rows with NaN in Id: {books_df['Id'].isna().sum()}")
    print(f"Rows with NaN in Books: {books_df['Books'].isna().sum()}")
    print(f"Rows with NaN in Customer ID: {books_df['Customer ID'].isna().sum()}")
    print(f"Completely empty rows: {books_df.isna().all(axis=1).sum()}")
    
    # check for invalid dates
    invalid_dates = []
    for idx, row in books_df.iterrows():
        if pd.notna(row['Book checkout']):
            checkout = str(row['Book checkout']).strip('"')
            # manual checks for known issues
            if '2063' in checkout:
                invalid_dates.append((idx, checkout, '2063 - too far in future'))
            if checkout.startswith('32/'):
                invalid_dates.append((idx, checkout, 'Not 32 days in month'))
    
    if invalid_dates:
        issues.extend(invalid_dates)
    
    print(f"Total rows: {len(customers_df)}")
    print(f"Rows with NaN in Customer ID: {customers_df['Customer ID'].isna().sum()}")
    print(f"Rows with NaN in Customer Name: {customers_df['Customer Name'].isna().sum()}")
    print(f"Completely empty rows: {customers_df.isna().all(axis=1).sum()}")
    
    books_customer_ids = books_df['Customer ID'].dropna().unique()
    customers_ids = customers_df['Customer ID'].dropna().unique()
    
    missing_customers = set(books_customer_ids) - set(customers_ids)
    if missing_customers:
        missing_customers_int = {int(x) for x in missing_customers}
        print(f"Customer IDs in books but not in customers table: {missing_customers_int}")
        issues.append(('referential_integrity', missing_customers_int))
    else:
        print("All customer IDs are valid!")
    
    print(issues)
    return issues

def clean_books_data(books_df, loan_period=14):

    # remove completely empty rows
    original_count = len(books_df)
    books_df = books_df.dropna(how='all')
    removed = original_count - len(books_df)
    
    # remove rows with NaN
    books_df = books_df.dropna(subset=['Id', 'Books'])
    
    # clean date formats (remove extra quotes)
    def clean_date(date_str):
        if pd.isna(date_str):
            return None
        date_str = str(date_str).strip().strip('"')
        return date_str
    
    books_df['Book checkout'] = books_df['Book checkout'].apply(clean_date)
    books_df['Book Returned'] = books_df['Book Returned'].apply(clean_date)
    
    def fix_invalid_date(date_str, row_id):
        if pd.isna(date_str) or date_str is None:
            return None
            
        # manually fix year 2063 to 2023
        if '2063' in date_str:
            date_str = date_str.replace('2063', '2023')
        
        # manually fix day 32 to day 31
        if date_str.startswith('32/'):
            date_str = '31/' + date_str[3:]
        
        return date_str
    
    books_df['Book checkout'] = books_df.apply(
        lambda row: fix_invalid_date(row['Book checkout'], row['Id']), axis=1
    )
    books_df['Book Returned'] = books_df.apply(
        lambda row: fix_invalid_date(row['Book Returned'], row['Id']), axis=1
    )
    
    books_df['checkout_date'] = pd.to_datetime(books_df['Book checkout'], format='%d/%m/%Y', errors='coerce')
    books_df['return_date'] = pd.to_datetime(books_df['Book Returned'], format='%d/%m/%Y', errors='coerce')
    
    books_df['days_borrowed'] = (books_df['return_date'] - books_df['checkout_date']).dt.days
    
    books_df['days_allowed'] = loan_period
    books_df['is_overdue'] = books_df['days_borrowed'] > books_df['days_allowed']
    books_df['days_overdue'] = books_df.apply(
        lambda row: max(0, row['days_borrowed'] - row['days_allowed']) if pd.notna(row['days_borrowed']) else 0,
        axis=1
    )
    
    return books_df

def clean_customers_data(customers_df):

    # remove completely empty rows 
    original_count = len(customers_df)
    customers_df = customers_df.dropna(how='all')
    removed = original_count - len(customers_df)
    
    # remove rows with NaN in Customer ID or Name
    customers_df = customers_df.dropna(subset=['Customer ID', 'Customer Name'])
    
    customers_df['Customer ID'] = customers_df['Customer ID'].astype(int)
    
    return customers_df

def add_missing_customers(customers_df, books_df):
    
    books_customer_ids = set(books_df['Customer ID'].dropna().astype(int).unique())
    existing_customer_ids = set(customers_df['Customer ID'].unique())
    
    missing_ids = books_customer_ids - existing_customer_ids
    
    if missing_ids:
    # add missing customers with placeholder names        
        for cust_id in missing_ids:
            new_customer = pd.DataFrame({
                'Customer ID': [cust_id],
                'Customer Name': [f'Unknown Customer {cust_id}']
            })
            customers_df = pd.concat([customers_df, new_customer], ignore_index=True)
        
        customers_df = customers_df.sort_values('Customer ID').reset_index(drop=True)
    
    return customers_df

def save_cleaned_data(books_df, customers_df, books_output_path, customers_output_path):
    
    # select columns for output
    books_output = books_df[[
        'Id', 'Books', 'Book checkout', 'Book Returned', 
        'Days allowed to borrow', 'Customer ID', 
        'days_borrowed', 'is_overdue', 'days_overdue'
    ]].copy()
    
    customers_output = customers_df[['Customer ID', 'Customer Name']].copy()
    
    books_output.to_csv(books_output_path, index=False)
    customers_output.to_csv(customers_output_path, index=False)
    
    print(f"\nCleaned data saved to:")
    print(f"  Books: {books_output_path}")
    print(f"  Customers: {customers_output_path}")

def save_to_database(books_df, customers_df, db_path='library_system.db'):
    """saves cleaned data to SQLite database using SQLAlchemy"""

    engine = create_engine(f'sqlite:///{db_path}', echo=False)
    Base.metadata.create_all(engine)
    
    Session = sessionmaker(bind=engine)
    session = Session()
    
    try:
        # delete existing data
        session.query(Loan).delete()
        session.query(Book).delete()
        session.query(Customer).delete()
        session.commit()
        
        # Insert customers
        for _, row in customers_df.iterrows():
            customer = Customer(
                customer_id=int(row['Customer ID']),
                customer_name=row['Customer Name']
            )
            session.add(customer)
        session.commit()
        
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
        
        print(f"\nDatabase saved: {db_path}")
        
    except Exception as e:
        session.rollback()
        print(f"\nDatabase error: {e}")
        raise
    finally:
        session.close()

def main():
    args = parse_arguments()
    
    print(f"Loading data from:")
    print(f"  Books: {args.books_input}")
    print(f"  Customers: {args.customers_input}")
    print()
    
    books_df, customers_df = load_data(args.books_input, args.customers_input)
    
    issues = analyse_data_quality(books_df, customers_df)
    
    books_df = clean_books_data(books_df, args.loan_period)
    customers_df = clean_customers_data(customers_df)

    customers_df = add_missing_customers(customers_df, books_df)
    
    save_cleaned_data(books_df, customers_df, args.books_output, args.customers_output)
    
    if args.save_to_db:
        save_to_database(books_df, customers_df, args.db_path)

if __name__ == "__main__":
    main()
