"""
Library System Data Cleaning & Validation Script
Phase 1: Clean and validate CSV data files
Phase 2: Save cleaned data to database using SQLAlchemy (not implemented yet)
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta

def load_data():
    
    books_df = pd.read_csv('03_Library Systembook.csv')
    customers_df = pd.read_csv('03_Library SystemCustomers.csv')
    
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

def clean_books_data(books_df):

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
    
    books_df['days_allowed'] = 14
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

def save_cleaned_data(books_df, customers_df):
    
    # Select columns for output
    books_output = books_df[[
        'Id', 'Books', 'Book checkout', 'Book Returned', 
        'Days allowed to borrow', 'Customer ID', 
        'days_borrowed', 'is_overdue', 'days_overdue'
    ]].copy()
    
    customers_output = customers_df[['Customer ID', 'Customer Name']].copy()
    
    # Save to CSV
    books_output.to_csv('03_Library Systembook_cleaned.csv', index=False)
    customers_output.to_csv('03_Library SystemCustomers_cleaned.csv', index=False)

def main():
    
    books_df, customers_df = load_data()
    
    issues = analyse_data_quality(books_df, customers_df)
    
    books_df = clean_books_data(books_df)
    customers_df = clean_customers_data(customers_df)

    customers_df = add_missing_customers(customers_df, books_df)
    
    save_cleaned_data(books_df, customers_df)

if __name__ == "__main__":
    main()
