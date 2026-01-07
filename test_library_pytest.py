"""
Pytest tests for library_data_cleaning.py
Tests key data cleaning and validation functions using pytest framework
"""

import pytest
import pandas as pd
import numpy as np
from datetime import datetime
import os
import tempfile
from library_data_cleaning import (
    clean_books_data,
    clean_customers_data,
    add_missing_customers,
    save_cleaned_data
)

@pytest.fixture
def sample_books_df():
    """Fixture providing sample books data with known issues"""
    return pd.DataFrame({
        'Id': [1, 2, 3, np.nan],
        'Books': ['Book A', 'Book B', 'Book C', np.nan],
        'Book checkout': ['01/01/2023', '"02/01/2023"', '32/01/2023', '01/01/2063'],
        'Book Returned': ['15/01/2023', '16/01/2023', '15/02/2023', '15/01/2063'],
        'Days allowed to borrow': ['2 weeks', '2 weeks', '2 weeks', '2 weeks'],
        'Customer ID': [1, 2, 3, np.nan]
    })

@pytest.fixture
def sample_customers_df():
    """Fixture providing sample customers data"""
    return pd.DataFrame({
        'Customer ID': [1, 2, np.nan],
        'Customer Name': ['Alice', 'Bob', np.nan]
    })

def test_clean_books_data_removes_nan_rows(sample_books_df):
    """Test that clean_books_data removes rows with NaN in Id or Books"""
    cleaned = clean_books_data(sample_books_df)
    
    # Should have 3 valid rows (4th has NaN)
    assert len(cleaned) == 3
    assert not cleaned['Id'].isna().any()
    assert not cleaned['Books'].isna().any()

def test_clean_books_data_fixes_invalid_dates(sample_books_df):
    """Test that invalid dates (year 2063, day 32) are corrected"""
    cleaned = clean_books_data(sample_books_df)
    
    # Check that 2063 was fixed to 2023
    checkout_dates = cleaned['Book checkout'].tolist()
    assert '01/01/2023' in checkout_dates  # Fixed from 2063
    
    # Check that day 32 was fixed to 31
    assert '31/01/2023' in checkout_dates  # Fixed from 32

def test_clean_books_data_calculates_overdue(sample_books_df):
    """Test that overdue status is calculated correctly"""
    cleaned = clean_books_data(sample_books_df, loan_period=14)
    
    # Check that days_allowed column is set correctly
    assert (cleaned['days_allowed'] == 14).all()
    
    # Check that is_overdue column exists and is boolean
    assert 'is_overdue' in cleaned.columns
    assert cleaned['is_overdue'].dtype == bool

def test_clean_books_data_custom_loan_period(sample_books_df):
    """Test that custom loan period is applied correctly"""
    cleaned = clean_books_data(sample_books_df, loan_period=21)
    
    # Check that custom loan period is set
    assert (cleaned['days_allowed'] == 21).all()

def test_clean_customers_data_removes_nan(sample_customers_df):
    """Test that clean_customers_data removes rows with NaN"""
    cleaned = clean_customers_data(sample_customers_df)
    
    # Should have 2 valid rows
    assert len(cleaned) == 2
    assert not cleaned['Customer ID'].isna().any()
    assert not cleaned['Customer Name'].isna().any()

def test_clean_customers_data_converts_id_to_int(sample_customers_df):
    """Test that Customer ID is converted to integer type"""
    cleaned = clean_customers_data(sample_customers_df)
    
    # Check that Customer ID is integer type
    assert cleaned['Customer ID'].dtype == 'int64'

def test_add_missing_customers():
    """Test that missing customer IDs are added with placeholder names"""
    # Create books with customer ID 5 that doesn't exist in customers
    books = pd.DataFrame({
        'Customer ID': [1, 2, 5]
    })
    
    customers = pd.DataFrame({
        'Customer ID': [1, 2],
        'Customer Name': ['Alice', 'Bob']
    })
    
    result = add_missing_customers(customers, books)
    
    # Should now have 3 customers including the missing one
    assert len(result) == 3
    assert 5 in result['Customer ID'].values
    
    # Check placeholder name format
    missing_customer = result[result['Customer ID'] == 5]
    assert 'Unknown Customer' in missing_customer['Customer Name'].values[0]

def test_add_missing_customers_no_missing():
    """Test that function works when no customers are missing"""
    books = pd.DataFrame({
        'Customer ID': [1, 2]
    })
    
    customers = pd.DataFrame({
        'Customer ID': [1, 2],
        'Customer Name': ['Alice', 'Bob']
    })
    
    result = add_missing_customers(customers, books)
    
    # Should still have 2 customers
    assert len(result) == 2

def test_save_cleaned_data_creates_files(sample_books_df, sample_customers_df):
    """Test that save_cleaned_data creates CSV files with correct columns"""
    with tempfile.TemporaryDirectory() as tmpdir:
        books_output = os.path.join(tmpdir, 'books_test.csv')
        customers_output = os.path.join(tmpdir, 'customers_test.csv')
        
        # Clean the data first
        cleaned_books = clean_books_data(sample_books_df)
        cleaned_customers = clean_customers_data(sample_customers_df)
        
        # Save the data
        save_cleaned_data(cleaned_books, cleaned_customers, books_output, customers_output)
        
        # Check that files were created
        assert os.path.exists(books_output)
        assert os.path.exists(customers_output)
        
        # Check that files have correct columns
        saved_books = pd.read_csv(books_output)
        assert 'Id' in saved_books.columns
        assert 'Books' in saved_books.columns
        assert 'is_overdue' in saved_books.columns
        
        saved_customers = pd.read_csv(customers_output)
        assert 'Customer ID' in saved_customers.columns
        assert 'Customer Name' in saved_customers.columns

def test_save_cleaned_data_preserves_row_count(sample_books_df, sample_customers_df):
    """Test that saved files preserve the correct number of rows"""
    with tempfile.TemporaryDirectory() as tmpdir:
        books_output = os.path.join(tmpdir, 'books_test.csv')
        customers_output = os.path.join(tmpdir, 'customers_test.csv')
        
        # Clean the data first
        cleaned_books = clean_books_data(sample_books_df)
        cleaned_customers = clean_customers_data(sample_customers_df)
        
        # Save the data
        save_cleaned_data(cleaned_books, cleaned_customers, books_output, customers_output)
        
        # Check row counts match
        saved_books = pd.read_csv(books_output)
        saved_customers = pd.read_csv(customers_output)
        
        assert len(saved_books) == len(cleaned_books)
        assert len(saved_customers) == len(cleaned_customers)
