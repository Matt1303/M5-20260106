### Summary
This project cleans and validates library checkout data from CSV files, then stores the cleaned data in both CSV format and a SQLite database. The system handles data quality issues, fixes invalid dates, calculates overdue status, and maintains referential integrity between customers and book loans.

---

## Data Cleaning

### Key Functions

#### 1. **`load_data(books_path, customers_path)`**
Loads raw CSV files into pandas DataFrames for processing.

#### 2. **`analyse_data_quality(books_df, customers_df)`**
Performs initial data quality assessment:
- Identifies rows with missing values (NaN)
- Detects invalid dates manually (year 2063, day 32)
- Checks referential integrity (customer IDs in loans that don't exist in customers table)
- Returns list of data quality issues

#### 3. **`clean_books_data(books_df, loan_period=14)`**
Core cleaning function for book loan records:
- **Removes empty rows**: Drops completely empty rows and rows with NaN in critical fields (Id, Books)
- **Cleans date formats**: Strips extra quotes from date strings
- **Fixes invalid dates**: 
  - Corrects year 2063 → 2023
  - Corrects day 32 → day 31
- **Calculates derived fields**:
  - `days_borrowed`: Difference between return and checkout dates
  - `is_overdue`: Boolean flag if borrowed days exceed allowed period
  - `days_overdue`: Number of days past the allowed period
- **Configurable loan period**: Accepts custom loan period via `loan_period` parameter (default: 14 days) - can be improved in future by using the column within the '03_Library Systembook' file, which may contain different loan periods (currently all are 2 weeks)

#### 4. **`clean_customers_data(customers_df)`**
Cleans customer records:
- Removes empty rows
- Drops records with missing Customer ID or Name
- Converts Customer ID to integer type for consistency

#### 5. **`add_missing_customers(customers_df, books_df)`**
Maintains referential integrity:
- Identifies customer IDs in loan records that don't exist in customers table
- Creates placeholder customer records: `"Unknown Customer {ID}"`
- Ensures all foreign key relationships are valid

#### 6. **`save_cleaned_data(books_df, customers_df, books_output_path, customers_output_path)`**
Exports cleaned data to CSV files with selected columns for analysis.

---

## Testing Strategy

### Pytest Implementation

 `test_library_pytest.py` using pytest framework:

**Test Coverage:**
- ✓ NaN removal verification
- ✓ Invalid date correction (2063 → 2023, day 32 → 31)
- ✓ Overdue calculation logic
- ✓ Custom loan period application
- ✓ Customer ID type conversion
- ✓ Missing customer handling
- ✓ CSV file creation and column validation
- ✓ Data preservation during save operations


**Run Tests:**
```bash
pytest test_library_pytest.py -v
```

---

## SQLite Database Storage

### Table Definitions (SQLAlchemy ORM)

#### **1. Customers Table**
```python
customer_id (Integer, Primary Key)
customer_name (String(200), Not Null)
```
Stores library member information.

#### **2. Books Table**
```python
book_id (Integer, Primary Key, Auto-increment)
title (String(500), Not Null, Unique)
```
Catalog of unique book titles in the library.

#### **3. Loans Table**
```python
loan_id (Integer, Primary Key)
book_id (Integer, Foreign Key → books.book_id)
customer_id (Integer, Foreign Key → customers.customer_id)
checkout_date (Date, Not Null)
return_date (Date, Nullable)
days_allowed (Integer, Default: 14)
```
Tracks book checkout and return transactions with foreign key relationships to Books and Customers tables.

**Design Rationale:**
- **Normalisation**: Books are stored separately to avoid duplication (multiple loans of same book)
- **Minimal storage**: Only stores base data (checkout/return dates), not calculated fields
- **Calculated fields** (days_borrowed, is_overdue, days_overdue) can be computed at query time
- **Foreign keys**: Maintain referential integrity between tables

### Database Operations

**`save_to_database(books_df, customers_df, db_path='library_system.db')`**
- Creates database schema using SQLAlchemy ORM
- Clears existing data (ensures clean state)
- Inserts customers, unique books, and loan records
- Maintains relationships through foreign keys
- Handles date conversion from pandas datetime to Python date objects

---

## Usage

### Command-Line Arguments (using argparse)

```bash
# basic usage (default files)
python library_data_cleaning.py

# custom input files
python library_data_cleaning.py --books-input "data/books.csv" --customers-input "data/customers.csv"

# save to database
python library_data_cleaning.py --save-to-db

# custom loan period
python library_data_cleaning.py --loan-period 21

# all
python library_data_cleaning.py \
  --books-input "books.csv" \
  --customers-input "customers.csv" \
  --books-output "cleaned_books.csv" \
  --customers-output "cleaned_customers.csv" \
  --db-path "library.db" \
  --loan-period 14 \
  --save-to-db
```

