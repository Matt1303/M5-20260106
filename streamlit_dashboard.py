"""
Library System Data Cleaning Dashboard
Displays data cleaning steps, statistics, and quality metrics using Streamlit
"""

import streamlit as st
import pandas as pd
import re
from pathlib import Path
from datetime import datetime

st.set_page_config(
    page_title="Library Project - Data Engineering Dashboard",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.title("📚 Library Project - Data Engineering Dashboard")
st.markdown("---")

# Helper functions
@st.cache_data
def load_original_data():
    try:
        books = pd.read_csv("03_Library Systembook.csv")
        customers = pd.read_csv("03_Library SystemCustomers.csv")
        return books, customers
    except FileNotFoundError:
        return None, None

@st.cache_data
def load_cleaned_data():
    try:
        books = pd.read_csv("03_Library Systembook_cleaned.csv")
        customers = pd.read_csv("03_Library SystemCustomers_cleaned.csv")
        return books, customers
    except FileNotFoundError:
        return None, None

def read_log_file(log_path="library_cleaning.log"):
    if Path(log_path).exists():
        with open(log_path, 'r') as f:
            return f.readlines()
    return None

def parse_log_for_metrics(logs):
    metrics = {
        'empty_rows_removed': 0,
        'nan_rows_removed': 0,
        'invalid_dates_fixed': 0,
        'missing_customers_added': 0,
        'overdue_loans': 0,
        'final_books': 0,
        'final_customers': 0
    }
    
    for line in logs:
    # use regex to search through log lines for text which matches the captured groups, and update dictionary containing metrics totals
        if "Removed" in line and "empty rows" in line:
            match = re.search(r'Removed (\d+)', line)
            if match:
                metrics['empty_rows_removed'] += int(match.group(1))
        
        if "Removed" in line and "rows with missing" in line:
            match = re.search(r'Removed (\d+)', line)
            if match:
                metrics['nan_rows_removed'] += int(match.group(1))
        
        if "Fixed invalid" in line:
            metrics['invalid_dates_fixed'] += 1
        
        if "Found" in line and "missing customer" in line.lower():
            match = re.search(r'Found (\d+)', line)
            if match:
                metrics['missing_customers_added'] = int(match.group(1))
        
        if "overdue" in line.lower() and "Found" in line:
            match = re.search(r'Found (\d+)', line)
            if match:
                metrics['overdue_loans'] = int(match.group(1))
        
        if "Cleaned books data saved" in line:
            match = re.search(r'\((\d+) records\)', line)
            if match:
                metrics['final_books'] = int(match.group(1))
        
        if "Cleaned customers data saved" in line:
            match = re.search(r'\((\d+) records\)', line)
            if match:
                metrics['final_customers'] = int(match.group(1))
    
    return metrics

# main dashboard
original_books, original_customers = load_original_data()
cleaned_books, cleaned_customers = load_cleaned_data()
logs = read_log_file()

if logs is None:
    st.error("Log file not found. Please run the data cleaning script first:")
    st.code("python library_data_cleaning.py", language="bash") 
else:
    metrics = parse_log_for_metrics(logs)
    
    # Section 1: Data Quality Summary (i.e. before and after stats)
    st.header("📊 Data Quality Summary")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.subheader("Books Dataset")
        st.metric("Original Records", len(original_books) if original_books is not None else "N/A")
        st.metric("Cleaned Records", metrics['final_books'])
        st.metric("Rows Removed", metrics['empty_rows_removed'] + metrics['nan_rows_removed'])
    
    with col2:
        st.subheader("Customers Dataset")
        st.metric("Original Records", len(original_customers) if original_customers is not None else "N/A")
        st.metric("Cleaned Records", metrics['final_customers'])
        st.metric("Customers Added", metrics['missing_customers_added'])
    
    with col3:
        st.subheader("Data Quality Issues")
        st.metric("Invalid Dates Fixed", metrics['invalid_dates_fixed'])
        st.metric("Overdue Loans Detected", metrics['overdue_loans'])
        
        if cleaned_books is not None:
            null_returns = cleaned_books['Book Returned'].isna().sum()
            st.metric("Missing Return Dates", null_returns)
    
    # Section 2: Data Quality Metrics
    st.markdown("---")
    st.header("📈 Data Quality Metrics")
    
    if cleaned_books is not None:
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric(
                "Loan Period (days)",
                "2 weeks" if len(cleaned_books) > 0 else "N/A"
            )
        
        with col2:
            avg_days_borrowed = cleaned_books['days_borrowed'].mean()
            st.metric("Avg Days Borrowed", f"{avg_days_borrowed:.1f}")
        
        with col3:
            max_overdue = cleaned_books['days_overdue'].max()
            st.metric("Max Days Overdue", int(max_overdue) if max_overdue > 0 else 0)
        
        with col4:
            missing_ids = cleaned_customers[cleaned_customers['Customer Name'].str.contains('Unknown', na=False)]
            st.metric("Unknown Customers", len(missing_ids))
        
        # Show overdue loans details
        st.subheader("Overdue Loans Details")
        overdue_loans = cleaned_books[cleaned_books['is_overdue'] == True][
            ['Id', 'Books', 'Customer ID', 'days_borrowed', 'days_overdue']
        ].copy()
        
        if len(overdue_loans) > 0:
            st.dataframe(overdue_loans, use_container_width=True)
        else:
            st.info("✅ No overdue loans found!")
    
    # Section 3: Cleaning Process Log
    st.markdown("---")
    st.header("🔍 Data Cleaning Process Log")
    
    # Create tabs for different log views
    tab1, tab2, tab3 = st.tabs(["Full Log", "Errors Only", "Warnings Only"])
    
    with tab1:
        st.subheader("Complete Cleaning Log")
        log_text = "".join(logs)
        st.text_area(
            "Full log output",
            value=log_text,
            height=400,
            disabled=True,
            key="full_log"
        )
    
    with tab2:
        st.subheader("Errors & Issues")
        error_logs = [line for line in logs if " - ERROR - " in line]
        if error_logs:
            error_text = "".join(error_logs)
            st.text_area(
                "Error logs",
                value=error_text,
                height=300,
                disabled=True,
                key="error_log"
            )
        else:
            st.info("✅ No errors found!")
    
    with tab3:
        st.subheader("Warnings")
        warning_logs = [line for line in logs if " - WARNING - " in line]
        if warning_logs:
            warning_text = "".join(warning_logs)
            st.text_area(
                "Warning logs",
                value=warning_text,
                height=300,
                disabled=True,
                key="warning_log"
            )
        else:
            st.info("✅ No warnings found!")
    
    # Section 4: Cleaned Data Preview
    st.markdown("---")
    st.header("📄 Cleaned Data Preview")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Books Data")
        if cleaned_books is not None:
            st.dataframe(cleaned_books.head(10), use_container_width=True)
            st.caption(f"Showing 10 of {len(cleaned_books)} records")
    
    with col2:
        st.subheader("Customers Data")
        if cleaned_customers is not None:
            st.dataframe(cleaned_customers.head(10), use_container_width=True)
            st.caption(f"Showing 10 of {len(cleaned_customers)} records")
    

    # Footer
    st.markdown("---")
    st.markdown("""
    ### 📌 About This Dashboard
    
    This dashboard visualizes the results of the library system data cleaning process. It shows:
    
    - **Data Quality Issues Found:** Missing values, invalid dates, referential integrity problems
    - **Cleaning Actions Taken:** Row removal, date fixing, customer additions
    - **Final Data Quality:** Metrics on overdue loans, data completeness, and validity
    - **Audit Trail:** Complete log of all cleaning steps for transparency and reproducibility
    
    For more information, check the `library_cleaning.log` file or re-run the cleaning script with the `--save-to-db` flag to save data to SQLite.
    """)

if __name__ == "__main__":
    pass
