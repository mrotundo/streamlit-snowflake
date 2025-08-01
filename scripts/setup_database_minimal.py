#!/usr/bin/env python3
"""
Minimal database setup script without pandas dependency
Creates tables and generates mock data using only standard libraries
"""

import sqlite3
import random
from datetime import datetime, timedelta
import os
import sys

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def create_database(db_path='data/banking.db', num_customers=100):
    """Create database with minimal dependencies"""
    
    # Ensure directory exists
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    
    # Connect to database
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    print("Creating tables...")
    
    # Create base tables
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS customers (
            customer_id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            email TEXT UNIQUE,
            phone TEXT,
            segment TEXT,
            join_date DATE,
            credit_score INTEGER,
            annual_income REAL,
            employment_status TEXT,
            products_count INTEGER DEFAULT 0,
            total_relationship_value REAL DEFAULT 0,
            status TEXT DEFAULT 'active',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS loans (
            loan_id TEXT PRIMARY KEY,
            customer_id TEXT NOT NULL,
            loan_type TEXT NOT NULL,
            amount REAL NOT NULL,
            interest_rate REAL NOT NULL,
            term_months INTEGER NOT NULL,
            monthly_payment REAL NOT NULL,
            origination_date DATE NOT NULL,
            maturity_date DATE NOT NULL,
            status TEXT DEFAULT 'current',
            remaining_balance REAL NOT NULL,
            paid_amount REAL DEFAULT 0,
            late_fees REAL DEFAULT 0,
            last_payment_date DATE,
            next_payment_date DATE,
            date DATE NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
        )
    """)
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS deposits (
            account_id TEXT PRIMARY KEY,
            customer_id TEXT NOT NULL,
            account_type TEXT NOT NULL,
            balance REAL NOT NULL DEFAULT 0,
            interest_rate REAL DEFAULT 0,
            opened_date DATE NOT NULL,
            last_transaction_date DATE,
            status TEXT DEFAULT 'active',
            minimum_balance REAL DEFAULT 0,
            overdraft_limit REAL DEFAULT 0,
            date DATE NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
        )
    """)
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS transactions (
            transaction_id TEXT PRIMARY KEY,
            account_id TEXT,
            loan_id TEXT,
            customer_id TEXT NOT NULL,
            transaction_type TEXT NOT NULL,
            amount REAL NOT NULL,
            balance_after REAL,
            description TEXT,
            category TEXT,
            transaction_date TIMESTAMP NOT NULL,
            posted_date TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (account_id) REFERENCES deposits(account_id),
            FOREIGN KEY (loan_id) REFERENCES loans(loan_id),
            FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
        )
    """)
    
    # Create lineage tables
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS jobs (
            job_id TEXT PRIMARY KEY,
            job_name TEXT NOT NULL UNIQUE,
            job_description TEXT,
            job_type TEXT NOT NULL,
            schedule TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS job_runs (
            job_run_id TEXT PRIMARY KEY,
            job_id TEXT NOT NULL,
            start_time TIMESTAMP NOT NULL,
            end_time TIMESTAMP,
            status TEXT NOT NULL,
            rows_processed INTEGER DEFAULT 0,
            error_message TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (job_id) REFERENCES jobs(job_id)
        )
    """)
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS source_files (
            file_id TEXT PRIMARY KEY,
            file_name TEXT NOT NULL,
            file_path TEXT NOT NULL,
            file_type TEXT,
            file_size INTEGER,
            file_hash TEXT,
            arrival_time TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS job_run_source_files (
            job_run_id TEXT NOT NULL,
            file_id TEXT NOT NULL,
            PRIMARY KEY (job_run_id, file_id),
            FOREIGN KEY (job_run_id) REFERENCES job_runs(job_run_id),
            FOREIGN KEY (file_id) REFERENCES source_files(file_id)
        )
    """)
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS job_run_target_tables (
            job_run_id TEXT NOT NULL,
            schema_name TEXT NOT NULL,
            table_name TEXT NOT NULL,
            rows_inserted INTEGER DEFAULT 0,
            rows_updated INTEGER DEFAULT 0,
            rows_deleted INTEGER DEFAULT 0,
            PRIMARY KEY (job_run_id, schema_name, table_name),
            FOREIGN KEY (job_run_id) REFERENCES job_runs(job_run_id)
        )
    """)
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS data_views (
            view_id TEXT PRIMARY KEY,
            schema_name TEXT NOT NULL,
            view_name TEXT NOT NULL,
            view_level INTEGER NOT NULL,
            description TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(schema_name, view_name)
        )
    """)
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS view_dependencies (
            dependency_id TEXT PRIMARY KEY,
            view_id TEXT NOT NULL,
            depends_on_schema TEXT NOT NULL,
            depends_on_object TEXT NOT NULL,
            depends_on_type TEXT NOT NULL CHECK (depends_on_type IN ('table', 'view')),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (view_id) REFERENCES data_views(view_id)
        )
    """)
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS data_quality_checks (
            check_id TEXT PRIMARY KEY,
            schema_name TEXT NOT NULL,
            object_name TEXT NOT NULL,
            object_type TEXT NOT NULL,
            check_type TEXT NOT NULL,
            check_result TEXT,
            check_value REAL,
            threshold_value REAL,
            status TEXT NOT NULL,
            check_timestamp TIMESTAMP NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    conn.commit()
    print("Tables created successfully!")
    
    # Generate mock data
    print(f"Generating mock data for {num_customers} customers...")
    
    # Generate customers
    segments = ['high_value', 'growth', 'maintain', 'at_risk', 'new']
    first_names = ['John', 'Jane', 'Michael', 'Sarah', 'David', 'Emma', 'James', 'Lisa']
    last_names = ['Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia', 'Miller']
    
    customers = []
    for i in range(num_customers):
        customer_id = f"C{i+1:06d}"
        name = f"{random.choice(first_names)} {random.choice(last_names)}"
        email = f"{name.lower().replace(' ', '.')}{i}@email.com"
        phone = f"+1{random.randint(2000000000, 9999999999)}"
        segment = random.choice(segments)
        join_date = datetime.now() - timedelta(days=random.randint(30, 1825))
        credit_score = random.randint(580, 850)
        annual_income = random.randint(30000, 200000)
        employment_status = random.choice(['employed', 'self_employed', 'retired'])
        
        cursor.execute("""
            INSERT OR REPLACE INTO customers (customer_id, name, email, phone, segment, 
                                 join_date, credit_score, annual_income, employment_status)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (customer_id, name, email, phone, segment, join_date.date(), 
              credit_score, annual_income, employment_status))
        
        customers.append({
            'customer_id': customer_id,
            'credit_score': credit_score,
            'annual_income': annual_income
        })
    
    # Generate loans
    loan_count = 0
    for customer in customers:
        if random.random() < 0.6:  # 60% have loans
            loan_id = f"L{loan_count+1:06d}"
            loan_type = random.choice(['mortgage', 'auto', 'personal'])
            
            if loan_type == 'mortgage':
                amount = random.randint(100000, 500000)
                interest_rate = random.uniform(3.0, 6.0)
                term_months = 360
            elif loan_type == 'auto':
                amount = random.randint(10000, 50000)
                interest_rate = random.uniform(4.0, 8.0)
                term_months = 60
            else:
                amount = random.randint(1000, 25000)
                interest_rate = random.uniform(8.0, 15.0)
                term_months = 36
            
            monthly_payment = amount * (interest_rate/100/12) / (1 - (1 + interest_rate/100/12)**(-term_months))
            origination_date = datetime.now() - timedelta(days=random.randint(30, 365))
            maturity_date = origination_date + timedelta(days=term_months * 30)
            status = random.choice(['current', 'current', 'current', 'late'])
            remaining_balance = amount * 0.8
            
            cursor.execute("""
                INSERT OR REPLACE INTO loans (loan_id, customer_id, loan_type, amount, interest_rate,
                                 term_months, monthly_payment, origination_date, maturity_date,
                                 status, remaining_balance, date)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (loan_id, customer['customer_id'], loan_type, amount, interest_rate,
                  term_months, monthly_payment, origination_date.date(), maturity_date.date(),
                  status, remaining_balance, origination_date.date()))
            
            loan_count += 1
    
    # Generate deposits
    account_count = 0
    for customer in customers:
        if random.random() < 0.9:  # 90% have deposits
            num_accounts = random.randint(1, 2)
            for _ in range(num_accounts):
                account_id = f"A{account_count+1:06d}"
                account_type = random.choice(['checking', 'savings'])
                balance = random.randint(100, 50000)
                interest_rate = 0.01 if account_type == 'checking' else random.uniform(0.5, 2.5)
                opened_date = datetime.now() - timedelta(days=random.randint(30, 730))
                
                cursor.execute("""
                    INSERT OR REPLACE INTO deposits (account_id, customer_id, account_type, balance,
                                        interest_rate, opened_date, date)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (account_id, customer['customer_id'], account_type, balance,
                      interest_rate, opened_date.date(), opened_date.date()))
                
                account_count += 1
    
    # Create simple views
    print("Creating views...")
    
    # Drop existing views
    cursor.execute("DROP VIEW IF EXISTS v_customer_summary")
    cursor.execute("DROP VIEW IF EXISTS v_loan_portfolio")
    cursor.execute("DROP VIEW IF EXISTS v_deposit_summary")
    
    # Customer summary view
    cursor.execute("""
        CREATE VIEW v_customer_summary AS
        SELECT 
            c.customer_id,
            c.name,
            c.segment,
            c.credit_score,
            COUNT(DISTINCT l.loan_id) as loan_count,
            COUNT(DISTINCT d.account_id) as account_count,
            COALESCE(SUM(l.remaining_balance), 0) as total_loan_balance,
            COALESCE(SUM(d.balance), 0) as total_deposit_balance
        FROM customers c
        LEFT JOIN loans l ON c.customer_id = l.customer_id
        LEFT JOIN deposits d ON c.customer_id = d.customer_id
        GROUP BY c.customer_id, c.name, c.segment, c.credit_score
    """)
    
    # Loan portfolio view
    cursor.execute("""
        CREATE VIEW v_loan_portfolio AS
        SELECT 
            loan_type,
            COUNT(*) as loan_count,
            SUM(amount) as total_originated,
            SUM(remaining_balance) as total_outstanding,
            AVG(interest_rate) as avg_interest_rate
        FROM loans
        GROUP BY loan_type
    """)
    
    # Deposit summary view
    cursor.execute("""
        CREATE VIEW v_deposit_summary AS
        SELECT 
            account_type,
            COUNT(*) as account_count,
            SUM(balance) as total_balance,
            AVG(balance) as avg_balance
        FROM deposits
        WHERE status = 'active'
        GROUP BY account_type
    """)
    
    # Add some job data
    jobs = [
        ('J001', 'LoadCustomerData', 'Loads customer data from CSV files', 'ETL', 'Daily'),
        ('J002', 'LoadLoanData', 'Loads loan data from CSV files', 'ETL', 'Daily'),
        ('J003', 'LoadDepositData', 'Loads deposit data from CSV files', 'ETL', 'Daily'),
        ('J004', 'LoadTransactionData', 'Loads transaction data from CSV files', 'ETL', 'Hourly')
    ]
    
    for job in jobs:
        cursor.execute("""
            INSERT OR IGNORE INTO jobs (job_id, job_name, job_description, job_type, schedule)
            VALUES (?, ?, ?, ?, ?)
        """, job)
    
    conn.commit()
    
    print(f"Generated {num_customers} customers")
    print(f"Generated {loan_count} loans")
    print(f"Generated {account_count} deposit accounts")
    print("Mock data generation complete!")
    
    # Verify data
    cursor.execute("SELECT COUNT(*) FROM customers")
    print(f"\nVerification - Customers: {cursor.fetchone()[0]}")
    
    cursor.execute("SELECT COUNT(*) FROM loans")
    print(f"Verification - Loans: {cursor.fetchone()[0]}")
    
    cursor.execute("SELECT COUNT(*) FROM deposits")
    print(f"Verification - Deposits: {cursor.fetchone()[0]}")
    
    conn.close()
    print("\nDatabase setup complete!")


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Minimal database setup")
    parser.add_argument('--customers', type=int, default=100,
                      help='Number of customers to generate (default: 100)')
    parser.add_argument('--db-path', type=str, default='data/banking.db',
                      help='Database file path (default: data/banking.db)')
    
    args = parser.parse_args()
    
    try:
        create_database(args.db_path, args.customers)
    except Exception as e:
        print(f"Error: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)