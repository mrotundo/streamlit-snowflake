#!/usr/bin/env python3
"""
Database setup script without pandas dependency
Creates all tables and generates mock data for local SQLite database
This is a workaround for the pandas segmentation fault issue
"""

import os
import sys
import sqlite3
import random
from datetime import datetime, timedelta
import uuid

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

class DatabaseSetupWorkaround:
    """Handles database setup without pandas"""
    
    def __init__(self):
        self.db_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'data', 'banking.db')
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        self.connection = None
    
    def connect(self):
        """Connect to SQLite database"""
        self.connection = sqlite3.connect(self.db_path)
        return self.connection
    
    def disconnect(self):
        """Disconnect from database"""
        if self.connection:
            self.connection.close()
            self.connection = None
    
    def create_tables(self, drop_existing=False):
        """Create all necessary tables"""
        cursor = self.connection.cursor()
        
        if drop_existing:
            print("Dropping existing tables...")
            # Drop views first
            views = [
                'v_executive_dashboard', 'v_risk_analytics', 'v_customer_risk_profile',
                'v_product_performance', 'v_customer_lifetime_value', 'v_customer_summary',
                'v_loan_portfolio', 'v_deposit_summary', 'v_customer_products'
            ]
            for view in views:
                cursor.execute(f"DROP VIEW IF EXISTS {view}")
            
            # Drop tables
            tables = [
                'data_quality_checks', 'view_dependencies', 'data_views',
                'job_run_target_tables', 'job_run_source_files', 'source_files',
                'job_runs', 'jobs', 'transactions', 'loans', 'deposits', 'customers'
            ]
            for table in tables:
                cursor.execute(f"DROP TABLE IF EXISTS {table}")
        
        print("Creating tables...")
        
        # Create customers table
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
        
        # Create loans table
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
        
        # Create deposits table
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
        
        # Create transactions table
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
        
        # Create data lineage tables
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
        
        # Create indexes
        indexes = [
            "CREATE INDEX IF NOT EXISTS idx_loans_customer ON loans(customer_id)",
            "CREATE INDEX IF NOT EXISTS idx_loans_status ON loans(status)",
            "CREATE INDEX IF NOT EXISTS idx_deposits_customer ON deposits(customer_id)",
            "CREATE INDEX IF NOT EXISTS idx_transactions_customer ON transactions(customer_id)",
            "CREATE INDEX IF NOT EXISTS idx_transactions_date ON transactions(transaction_date)"
        ]
        
        for index in indexes:
            cursor.execute(index)
        
        self.connection.commit()
        print("Tables created successfully!")
    
    def generate_mock_data(self, num_customers=1000):
        """Generate mock data for all tables"""
        print(f"Generating mock data for {num_customers} customers...")
        
        cursor = self.connection.cursor()
        
        # Generate customers
        print("Generating customers...")
        customers = self._generate_customers(num_customers)
        for customer in customers:
            cursor.execute("""
                INSERT OR REPLACE INTO customers 
                (customer_id, name, email, phone, segment, join_date, credit_score, 
                 annual_income, employment_status, products_count, total_relationship_value, status)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                customer['customer_id'], customer['name'], customer['email'], 
                customer['phone'], customer['segment'], customer['join_date'],
                customer['credit_score'], customer['annual_income'], 
                customer['employment_status'], 0, 0, customer['status']
            ))
        
        self.connection.commit()
        
        # Generate loans and deposits
        print("Generating loans and deposits...")
        total_loans = 0
        total_deposits = 0
        
        for customer in customers:
            # Generate deposits
            if random.random() < 0.9:  # 90% have deposits
                num_accounts = random.randint(1, 3)
                for _ in range(num_accounts):
                    deposit = self._generate_deposit(customer)
                    cursor.execute("""
                        INSERT OR REPLACE INTO deposits
                        (account_id, customer_id, account_type, balance, interest_rate,
                         opened_date, last_transaction_date, status, minimum_balance, 
                         overdraft_limit, date)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        deposit['account_id'], deposit['customer_id'], 
                        deposit['account_type'], deposit['balance'],
                        deposit['interest_rate'], deposit['opened_date'],
                        deposit['last_transaction_date'], deposit['status'],
                        deposit['minimum_balance'], deposit['overdraft_limit'],
                        deposit['date']
                    ))
                    total_deposits += 1
            
            # Generate loans
            if random.random() < 0.6:  # 60% have loans
                num_loans = random.randint(1, 2)
                for _ in range(num_loans):
                    loan = self._generate_loan(customer)
                    cursor.execute("""
                        INSERT OR REPLACE INTO loans
                        (loan_id, customer_id, loan_type, amount, interest_rate,
                         term_months, monthly_payment, origination_date, maturity_date,
                         status, remaining_balance, paid_amount, late_fees,
                         last_payment_date, next_payment_date, date)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        loan['loan_id'], loan['customer_id'], loan['loan_type'],
                        loan['amount'], loan['interest_rate'], loan['term_months'],
                        loan['monthly_payment'], loan['origination_date'],
                        loan['maturity_date'], loan['status'], loan['remaining_balance'],
                        loan['paid_amount'], loan['late_fees'], loan['last_payment_date'],
                        loan['next_payment_date'], loan['date']
                    ))
                    total_loans += 1
        
        self.connection.commit()
        
        print(f"Generated {num_customers} customers")
        print(f"Generated {total_loans} loans")
        print(f"Generated {total_deposits} deposit accounts")
        
        # Create views
        self.create_views()
        
        # Generate lineage data
        self.generate_lineage_data()
        
        print("Mock data generation complete!")
    
    def _generate_customers(self, count):
        """Generate customer data"""
        customers = []
        segments = ['high_value', 'growth', 'maintain', 'at_risk', 'new']
        employment_statuses = ['employed', 'self_employed', 'retired', 'student']
        
        first_names = ['John', 'Jane', 'Michael', 'Sarah', 'David', 'Emma', 'James', 'Lisa']
        last_names = ['Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia', 'Miller']
        
        for i in range(count):
            customer_id = f"C{i+1:06d}"
            first_name = random.choice(first_names)
            last_name = random.choice(last_names)
            name = f"{first_name} {last_name}"
            email = f"{first_name.lower()}.{last_name.lower()}{i}@email.com"
            
            segment = random.choice(segments)
            
            # Set attributes based on segment
            if segment == 'high_value':
                credit_score = random.randint(720, 850)
                annual_income = random.randint(100000, 500000)
                join_days_ago = random.randint(730, 3650)
            elif segment == 'growth':
                credit_score = random.randint(680, 780)
                annual_income = random.randint(60000, 150000)
                join_days_ago = random.randint(180, 730)
            else:
                credit_score = random.randint(600, 750)
                annual_income = random.randint(35000, 120000)
                join_days_ago = random.randint(0, 365)
            
            join_date = (datetime.now() - timedelta(days=join_days_ago)).date()
            
            customers.append({
                'customer_id': customer_id,
                'name': name,
                'email': email,
                'phone': f"+1{random.randint(2000000000, 9999999999)}",
                'segment': segment,
                'join_date': join_date,
                'credit_score': credit_score,
                'annual_income': annual_income,
                'employment_status': random.choice(employment_statuses),
                'status': 'active' if random.random() > 0.05 else 'inactive'
            })
        
        return customers
    
    def _generate_deposit(self, customer):
        """Generate a deposit account"""
        account_types = ['checking', 'savings', 'cd', 'money_market']
        account_type = random.choice(account_types)
        account_id = f"A{customer['customer_id'][1:]}{random.randint(1, 99):02d}"
        
        # Set balance based on account type
        if account_type == 'checking':
            balance = round(random.uniform(100, 10000), 2)
            interest_rate = 0.01
            minimum_balance = 100
            overdraft_limit = 500
        elif account_type == 'savings':
            balance = round(random.uniform(500, 50000), 2)
            interest_rate = round(random.uniform(0.5, 2.5), 2)
            minimum_balance = 300
            overdraft_limit = 0
        elif account_type == 'cd':
            balance = round(random.uniform(1000, 100000), 2)
            interest_rate = round(random.uniform(3.0, 5.0), 2)
            minimum_balance = balance
            overdraft_limit = 0
        else:  # money_market
            balance = round(random.uniform(2500, 75000), 2)
            interest_rate = round(random.uniform(2.0, 4.0), 2)
            minimum_balance = 2500
            overdraft_limit = 0
        
        days_after_join = random.randint(0, (datetime.now().date() - customer['join_date']).days)
        opened_date = customer['join_date'] + timedelta(days=days_after_join)
        
        return {
            'account_id': account_id,
            'customer_id': customer['customer_id'],
            'account_type': account_type,
            'balance': balance,
            'interest_rate': interest_rate,
            'opened_date': opened_date,
            'last_transaction_date': opened_date + timedelta(days=random.randint(0, 30)),
            'status': 'active',
            'minimum_balance': minimum_balance,
            'overdraft_limit': overdraft_limit,
            'date': opened_date
        }
    
    def _generate_loan(self, customer):
        """Generate a loan"""
        loan_types = ['mortgage', 'auto', 'personal', 'business']
        loan_type = random.choice(loan_types)
        loan_id = f"L{customer['customer_id'][1:]}{random.randint(1, 99):02d}"
        
        # Set loan parameters based on type
        if loan_type == 'mortgage':
            amount = round(random.uniform(100000, 500000), 2)
            interest_rate = round(random.uniform(3.0, 6.0), 2)
            term_months = random.choice([180, 240, 360])
        elif loan_type == 'auto':
            amount = round(random.uniform(10000, 50000), 2)
            interest_rate = round(random.uniform(4.0, 10.0), 2)
            term_months = random.choice([36, 48, 60, 72])
        elif loan_type == 'personal':
            amount = round(random.uniform(1000, 25000), 2)
            interest_rate = round(random.uniform(8.0, 20.0), 2)
            term_months = random.choice([12, 24, 36, 48, 60])
        else:  # business
            amount = round(random.uniform(25000, 200000), 2)
            interest_rate = round(random.uniform(6.0, 15.0), 2)
            term_months = random.choice([36, 60, 84, 120])
        
        # Calculate monthly payment
        monthly_rate = interest_rate / 100 / 12
        if monthly_rate > 0:
            monthly_payment = amount * (monthly_rate * (1 + monthly_rate)**term_months) / ((1 + monthly_rate)**term_months - 1)
        else:
            monthly_payment = amount / term_months
        monthly_payment = round(monthly_payment, 2)
        
        # Set dates
        days_since_join = (datetime.now().date() - customer['join_date']).days
        if days_since_join < 30:
            days_after_join = random.randint(0, max(1, days_since_join))
        else:
            days_after_join = random.randint(30, days_since_join)
        origination_date = customer['join_date'] + timedelta(days=days_after_join)
        maturity_date = origination_date + timedelta(days=term_months * 30)
        
        # Set status
        if customer['segment'] == 'at_risk':
            status = random.choice(['current'] * 60 + ['late'] * 30 + ['default'] * 10)
        else:
            status = random.choice(['current'] * 85 + ['late'] * 10 + ['paid_off'] * 4 + ['default'] * 1)
        
        # Calculate balances
        months_elapsed = min(term_months, (datetime.now().date() - origination_date).days // 30)
        if status == 'paid_off':
            remaining_balance = 0
            paid_amount = amount
        else:
            paid_amount = monthly_payment * months_elapsed
            remaining_balance = round(max(0, amount - paid_amount), 2)
        
        return {
            'loan_id': loan_id,
            'customer_id': customer['customer_id'],
            'loan_type': loan_type,
            'amount': amount,
            'interest_rate': interest_rate,
            'term_months': term_months,
            'monthly_payment': monthly_payment,
            'origination_date': origination_date,
            'maturity_date': maturity_date,
            'status': status,
            'remaining_balance': remaining_balance,
            'paid_amount': round(paid_amount, 2),
            'late_fees': round(random.uniform(0, 500), 2) if status == 'late' else 0,
            'last_payment_date': origination_date + timedelta(days=(months_elapsed - 1) * 30) if months_elapsed > 0 else None,
            'next_payment_date': origination_date + timedelta(days=months_elapsed * 30) if status in ['current', 'late'] else None,
            'date': origination_date
        }
    
    def create_views(self):
        """Create database views"""
        print("Creating views...")
        cursor = self.connection.cursor()
        
        # Level 1 Views
        cursor.execute("""
            CREATE VIEW IF NOT EXISTS v_customer_summary AS
            SELECT 
                c.customer_id,
                c.name,
                c.segment,
                c.credit_score,
                c.annual_income,
                c.join_date,
                COUNT(DISTINCT d.account_id) as num_accounts,
                COUNT(DISTINCT l.loan_id) as num_loans,
                COALESCE(SUM(d.balance), 0) as total_deposits,
                COALESCE(SUM(l.remaining_balance), 0) as total_loan_balance
            FROM customers c
            LEFT JOIN deposits d ON c.customer_id = d.customer_id AND d.status = 'active'
            LEFT JOIN loans l ON c.customer_id = l.customer_id AND l.status IN ('current', 'late')
            GROUP BY c.customer_id, c.name, c.segment, c.credit_score, c.annual_income, c.join_date
        """)
        
        cursor.execute("""
            CREATE VIEW IF NOT EXISTS v_loan_portfolio AS
            SELECT 
                l.loan_type,
                l.status,
                COUNT(*) as loan_count,
                SUM(l.amount) as total_originated,
                SUM(l.remaining_balance) as total_outstanding,
                AVG(l.interest_rate) as avg_interest_rate,
                SUM(CASE WHEN l.status = 'default' THEN l.remaining_balance ELSE 0 END) as default_amount,
                COUNT(CASE WHEN l.status = 'default' THEN 1 END) * 100.0 / COUNT(*) as default_rate
            FROM loans l
            GROUP BY l.loan_type, l.status
        """)
        
        cursor.execute("""
            CREATE VIEW IF NOT EXISTS v_deposit_summary AS
            SELECT 
                d.account_type,
                COUNT(*) as account_count,
                SUM(d.balance) as total_balance,
                AVG(d.balance) as avg_balance,
                MIN(d.balance) as min_balance,
                MAX(d.balance) as max_balance,
                AVG(d.interest_rate) as avg_interest_rate
            FROM deposits d
            WHERE d.status = 'active'
            GROUP BY d.account_type
        """)
        
        cursor.execute("""
            CREATE VIEW IF NOT EXISTS v_customer_products AS
            SELECT 
                c.customer_id,
                c.name,
                c.segment,
                COUNT(DISTINCT d.account_id) as deposit_accounts,
                COUNT(DISTINCT l.loan_id) as loan_accounts,
                COUNT(DISTINCT d.account_id) + COUNT(DISTINCT l.loan_id) as total_products,
                MAX(d.opened_date) as last_account_opened,
                MAX(l.origination_date) as last_loan_originated
            FROM customers c
            LEFT JOIN deposits d ON c.customer_id = d.customer_id
            LEFT JOIN loans l ON c.customer_id = l.customer_id
            GROUP BY c.customer_id, c.name, c.segment
        """)
        
        self.connection.commit()
        print("Views created successfully!")
    
    def generate_lineage_data(self):
        """Generate data lineage information"""
        print("Generating lineage data...")
        cursor = self.connection.cursor()
        
        # Insert job definitions
        jobs = [
            ('J001', 'LoadCustomerData', 'Loads customer data from CSV files', 'ETL', 'Daily at 2:00 AM'),
            ('J002', 'LoadLoanData', 'Loads loan data from CSV files', 'ETL', 'Daily at 2:30 AM'),
            ('J003', 'LoadDepositData', 'Loads deposit account data from CSV files', 'ETL', 'Daily at 3:00 AM'),
            ('J004', 'LoadTransactionData', 'Loads transaction data from CSV files', 'ETL', 'Hourly')
        ]
        
        for job in jobs:
            cursor.execute(
                "INSERT OR REPLACE INTO jobs (job_id, job_name, job_description, job_type, schedule) VALUES (?, ?, ?, ?, ?)",
                job
            )
        
        self.connection.commit()
        print("Lineage data generated successfully!")
    
    def verify_data(self):
        """Verify data was created correctly"""
        print("\nVerifying data...")
        cursor = self.connection.cursor()
        
        tables = ['customers', 'loans', 'deposits', 'transactions', 'jobs']
        
        for table in tables:
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            count = cursor.fetchone()[0]
            print(f"{table}: {count} records")
        
        # Verify views
        print("\nVerifying views...")
        views = ['v_customer_summary', 'v_loan_portfolio', 'v_deposit_summary']
        
        for view in views:
            try:
                cursor.execute(f"SELECT COUNT(*) FROM {view}")
                count = cursor.fetchone()[0]
                print(f"{view}: {count} records")
            except Exception as e:
                print(f"{view}: Error - {str(e)}")


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Set up banking database without pandas")
    parser.add_argument('--customers', type=int, default=1000,
                        help='Number of customers to generate (default: 1000)')
    parser.add_argument('--drop-existing', action='store_true',
                        help='Drop existing tables before creating new ones')
    parser.add_argument('--verify-only', action='store_true',
                        help='Only verify existing data, no creation')
    
    args = parser.parse_args()
    
    setup = DatabaseSetupWorkaround()
    
    try:
        setup.connect()
        print(f"Connected to database: {setup.db_path}")
        
        if args.verify_only:
            setup.verify_data()
        else:
            # Create tables
            setup.create_tables(drop_existing=args.drop_existing)
            
            # Generate mock data
            setup.generate_mock_data(args.customers)
            
            # Verify the setup
            setup.verify_data()
        
    except Exception as e:
        print(f"Error: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        setup.disconnect()
        print("\nDatabase connection closed.")