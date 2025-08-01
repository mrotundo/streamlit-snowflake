#!/usr/bin/env python3
"""
Database setup script for Banking AI Assistant
Creates tables and generates mock data for both local SQLite and Snowflake databases
"""

import os
import sys
import argparse
import random
from datetime import datetime, timedelta
from typing import List, Dict, Any

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.settings import Settings
from services.data_factory import DataServiceFactory
from services.local_data_service import LocalDataService
from services.snowflake_data_service import SnowflakeDataService


class DatabaseSetup:
    """Handles database setup and data generation"""
    
    def __init__(self, provider: str = 'local', drop_existing: bool = False):
        self.provider = provider
        self.data_service = DataServiceFactory.create_data_service(provider)
        self.transaction_counter = 0  # Global counter for unique transaction IDs
        self.drop_existing = drop_existing
        
    def create_tables(self):
        """Create all necessary tables"""
        print(f"Creating tables for {self.provider} database...")
        
        if isinstance(self.data_service, LocalDataService):
            self._create_sqlite_tables()
        elif isinstance(self.data_service, SnowflakeDataService):
            self._create_snowflake_tables()
    
    def _create_sqlite_tables(self):
        """Create tables for SQLite"""
        if not self.data_service.connection:
            self.data_service.connect()
        
        cursor = self.data_service.connection.cursor()
        print("  Creating SQLite tables...")
        
        # Drop existing tables if requested
        if self.drop_existing:
            print("  - Dropping existing tables...")
            # Drop views first
            cursor.execute("DROP VIEW IF EXISTS v_executive_dashboard")
            cursor.execute("DROP VIEW IF EXISTS v_risk_analytics")
            cursor.execute("DROP VIEW IF EXISTS v_customer_risk_profile")
            cursor.execute("DROP VIEW IF EXISTS v_product_performance")
            cursor.execute("DROP VIEW IF EXISTS v_customer_lifetime_value")
            cursor.execute("DROP VIEW IF EXISTS v_customer_summary")
            cursor.execute("DROP VIEW IF EXISTS v_loan_portfolio")
            cursor.execute("DROP VIEW IF EXISTS v_deposit_summary")
            cursor.execute("DROP VIEW IF EXISTS v_customer_products")
            # Drop lineage tables
            cursor.execute("DROP TABLE IF EXISTS data_quality_checks")
            cursor.execute("DROP TABLE IF EXISTS view_dependencies")
            cursor.execute("DROP TABLE IF EXISTS data_views")
            cursor.execute("DROP TABLE IF EXISTS job_run_target_tables")
            cursor.execute("DROP TABLE IF EXISTS job_run_source_files")
            cursor.execute("DROP TABLE IF EXISTS source_files")
            cursor.execute("DROP TABLE IF EXISTS job_runs")
            cursor.execute("DROP TABLE IF EXISTS jobs")
            # Drop business tables
            cursor.execute("DROP TABLE IF EXISTS transactions")
            cursor.execute("DROP TABLE IF EXISTS loans")
            cursor.execute("DROP TABLE IF EXISTS deposits")
            cursor.execute("DROP TABLE IF EXISTS customers")
        
        # Create customers table
        print("  - Creating customers table...")
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
        print("  - Creating loans table...")
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
        print("  - Creating deposits table...")
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
        print("  - Creating transactions table...")
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
        
        # Create indexes for better query performance
        print("  - Creating indexes...")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_loans_customer ON loans(customer_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_loans_status ON loans(status)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_loans_type ON loans(loan_type)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_deposits_customer ON deposits(customer_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_deposits_type ON deposits(account_type)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_transactions_customer ON transactions(customer_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_transactions_date ON transactions(transaction_date)")
        
        # Create data lineage tables
        print("  - Creating lineage tables...")
        
        # Jobs table
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
        
        # Job runs table
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
        
        # Source files table
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
        
        # Job run source files junction table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS job_run_source_files (
                job_run_id TEXT NOT NULL,
                file_id TEXT NOT NULL,
                PRIMARY KEY (job_run_id, file_id),
                FOREIGN KEY (job_run_id) REFERENCES job_runs(job_run_id),
                FOREIGN KEY (file_id) REFERENCES source_files(file_id)
            )
        """)
        
        # Job run target tables junction table
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
        
        # Data views table
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
        
        # View dependencies table
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
        
        # Data quality checks table
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
        
        # Create indexes for lineage tables
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_job_runs_job ON job_runs(job_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_job_runs_status ON job_runs(status)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_job_runs_time ON job_runs(start_time)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_view_deps_view ON view_dependencies(view_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_view_deps_object ON view_dependencies(depends_on_object)")
        
        self.data_service.connection.commit()
        print("  - All tables created successfully!")
    
    def _create_snowflake_tables(self):
        """Create tables for Snowflake"""
        if not self.data_service.connection:
            self.data_service.connect()
        
        cursor = self.data_service.connection.cursor()
        
        # Drop existing tables if requested
        if self.drop_existing:
            print("  - Dropping existing tables...")
            # Drop views first
            cursor.execute("DROP VIEW IF EXISTS V_EXECUTIVE_DASHBOARD")
            cursor.execute("DROP VIEW IF EXISTS V_RISK_ANALYTICS")
            cursor.execute("DROP VIEW IF EXISTS V_CUSTOMER_RISK_PROFILE")
            cursor.execute("DROP VIEW IF EXISTS V_PRODUCT_PERFORMANCE")
            cursor.execute("DROP VIEW IF EXISTS V_CUSTOMER_LIFETIME_VALUE")
            cursor.execute("DROP VIEW IF EXISTS V_CUSTOMER_SUMMARY")
            cursor.execute("DROP VIEW IF EXISTS V_LOAN_PORTFOLIO")
            cursor.execute("DROP VIEW IF EXISTS V_DEPOSIT_SUMMARY")
            cursor.execute("DROP VIEW IF EXISTS V_CUSTOMER_PRODUCTS")
            # Drop lineage tables
            cursor.execute("DROP TABLE IF EXISTS DATA_QUALITY_CHECKS")
            cursor.execute("DROP TABLE IF EXISTS VIEW_DEPENDENCIES")
            cursor.execute("DROP TABLE IF EXISTS DATA_VIEWS")
            cursor.execute("DROP TABLE IF EXISTS JOB_RUN_TARGET_TABLES")
            cursor.execute("DROP TABLE IF EXISTS JOB_RUN_SOURCE_FILES")
            cursor.execute("DROP TABLE IF EXISTS SOURCE_FILES")
            cursor.execute("DROP TABLE IF EXISTS JOB_RUNS")
            cursor.execute("DROP TABLE IF EXISTS JOBS")
            # Drop business tables
            cursor.execute("DROP TABLE IF EXISTS TRANSACTIONS")
            cursor.execute("DROP TABLE IF EXISTS LOANS")
            cursor.execute("DROP TABLE IF EXISTS DEPOSITS")
            cursor.execute("DROP TABLE IF EXISTS CUSTOMERS")
        
        # Create customers table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS CUSTOMERS (
                customer_id VARCHAR(20) PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                email VARCHAR(100) UNIQUE,
                phone VARCHAR(20),
                segment VARCHAR(20),
                join_date DATE,
                credit_score NUMBER,
                annual_income NUMBER(12,2),
                employment_status VARCHAR(50),
                products_count NUMBER DEFAULT 0,
                total_relationship_value NUMBER(12,2) DEFAULT 0,
                status VARCHAR(20) DEFAULT 'active',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
            )
        """)
        
        # Create loans table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS LOANS (
                loan_id VARCHAR(20) PRIMARY KEY,
                customer_id VARCHAR(20) NOT NULL,
                loan_type VARCHAR(20) NOT NULL,
                amount NUMBER(12,2) NOT NULL,
                interest_rate NUMBER(5,2) NOT NULL,
                term_months NUMBER NOT NULL,
                monthly_payment NUMBER(12,2) NOT NULL,
                origination_date DATE NOT NULL,
                maturity_date DATE NOT NULL,
                status VARCHAR(20) DEFAULT 'current',
                remaining_balance NUMBER(12,2) NOT NULL,
                paid_amount NUMBER(12,2) DEFAULT 0,
                late_fees NUMBER(12,2) DEFAULT 0,
                last_payment_date DATE,
                next_payment_date DATE,
                date DATE NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
                FOREIGN KEY (customer_id) REFERENCES CUSTOMERS(customer_id)
            )
        """)
        
        # Create deposits table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS DEPOSITS (
                account_id VARCHAR(20) PRIMARY KEY,
                customer_id VARCHAR(20) NOT NULL,
                account_type VARCHAR(20) NOT NULL,
                balance NUMBER(12,2) NOT NULL DEFAULT 0,
                interest_rate NUMBER(5,2) DEFAULT 0,
                opened_date DATE NOT NULL,
                last_transaction_date DATE,
                status VARCHAR(20) DEFAULT 'active',
                minimum_balance NUMBER(12,2) DEFAULT 0,
                overdraft_limit NUMBER(12,2) DEFAULT 0,
                date DATE NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
                FOREIGN KEY (customer_id) REFERENCES CUSTOMERS(customer_id)
            )
        """)
        
        # Create transactions table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS TRANSACTIONS (
                transaction_id VARCHAR(20) PRIMARY KEY,
                account_id VARCHAR(20),
                loan_id VARCHAR(20),
                customer_id VARCHAR(20) NOT NULL,
                transaction_type VARCHAR(50) NOT NULL,
                amount NUMBER(12,2) NOT NULL,
                balance_after NUMBER(12,2),
                description VARCHAR(200),
                category VARCHAR(50),
                transaction_date TIMESTAMP NOT NULL,
                posted_date TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
                FOREIGN KEY (account_id) REFERENCES DEPOSITS(account_id),
                FOREIGN KEY (loan_id) REFERENCES LOANS(loan_id),
                FOREIGN KEY (customer_id) REFERENCES CUSTOMERS(customer_id)
            )
        """)
        
        # Create data lineage tables for Snowflake
        
        # Jobs table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS JOBS (
                job_id VARCHAR(50) PRIMARY KEY,
                job_name VARCHAR(100) NOT NULL UNIQUE,
                job_description TEXT,
                job_type VARCHAR(50) NOT NULL,
                schedule VARCHAR(100),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
            )
        """)
        
        # Job runs table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS JOB_RUNS (
                job_run_id VARCHAR(50) PRIMARY KEY,
                job_id VARCHAR(50) NOT NULL,
                start_time TIMESTAMP NOT NULL,
                end_time TIMESTAMP,
                status VARCHAR(20) NOT NULL,
                rows_processed NUMBER DEFAULT 0,
                error_message TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
                FOREIGN KEY (job_id) REFERENCES JOBS(job_id)
            )
        """)
        
        # Source files table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS SOURCE_FILES (
                file_id VARCHAR(50) PRIMARY KEY,
                file_name VARCHAR(200) NOT NULL,
                file_path VARCHAR(500) NOT NULL,
                file_type VARCHAR(20),
                file_size NUMBER,
                file_hash VARCHAR(100),
                arrival_time TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
            )
        """)
        
        # Job run source files junction table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS JOB_RUN_SOURCE_FILES (
                job_run_id VARCHAR(50) NOT NULL,
                file_id VARCHAR(50) NOT NULL,
                PRIMARY KEY (job_run_id, file_id),
                FOREIGN KEY (job_run_id) REFERENCES JOB_RUNS(job_run_id),
                FOREIGN KEY (file_id) REFERENCES SOURCE_FILES(file_id)
            )
        """)
        
        # Job run target tables junction table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS JOB_RUN_TARGET_TABLES (
                job_run_id VARCHAR(50) NOT NULL,
                schema_name VARCHAR(50) NOT NULL,
                table_name VARCHAR(50) NOT NULL,
                rows_inserted NUMBER DEFAULT 0,
                rows_updated NUMBER DEFAULT 0,
                rows_deleted NUMBER DEFAULT 0,
                PRIMARY KEY (job_run_id, schema_name, table_name),
                FOREIGN KEY (job_run_id) REFERENCES JOB_RUNS(job_run_id)
            )
        """)
        
        # Data views table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS DATA_VIEWS (
                view_id VARCHAR(50) PRIMARY KEY,
                schema_name VARCHAR(50) NOT NULL,
                view_name VARCHAR(100) NOT NULL,
                view_level NUMBER NOT NULL,
                description TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
                UNIQUE(schema_name, view_name)
            )
        """)
        
        # View dependencies table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS VIEW_DEPENDENCIES (
                dependency_id VARCHAR(50) PRIMARY KEY,
                view_id VARCHAR(50) NOT NULL,
                depends_on_schema VARCHAR(50) NOT NULL,
                depends_on_object VARCHAR(100) NOT NULL,
                depends_on_type VARCHAR(10) NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
                FOREIGN KEY (view_id) REFERENCES DATA_VIEWS(view_id),
                CONSTRAINT chk_dep_type CHECK (depends_on_type IN ('table', 'view'))
            )
        """)
        
        # Data quality checks table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS DATA_QUALITY_CHECKS (
                check_id VARCHAR(50) PRIMARY KEY,
                schema_name VARCHAR(50) NOT NULL,
                object_name VARCHAR(100) NOT NULL,
                object_type VARCHAR(20) NOT NULL,
                check_type VARCHAR(50) NOT NULL,
                check_result TEXT,
                check_value NUMBER(12,2),
                threshold_value NUMBER(12,2),
                status VARCHAR(20) NOT NULL,
                check_timestamp TIMESTAMP NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
            )
        """)
        
        self.data_service.connection.commit()
        print("Tables created successfully!")
    
    def generate_mock_data(self, num_customers: int = 100):
        """Generate mock data for all tables"""
        print(f"\nGenerating mock data for {num_customers} customers...")
        
        if not self.data_service.connection:
            self.data_service.connect()
        
        # Generate customers
        print("  - Generating customer records...")
        customers = self._generate_customers(num_customers)
        print(f"    Generated {len(customers)} customers")
        
        print("  - Inserting customers into database...")
        self._insert_customers(customers)
        print("    Customers inserted successfully")
        
        # Generate accounts and loans for customers
        total_loans = 0
        total_deposits = 0
        total_transactions = 0
        self.transaction_counter = 0  # Reset transaction counter
        
        # Process customers in batches to avoid memory issues
        batch_size = 20
        for batch_start in range(0, len(customers), batch_size):
            batch_end = min(batch_start + batch_size, len(customers))
            customer_batch = customers[batch_start:batch_end]
            
            if batch_start % 100 == 0:
                print(f"  - Processing customers {batch_start+1}-{batch_end}/{len(customers)}...")
            
            for customer in customer_batch:
                # Each customer has 1-3 products (reduced from 1-4)
                num_products = random.randint(1, 3)
            
                # Generate deposit accounts
                if random.random() < 0.9:  # 90% have at least one deposit account
                    num_accounts = random.randint(1, min(3, num_products))
                    accounts = self._generate_deposit_accounts(customer, num_accounts)
                    self._insert_deposits(accounts)
                    total_deposits += len(accounts)
                    
                    # Generate transactions for each account
                    for account in accounts:
                        # Limit transactions per account
                        transactions = self._generate_transactions(customer, account=account, max_transactions=20)
                        if transactions:
                            self._insert_transactions(transactions)
                            total_transactions += len(transactions)
                
                # Generate loans
                if random.random() < 0.6:  # 60% have at least one loan
                    num_loans = random.randint(1, min(2, num_products))
                    loans = self._generate_loans(customer, num_loans)
                    self._insert_loans(loans)
                    total_loans += len(loans)
                    
                    # Generate loan payment transactions
                    for loan in loans:
                        # Limit loan payment transactions
                        transactions = self._generate_transactions(customer, loan=loan, max_transactions=12)
                        if transactions:
                            self._insert_transactions(transactions)
                            total_transactions += len(transactions)
        
        print(f"\n  Summary of generated data:")
        print(f"    - {num_customers} customers")
        print(f"    - {total_loans} loans")
        print(f"    - {total_deposits} deposit accounts")
        print(f"    - {total_transactions} transactions")
        
        # Create views
        print("\n  - Creating database views...")
        self.create_views()
        
        # Generate lineage data
        print("\n  - Generating lineage metadata...")
        self.generate_lineage_data()
        
        print("\nMock data generation complete!")
    
    def _generate_customers(self, count: int) -> List[Dict[str, Any]]:
        """Generate customer data"""
        customers = []
        segments = ['high_value', 'growth', 'maintain', 'at_risk', 'new']
        employment_statuses = ['employed', 'self_employed', 'retired', 'student']
        
        first_names = ['John', 'Jane', 'Michael', 'Sarah', 'David', 'Emma', 'James', 'Lisa', 
                       'Robert', 'Mary', 'William', 'Jennifer', 'Richard', 'Linda', 'Thomas',
                       'Patricia', 'Charles', 'Barbara', 'Christopher', 'Susan']
        last_names = ['Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia', 'Miller',
                      'Davis', 'Rodriguez', 'Martinez', 'Hernandez', 'Lopez', 'Wilson',
                      'Anderson', 'Thomas', 'Taylor', 'Moore', 'Jackson', 'Martin', 'Lee']
        
        for i in range(count):
            customer_id = f"C{i+1:06d}"
            first_name = random.choice(first_names)
            last_name = random.choice(last_names)
            name = f"{first_name} {last_name}"
            email = f"{first_name.lower()}.{last_name.lower()}{i}@email.com"
            
            # Segment affects other attributes
            segment = random.choice(segments)
            
            # High value customers have better stats
            if segment == 'high_value':
                credit_score = random.randint(720, 850)
                annual_income = random.randint(100000, 500000)
                join_days_ago = random.randint(730, 3650)  # 2-10 years
            elif segment == 'growth':
                credit_score = random.randint(680, 780)
                annual_income = random.randint(60000, 150000)
                join_days_ago = random.randint(180, 730)  # 6 months - 2 years
            elif segment == 'maintain':
                credit_score = random.randint(640, 720)
                annual_income = random.randint(40000, 100000)
                join_days_ago = random.randint(365, 1825)  # 1-5 years
            elif segment == 'at_risk':
                credit_score = random.randint(580, 680)
                annual_income = random.randint(30000, 80000)
                join_days_ago = random.randint(90, 1095)  # 3 months - 3 years
            else:  # new
                credit_score = random.randint(600, 750)
                annual_income = random.randint(35000, 120000)
                join_days_ago = random.randint(30, 90)  # 1-3 months (ensures they can have loans)
            
            join_date = datetime.now() - timedelta(days=join_days_ago)
            
            customers.append({
                'customer_id': customer_id,
                'name': name,
                'email': email,
                'phone': f"+1{random.randint(2000000000, 9999999999)}",
                'segment': segment,
                'join_date': join_date.date(),
                'credit_score': credit_score,
                'annual_income': annual_income,
                'employment_status': random.choice(employment_statuses),
                'products_count': 0,  # Will be updated
                'total_relationship_value': 0,  # Will be updated
                'status': 'active' if random.random() > 0.05 else 'inactive'
            })
        
        return customers
    
    def _generate_deposit_accounts(self, customer: Dict[str, Any], count: int) -> List[Dict[str, Any]]:
        """Generate deposit accounts for a customer"""
        accounts = []
        account_types = ['checking', 'savings', 'cd', 'money_market']
        used_types = []
        
        for i in range(count):
            # Ensure variety in account types
            available_types = [t for t in account_types if t not in used_types]
            if not available_types:
                available_types = account_types
                used_types = []  # Reset used types to allow duplicates
            
            account_type = random.choice(available_types)
            used_types.append(account_type)
            
            account_id = f"A{customer['customer_id'][1:]}{i+1:02d}"
            
            # Balance based on account type and customer segment
            base_multiplier = 1.0
            if customer['segment'] == 'high_value':
                base_multiplier = 5.0
            elif customer['segment'] == 'growth':
                base_multiplier = 2.0
            elif customer['segment'] == 'at_risk':
                base_multiplier = 0.5
            
            if account_type == 'checking':
                balance = random.uniform(100, 10000) * base_multiplier
                interest_rate = 0.01
                minimum_balance = 100
                overdraft_limit = 500 * base_multiplier
            elif account_type == 'savings':
                balance = random.uniform(500, 50000) * base_multiplier
                interest_rate = random.uniform(0.5, 2.5)
                minimum_balance = 300
                overdraft_limit = 0
            elif account_type == 'cd':
                balance = random.uniform(1000, 100000) * base_multiplier
                interest_rate = random.uniform(3.0, 5.0)
                minimum_balance = balance  # CDs have fixed amount
                overdraft_limit = 0
            else:  # money_market
                balance = random.uniform(2500, 75000) * base_multiplier
                interest_rate = random.uniform(2.0, 4.0)
                minimum_balance = 2500
                overdraft_limit = 0
            
            # Open date is after customer join date
            days_since_join = (datetime.now().date() - customer['join_date']).days
            days_after_join = random.randint(0, max(0, days_since_join))
            opened_date = customer['join_date'] + timedelta(days=days_after_join)
            
            accounts.append({
                'account_id': account_id,
                'customer_id': customer['customer_id'],
                'account_type': account_type,
                'balance': round(balance, 2),
                'interest_rate': round(interest_rate, 2),
                'opened_date': opened_date,
                'last_transaction_date': opened_date + timedelta(days=random.randint(0, 30)),
                'status': 'active',
                'minimum_balance': minimum_balance,
                'overdraft_limit': overdraft_limit,
                'date': opened_date
            })
        
        return accounts
    
    def _generate_loans(self, customer: Dict[str, Any], count: int) -> List[Dict[str, Any]]:
        """Generate loans for a customer"""
        loans = []
        loan_types = ['mortgage', 'auto', 'personal', 'business']
        used_types = []
        
        for i in range(count):
            # Ensure variety in loan types
            available_types = [t for t in loan_types if t not in used_types]
            if not available_types:
                available_types = ['personal', 'auto']  # Can have multiple of these
            
            loan_type = random.choice(available_types)
            used_types.append(loan_type)
            
            loan_id = f"L{customer['customer_id'][1:]}{i+1:02d}"
            
            # Loan amount based on type and customer creditworthiness
            credit_multiplier = customer['credit_score'] / 700
            income_multiplier = customer['annual_income'] / 50000
            
            if loan_type == 'mortgage':
                amount = random.uniform(100000, 500000) * credit_multiplier
                interest_rate = random.uniform(3.0, 6.0) - (customer['credit_score'] - 600) * 0.01
                term_months = random.choice([180, 240, 360])
            elif loan_type == 'auto':
                amount = random.uniform(10000, 50000) * min(credit_multiplier, 2.0)
                interest_rate = random.uniform(4.0, 10.0) - (customer['credit_score'] - 600) * 0.02
                term_months = random.choice([36, 48, 60, 72])
            elif loan_type == 'personal':
                amount = random.uniform(1000, 25000) * min(income_multiplier, 3.0)
                interest_rate = random.uniform(8.0, 20.0) - (customer['credit_score'] - 600) * 0.05
                term_months = random.choice([12, 24, 36, 48, 60])
            else:  # business
                amount = random.uniform(25000, 200000) * credit_multiplier
                interest_rate = random.uniform(6.0, 15.0) - (customer['credit_score'] - 600) * 0.03
                term_months = random.choice([36, 60, 84, 120])
            
            amount = round(amount, 2)
            interest_rate = max(1.0, min(25.0, round(interest_rate, 2)))
            
            # Calculate monthly payment
            monthly_rate = interest_rate / 100 / 12
            if monthly_rate > 0:
                monthly_payment = amount * (monthly_rate * (1 + monthly_rate)**term_months) / ((1 + monthly_rate)**term_months - 1)
            else:
                monthly_payment = amount / term_months
            monthly_payment = round(monthly_payment, 2)
            
            # Origination date
            days_since_join = (datetime.now().date() - customer['join_date']).days
            if days_since_join < 30:
                # Skip loans for very new customers
                continue
            days_after_join = random.randint(30, days_since_join)
            origination_date = customer['join_date'] + timedelta(days=days_after_join)
            maturity_date = origination_date + timedelta(days=term_months * 30)
            
            # Loan status based on customer segment and randomness
            if customer['segment'] == 'at_risk':
                status_choices = ['current'] * 60 + ['late'] * 30 + ['default'] * 10
            elif customer['segment'] == 'high_value':
                status_choices = ['current'] * 95 + ['paid_off'] * 5
            else:
                status_choices = ['current'] * 85 + ['late'] * 10 + ['paid_off'] * 4 + ['default'] * 1
            
            status = random.choice(status_choices)
            
            # Calculate balances based on status
            months_elapsed = min(term_months, (datetime.now().date() - origination_date).days // 30)
            if status == 'paid_off':
                remaining_balance = 0
                paid_amount = amount
            elif status == 'default':
                paid_amount = monthly_payment * max(3, months_elapsed // 2)
                remaining_balance = amount - paid_amount + random.uniform(0, amount * 0.1)  # Add fees
            else:
                paid_amount = monthly_payment * months_elapsed
                remaining_balance = max(0, amount - paid_amount)
            
            # Payment dates
            if status in ['current', 'late']:
                last_payment_date = origination_date + timedelta(days=(months_elapsed - 1) * 30)
                next_payment_date = origination_date + timedelta(days=months_elapsed * 30)
            else:
                last_payment_date = origination_date + timedelta(days=months_elapsed * 30)
                next_payment_date = None
            
            loans.append({
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
                'remaining_balance': round(remaining_balance, 2),
                'paid_amount': round(paid_amount, 2),
                'late_fees': round(random.uniform(0, 500), 2) if status == 'late' else 0,
                'last_payment_date': last_payment_date,
                'next_payment_date': next_payment_date,
                'date': origination_date
            })
        
        return loans
    
    def _generate_transactions(self, customer: Dict[str, Any], account: Dict[str, Any] = None, 
                             loan: Dict[str, Any] = None, max_transactions: int = 50) -> List[Dict[str, Any]]:
        """Generate transactions for accounts or loans"""
        transactions = []
        
        if account:
            # Generate deposit account transactions (reduced from 5-50)
            num_transactions = min(random.randint(5, 20), max_transactions)
            current_balance = account['balance']
            
            # Work backwards from current balance
            transaction_amounts = []
            for _ in range(num_transactions):
                if account['account_type'] == 'checking':
                    # Mix of deposits and withdrawals
                    if random.random() < 0.6:  # 60% deposits
                        amount = random.uniform(100, 5000)
                        trans_type = random.choice(['deposit', 'direct_deposit', 'transfer_in'])
                    else:
                        amount = -random.uniform(20, 1000)
                        trans_type = random.choice(['withdrawal', 'debit_card', 'check', 'transfer_out'])
                else:
                    # Mostly deposits for savings accounts
                    if random.random() < 0.8:
                        amount = random.uniform(50, 2000)
                        trans_type = 'deposit'
                    else:
                        amount = -random.uniform(100, 1000)
                        trans_type = 'withdrawal'
                
                transaction_amounts.append((amount, trans_type))
            
            # Generate transactions chronologically
            days_since_open = (datetime.now().date() - account['opened_date']).days
            for i, (amount, trans_type) in enumerate(transaction_amounts):
                trans_date = account['opened_date'] + timedelta(
                    days=random.randint(0, days_since_open)
                )
                
                self.transaction_counter += 1
                transaction_id = f"T{self.transaction_counter:08d}"
                
                # Determine category based on transaction type
                if trans_type in ['deposit', 'direct_deposit']:
                    category = random.choice(['salary', 'transfer', 'refund', 'other'])
                elif trans_type == 'debit_card':
                    category = random.choice(['groceries', 'dining', 'shopping', 'gas', 'entertainment'])
                elif trans_type == 'check':
                    category = random.choice(['rent', 'utilities', 'insurance', 'other'])
                else:
                    category = 'transfer'
                
                transactions.append({
                    'transaction_id': transaction_id,
                    'account_id': account['account_id'],
                    'loan_id': None,
                    'customer_id': customer['customer_id'],
                    'transaction_type': trans_type,
                    'amount': round(abs(amount), 2) if amount < 0 else round(amount, 2),
                    'balance_after': round(current_balance, 2),
                    'description': f"{trans_type.replace('_', ' ').title()} - {category}",
                    'category': category,
                    'transaction_date': datetime.combine(trans_date, datetime.min.time()),
                    'posted_date': datetime.combine(trans_date + timedelta(days=random.randint(0, 2)), datetime.min.time())
                })
                
                current_balance -= amount  # Work backwards
        
        elif loan:
            # Generate loan payment transactions
            months_paid = min(
                loan['term_months'],
                (datetime.now().date() - loan['origination_date']).days // 30
            )
            
            if loan['status'] == 'default':
                months_paid = min(months_paid, random.randint(3, 12))
            elif loan['status'] == 'paid_off':
                months_paid = loan['term_months']
            
            for month in range(months_paid):
                payment_date = loan['origination_date'] + timedelta(days=month * 30)
                self.transaction_counter += 1
                transaction_id = f"T{self.transaction_counter:08d}"
                
                # Most payments are on time, some are late
                is_late = random.random() < 0.1 and loan['status'] != 'paid_off'
                if is_late:
                    payment_date += timedelta(days=random.randint(5, 30))
                    amount = loan['monthly_payment'] + random.uniform(25, 100)  # Late fee
                else:
                    amount = loan['monthly_payment']
                
                transactions.append({
                    'transaction_id': transaction_id,
                    'account_id': None,
                    'loan_id': loan['loan_id'],
                    'customer_id': customer['customer_id'],
                    'transaction_type': 'loan_payment',
                    'amount': round(amount, 2),
                    'balance_after': None,
                    'description': f"Loan payment - {loan['loan_type']}",
                    'category': 'loan_payment',
                    'transaction_date': datetime.combine(payment_date, datetime.min.time()),
                    'posted_date': datetime.combine(payment_date, datetime.min.time())
                })
        
        return transactions
    
    def _insert_customers(self, customers: List[Dict[str, Any]]):
        """Insert customers into database"""
        # Insert in batches to avoid memory issues
        batch_size = 100
        
        if isinstance(self.data_service, LocalDataService):
            cursor = self.data_service.connection.cursor()
            
            # Process in batches
            for i in range(0, len(customers), batch_size):
                batch = customers[i:i+batch_size]
                cursor.executemany(
                    """INSERT INTO customers (customer_id, name, email, phone, segment, 
                       join_date, credit_score, annual_income, employment_status, 
                       products_count, total_relationship_value, status)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    [(c['customer_id'], c['name'], c['email'], c['phone'], c['segment'],
                      c['join_date'], c['credit_score'], c['annual_income'], 
                      c['employment_status'], c['products_count'], 
                      c['total_relationship_value'], c['status']) for c in batch]
                )
                self.data_service.connection.commit()
        else:
            # Snowflake bulk insert
            # Convert to format Snowflake expects
            data = [(c['customer_id'], c['name'], c['email'], c['phone'], c['segment'],
                    c['join_date'], c['credit_score'], c['annual_income'], 
                    c['employment_status'], c['products_count'], 
                    c['total_relationship_value'], c['status']) for c in customers]
            
            cursor = self.data_service.connection.cursor()
            cursor.executemany(
                """INSERT INTO CUSTOMERS (customer_id, name, email, phone, segment, 
                   join_date, credit_score, annual_income, employment_status, 
                   products_count, total_relationship_value, status)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                data
            )
            self.data_service.connection.commit()
    
    def _insert_deposits(self, deposits: List[Dict[str, Any]]):
        """Insert deposit accounts into database"""
        if not deposits:
            return
            
        batch_size = 100
        
        if isinstance(self.data_service, LocalDataService):
            cursor = self.data_service.connection.cursor()
            
            # Process in batches
            for i in range(0, len(deposits), batch_size):
                batch = deposits[i:i+batch_size]
                cursor.executemany(
                    """INSERT INTO deposits (account_id, customer_id, account_type, balance,
                       interest_rate, opened_date, last_transaction_date, status, 
                       minimum_balance, overdraft_limit, date)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    [(d['account_id'], d['customer_id'], d['account_type'], d['balance'],
                      d['interest_rate'], d['opened_date'], d['last_transaction_date'],
                      d['status'], d['minimum_balance'], d['overdraft_limit'], d['date']) 
                     for d in batch]
                )
                self.data_service.connection.commit()
        else:
            # Snowflake bulk insert
            data = [(d['account_id'], d['customer_id'], d['account_type'], d['balance'],
                    d['interest_rate'], d['opened_date'], d['last_transaction_date'],
                    d['status'], d['minimum_balance'], d['overdraft_limit'], d['date']) 
                   for d in deposits]
            
            cursor = self.data_service.connection.cursor()
            cursor.executemany(
                """INSERT INTO DEPOSITS (account_id, customer_id, account_type, balance,
                   interest_rate, opened_date, last_transaction_date, status, 
                   minimum_balance, overdraft_limit, date)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                data
            )
            self.data_service.connection.commit()
    
    def _insert_loans(self, loans: List[Dict[str, Any]]):
        """Insert loans into database"""
        if not loans:
            return
            
        batch_size = 100
        
        if isinstance(self.data_service, LocalDataService):
            cursor = self.data_service.connection.cursor()
            
            # Process in batches
            for i in range(0, len(loans), batch_size):
                batch = loans[i:i+batch_size]
                cursor.executemany(
                    """INSERT INTO loans (loan_id, customer_id, loan_type, amount,
                       interest_rate, term_months, monthly_payment, origination_date,
                       maturity_date, status, remaining_balance, paid_amount, late_fees,
                       last_payment_date, next_payment_date, date)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    [(l['loan_id'], l['customer_id'], l['loan_type'], l['amount'],
                      l['interest_rate'], l['term_months'], l['monthly_payment'],
                      l['origination_date'], l['maturity_date'], l['status'],
                      l['remaining_balance'], l['paid_amount'], l['late_fees'],
                      l['last_payment_date'], l['next_payment_date'], l['date'])
                     for l in batch]
                )
                self.data_service.connection.commit()
        else:
            # Snowflake bulk insert
            data = [(l['loan_id'], l['customer_id'], l['loan_type'], l['amount'],
                    l['interest_rate'], l['term_months'], l['monthly_payment'],
                    l['origination_date'], l['maturity_date'], l['status'],
                    l['remaining_balance'], l['paid_amount'], l['late_fees'],
                    l['last_payment_date'], l['next_payment_date'], l['date'])
                   for l in loans]
            
            cursor = self.data_service.connection.cursor()
            cursor.executemany(
                """INSERT INTO LOANS (loan_id, customer_id, loan_type, amount,
                   interest_rate, term_months, monthly_payment, origination_date,
                   maturity_date, status, remaining_balance, paid_amount, late_fees,
                   last_payment_date, next_payment_date, date)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                data
            )
            self.data_service.connection.commit()
    
    def _insert_transactions(self, transactions: List[Dict[str, Any]]):
        """Insert transactions into database"""
        if not transactions:
            return
            
        batch_size = 50  # Smaller batch size for transactions
            
        if isinstance(self.data_service, LocalDataService):
            cursor = self.data_service.connection.cursor()
            
            # Process in batches
            for i in range(0, len(transactions), batch_size):
                batch = transactions[i:i+batch_size]
                cursor.executemany(
                    """INSERT INTO transactions (transaction_id, account_id, loan_id,
                       customer_id, transaction_type, amount, balance_after, description,
                       category, transaction_date, posted_date)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    [(t['transaction_id'], t['account_id'], t['loan_id'], t['customer_id'],
                      t['transaction_type'], t['amount'], t['balance_after'], t['description'],
                      t['category'], t['transaction_date'], t['posted_date'])
                     for t in batch]
                )
                self.data_service.connection.commit()
        else:
            # Snowflake bulk insert
            data = [(t['transaction_id'], t['account_id'], t['loan_id'], t['customer_id'],
                    t['transaction_type'], t['amount'], t['balance_after'], t['description'],
                    t['category'], t['transaction_date'], t['posted_date'])
                   for t in transactions]
            
            cursor = self.data_service.connection.cursor()
            cursor.executemany(
                """INSERT INTO TRANSACTIONS (transaction_id, account_id, loan_id,
                   customer_id, transaction_type, amount, balance_after, description,
                   category, transaction_date, posted_date)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                data
            )
            self.data_service.connection.commit()
    
    def create_views(self):
        """Create database views with multiple levels of dependencies"""
        print("    Creating database views...")
        
        if not self.data_service.connection:
            self.data_service.connect()
        
        cursor = self.data_service.connection.cursor()
        
        # Adjust syntax for SQLite vs Snowflake
        if isinstance(self.data_service, LocalDataService):
            # SQLite doesn't support OR REPLACE for views, need to drop first
            views_to_create = [
                'v_customer_summary', 'v_loan_portfolio', 'v_deposit_summary', 'v_customer_products',
                'v_customer_risk_profile', 'v_product_performance', 'v_customer_lifetime_value',
                'v_executive_dashboard', 'v_risk_analytics'
            ]
            for view in views_to_create:
                cursor.execute(f"DROP VIEW IF EXISTS {view}")
        
        # Level 1 Views - Based on base tables
        
        # Customer Summary View
        create_view_cmd = "CREATE VIEW" if isinstance(self.data_service, LocalDataService) else "CREATE OR REPLACE VIEW"
        cursor.execute(f"""
            {create_view_cmd} v_customer_summary AS
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
        
        # Loan Portfolio View
        cursor.execute(f"""
            {create_view_cmd} v_loan_portfolio AS
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
        
        # Deposit Summary View
        cursor.execute(f"""
            {create_view_cmd} v_deposit_summary AS
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
        
        # Customer Products View
        cursor.execute(f"""
            {create_view_cmd} v_customer_products AS
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
        
        # Level 2 Views - Based on Level 1 views and base tables
        
        # Customer Risk Profile View
        cursor.execute(f"""
            {create_view_cmd} v_customer_risk_profile AS
            SELECT 
                cs.customer_id,
                cs.name,
                cs.segment,
                cs.credit_score,
                cs.total_deposits,
                cs.total_loan_balance,
                CASE 
                    WHEN cs.total_loan_balance = 0 THEN 0
                    ELSE cs.total_loan_balance / NULLIF(cs.annual_income, 0)
                END as debt_to_income_ratio,
                CASE
                    WHEN cs.credit_score >= 720 AND cs.total_loan_balance / NULLIF(cs.annual_income, 0) < 0.3 THEN 'Low'
                    WHEN cs.credit_score >= 650 AND cs.total_loan_balance / NULLIF(cs.annual_income, 0) < 0.5 THEN 'Medium'
                    ELSE 'High'
                END as risk_category,
                lp.default_rate as portfolio_default_rate
            FROM v_customer_summary cs
            LEFT JOIN v_loan_portfolio lp ON lp.status = 'current'
        """)
        
        # Product Performance View
        cursor.execute(f"""
            {create_view_cmd} v_product_performance AS
            SELECT 
                'Loans' as product_category,
                lp.loan_type as product_type,
                lp.loan_count as count,
                lp.total_outstanding as total_value,
                lp.avg_interest_rate as avg_rate,
                lp.default_rate as risk_metric
            FROM v_loan_portfolio lp
            UNION ALL
            SELECT 
                'Deposits' as product_category,
                ds.account_type as product_type,
                ds.account_count as count,
                ds.total_balance as total_value,
                ds.avg_interest_rate as avg_rate,
                0 as risk_metric
            FROM v_deposit_summary ds
        """)
        
        # Customer Lifetime Value View
        
        # Handle date differences between SQLite and Snowflake
        if isinstance(self.data_service, LocalDataService):
            date_diff_expr = """
                CASE 
                    WHEN MAX(t.transaction_date) IS NOT NULL AND MIN(t.transaction_date) IS NOT NULL 
                    THEN CAST(JULIANDAY(MAX(t.transaction_date)) - JULIANDAY(MIN(t.transaction_date)) AS INTEGER)
                    ELSE 0 
                END
            """
        else:
            date_diff_expr = "DATEDIFF(day, MIN(t.transaction_date), MAX(t.transaction_date))"
        
        cursor.execute(f"""
            {create_view_cmd} v_customer_lifetime_value AS
            SELECT 
                cp.customer_id,
                cp.name,
                cp.segment,
                cp.total_products,
                cs.total_deposits,
                cs.total_loan_balance,
                COUNT(DISTINCT t.transaction_id) as transaction_count,
                {date_diff_expr} as active_days,
                (cs.total_deposits * 0.02 + cs.total_loan_balance * 0.05) as estimated_annual_revenue
            FROM v_customer_products cp
            JOIN v_customer_summary cs ON cp.customer_id = cs.customer_id
            LEFT JOIN transactions t ON cp.customer_id = t.customer_id
            GROUP BY cp.customer_id, cp.name, cp.segment, cp.total_products, 
                     cs.total_deposits, cs.total_loan_balance
        """)
        
        # Level 3 Views - Based on Level 2 views
        
        # Executive Dashboard View
        cursor.execute(f"""
            {create_view_cmd} v_executive_dashboard AS
            SELECT 
                COUNT(DISTINCT crp.customer_id) as total_customers,
                COUNT(DISTINCT CASE WHEN crp.risk_category = 'High' THEN crp.customer_id END) as high_risk_customers,
                SUM(crp.total_deposits) as total_deposits,
                SUM(crp.total_loan_balance) as total_loans,
                AVG(crp.debt_to_income_ratio) as avg_debt_to_income,
                SUM(pp.total_value) as total_portfolio_value,
                AVG(CASE WHEN pp.product_category = 'Loans' THEN pp.risk_metric END) as avg_loan_default_rate
            FROM v_customer_risk_profile crp
            CROSS JOIN v_product_performance pp
        """)
        
        # Risk Analytics View
        cursor.execute(f"""
            {create_view_cmd} v_risk_analytics AS
            SELECT 
                crp.risk_category,
                crp.segment as customer_segment,
                COUNT(DISTINCT crp.customer_id) as customer_count,
                AVG(crp.credit_score) as avg_credit_score,
                AVG(crp.debt_to_income_ratio) as avg_dti_ratio,
                SUM(clv.estimated_annual_revenue) as total_revenue_at_risk,
                AVG(clv.active_days) as avg_customer_tenure_days
            FROM v_customer_risk_profile crp
            JOIN v_customer_lifetime_value clv ON crp.customer_id = clv.customer_id
            GROUP BY crp.risk_category, crp.segment
        """)
        
        self.data_service.connection.commit()
        print("    All views created successfully!")
    
    def generate_lineage_data(self):
        """Generate data lineage tracking information"""
        print("    Generating lineage metadata...")
        
        if not self.data_service.connection:
            self.data_service.connect()
        
        cursor = self.data_service.connection.cursor()
        
        # Generate job definitions
        jobs = [
            ('J001', 'LoadCustomerData', 'Loads customer data from CSV files', 'ETL', 'Daily at 2:00 AM'),
            ('J002', 'LoadLoanData', 'Loads loan data from CSV files', 'ETL', 'Daily at 2:30 AM'),
            ('J003', 'LoadDepositData', 'Loads deposit account data from CSV files', 'ETL', 'Daily at 3:00 AM'),
            ('J004', 'LoadTransactionData', 'Loads transaction data from CSV files', 'ETL', 'Hourly'),
            ('J005', 'RefreshCustomerViews', 'Refreshes customer-related views', 'VIEW_REFRESH', 'Daily at 4:00 AM'),
            ('J006', 'DataQualityCheck', 'Runs data quality checks on all tables', 'QUALITY_CHECK', 'Daily at 5:00 AM')
        ]
        
        for job in jobs:
            if isinstance(self.data_service, LocalDataService):
                cursor.execute(
                    "INSERT OR REPLACE INTO jobs (job_id, job_name, job_description, job_type, schedule) VALUES (?, ?, ?, ?, ?)",
                    job
                )
            else:
                cursor.execute(
                    "INSERT INTO JOBS (job_id, job_name, job_description, job_type, schedule) VALUES (%s, %s, %s, %s, %s)",
                    job
                )
        
        # Generate source files
        base_date = datetime.now() - timedelta(days=7)
        source_files = []
        file_id = 1
        
        for day in range(7):
            date = base_date + timedelta(days=day)
            date_str = date.strftime('%Y%m%d')
            
            files = [
                (f'F{file_id:04d}', f'customer_{date_str}.csv', f'/data/raw/customers/customer_{date_str}.csv', 'CSV', random.randint(100000, 500000), f'hash_{file_id}', date),
                (f'F{file_id+1:04d}', f'loan_{date_str}.csv', f'/data/raw/loans/loan_{date_str}.csv', 'CSV', random.randint(200000, 800000), f'hash_{file_id+1}', date),
                (f'F{file_id+2:04d}', f'deposit_{date_str}.csv', f'/data/raw/deposits/deposit_{date_str}.csv', 'CSV', random.randint(150000, 600000), f'hash_{file_id+2}', date),
                (f'F{file_id+3:04d}', f'transaction_{date_str}.csv', f'/data/raw/transactions/transaction_{date_str}.csv', 'CSV', random.randint(500000, 2000000), f'hash_{file_id+3}', date)
            ]
            
            source_files.extend(files)
            file_id += 4
        
        # Insert source files
        for file in source_files:
            if isinstance(self.data_service, LocalDataService):
                cursor.execute(
                    "INSERT OR REPLACE INTO source_files (file_id, file_name, file_path, file_type, file_size, file_hash, arrival_time) VALUES (?, ?, ?, ?, ?, ?, ?)",
                    file
                )
            else:
                cursor.execute(
                    "INSERT INTO SOURCE_FILES (file_id, file_name, file_path, file_type, file_size, file_hash, arrival_time) VALUES (%s, %s, %s, %s, %s, %s, %s)",
                    file
                )
        
        # Generate job runs with some failures
        job_runs = []
        run_id = 1
        
        for day in range(7):
            date = base_date + timedelta(days=day)
            
            # Customer data load
            status = 'SUCCESS' if day != 2 else 'FAILED'  # Failed on day 3
            start_time = date.replace(hour=2, minute=0, second=0)
            end_time = start_time + timedelta(minutes=random.randint(5, 15))
            error_msg = 'Corrupted file: invalid CSV format' if status == 'FAILED' else None
            rows = random.randint(800, 1200) if status == 'SUCCESS' else 0
            
            job_runs.append((
                f'R{run_id:04d}', 'J001', start_time, end_time, status, rows, error_msg
            ))
            run_id += 1
            
            # Loan data load
            start_time = date.replace(hour=2, minute=30, second=0)
            end_time = start_time + timedelta(minutes=random.randint(10, 20))
            job_runs.append((
                f'R{run_id:04d}', 'J002', start_time, end_time, 'SUCCESS', random.randint(1500, 2500), None
            ))
            run_id += 1
            
            # Deposit data load
            start_time = date.replace(hour=3, minute=0, second=0)
            end_time = start_time + timedelta(minutes=random.randint(8, 18))
            job_runs.append((
                f'R{run_id:04d}', 'J003', start_time, end_time, 'SUCCESS', random.randint(2000, 3000), None
            ))
            run_id += 1
            
            # Transaction data loads (multiple per day)
            for hour in [6, 12, 18]:
                start_time = date.replace(hour=hour, minute=0, second=0)
                end_time = start_time + timedelta(minutes=random.randint(15, 30))
                job_runs.append((
                    f'R{run_id:04d}', 'J004', start_time, end_time, 'SUCCESS', random.randint(5000, 15000), None
                ))
                run_id += 1
        
        # Insert job runs
        for run in job_runs:
            if isinstance(self.data_service, LocalDataService):
                cursor.execute(
                    "INSERT OR REPLACE INTO job_runs (job_run_id, job_id, start_time, end_time, status, rows_processed, error_message) VALUES (?, ?, ?, ?, ?, ?, ?)",
                    run
                )
            else:
                cursor.execute(
                    "INSERT INTO JOB_RUNS (job_run_id, job_id, start_time, end_time, status, rows_processed, error_message) VALUES (%s, %s, %s, %s, %s, %s, %s)",
                    run
                )
        
        # Link job runs to source files
        file_idx = 0
        for i, run in enumerate(job_runs):
            job_run_id = run[0]
            job_id = run[1]
            
            # Map jobs to their source files
            if job_id == 'J001':  # Customer data
                file_id = source_files[file_idx * 4][0]
            elif job_id == 'J002':  # Loan data
                file_id = source_files[file_idx * 4 + 1][0]
            elif job_id == 'J003':  # Deposit data
                file_id = source_files[file_idx * 4 + 2][0]
            elif job_id == 'J004':  # Transaction data
                file_id = source_files[file_idx * 4 + 3][0]
            else:
                continue
            
            if job_id in ['J001', 'J002', 'J003']:
                file_idx = (file_idx + 1) % 7
            
            if isinstance(self.data_service, LocalDataService):
                cursor.execute(
                    "INSERT OR REPLACE INTO job_run_source_files (job_run_id, file_id) VALUES (?, ?)",
                    (job_run_id, file_id)
                )
            else:
                cursor.execute(
                    "INSERT INTO JOB_RUN_SOURCE_FILES (job_run_id, file_id) VALUES (%s, %s)",
                    (job_run_id, file_id)
                )
        
        # Link job runs to target tables
        schema_name = 'main' if isinstance(self.data_service, LocalDataService) else self.data_service.connection_params['schema']
        
        for run in job_runs:
            job_run_id, job_id, _, _, status, rows_processed, _ = run
            
            if status == 'SUCCESS':
                if job_id == 'J001':
                    target_table = 'customers'
                elif job_id == 'J002':
                    target_table = 'loans'
                elif job_id == 'J003':
                    target_table = 'deposits'
                elif job_id == 'J004':
                    target_table = 'transactions'
                else:
                    continue
                
                if isinstance(self.data_service, SnowflakeDataService):
                    target_table = target_table.upper()
                
                if isinstance(self.data_service, LocalDataService):
                    cursor.execute(
                        "INSERT OR REPLACE INTO job_run_target_tables (job_run_id, schema_name, table_name, rows_inserted) VALUES (?, ?, ?, ?)",
                        (job_run_id, schema_name, target_table, rows_processed)
                    )
                else:
                    cursor.execute(
                        "INSERT INTO JOB_RUN_TARGET_TABLES (job_run_id, schema_name, table_name, rows_inserted) VALUES (%s, %s, %s, %s)",
                        (job_run_id, schema_name, target_table, rows_processed)
                    )
        
        # Register views in metadata
        views = [
            # Level 1 views
            ('V001', schema_name, 'v_customer_summary', 1, 'Summary of customer accounts and loans'),
            ('V002', schema_name, 'v_loan_portfolio', 1, 'Loan portfolio analysis by type and status'),
            ('V003', schema_name, 'v_deposit_summary', 1, 'Deposit account summary by type'),
            ('V004', schema_name, 'v_customer_products', 1, 'Customer product holdings'),
            # Level 2 views
            ('V005', schema_name, 'v_customer_risk_profile', 2, 'Customer risk assessment'),
            ('V006', schema_name, 'v_product_performance', 2, 'Product performance metrics'),
            ('V007', schema_name, 'v_customer_lifetime_value', 2, 'Customer lifetime value calculation'),
            # Level 3 views
            ('V008', schema_name, 'v_executive_dashboard', 3, 'Executive summary dashboard'),
            ('V009', schema_name, 'v_risk_analytics', 3, 'Risk analytics by segment')
        ]
        
        for view in views:
            if isinstance(self.data_service, LocalDataService):
                cursor.execute(
                    "INSERT OR REPLACE INTO data_views (view_id, schema_name, view_name, view_level, description) VALUES (?, ?, ?, ?, ?)",
                    view
                )
            else:
                cursor.execute(
                    "INSERT INTO DATA_VIEWS (view_id, schema_name, view_name, view_level, description) VALUES (%s, %s, %s, %s, %s)",
                    view
                )
        
        # Define view dependencies
        dependencies = [
            # v_customer_summary depends on base tables
            ('D001', 'V001', schema_name, 'customers', 'table'),
            ('D002', 'V001', schema_name, 'deposits', 'table'),
            ('D003', 'V001', schema_name, 'loans', 'table'),
            # v_loan_portfolio depends on loans
            ('D004', 'V002', schema_name, 'loans', 'table'),
            # v_deposit_summary depends on deposits
            ('D005', 'V003', schema_name, 'deposits', 'table'),
            # v_customer_products depends on base tables
            ('D006', 'V004', schema_name, 'customers', 'table'),
            ('D007', 'V004', schema_name, 'deposits', 'table'),
            ('D008', 'V004', schema_name, 'loans', 'table'),
            # v_customer_risk_profile depends on views
            ('D009', 'V005', schema_name, 'v_customer_summary', 'view'),
            ('D010', 'V005', schema_name, 'v_loan_portfolio', 'view'),
            # v_product_performance depends on views
            ('D011', 'V006', schema_name, 'v_loan_portfolio', 'view'),
            ('D012', 'V006', schema_name, 'v_deposit_summary', 'view'),
            # v_customer_lifetime_value depends on views and tables
            ('D013', 'V007', schema_name, 'v_customer_products', 'view'),
            ('D014', 'V007', schema_name, 'v_customer_summary', 'view'),
            ('D015', 'V007', schema_name, 'transactions', 'table'),
            # v_executive_dashboard depends on level 2 views
            ('D016', 'V008', schema_name, 'v_customer_risk_profile', 'view'),
            ('D017', 'V008', schema_name, 'v_product_performance', 'view'),
            # v_risk_analytics depends on level 2 views
            ('D018', 'V009', schema_name, 'v_customer_risk_profile', 'view'),
            ('D019', 'V009', schema_name, 'v_customer_lifetime_value', 'view')
        ]
        
        if isinstance(self.data_service, SnowflakeDataService):
            # Convert table names to uppercase for Snowflake
            dependencies = [
                (d[0], d[1], d[2], d[3].upper() if d[4] == 'table' else d[3], d[4])
                for d in dependencies
            ]
        
        for dep in dependencies:
            if isinstance(self.data_service, LocalDataService):
                cursor.execute(
                    "INSERT OR REPLACE INTO view_dependencies (dependency_id, view_id, depends_on_schema, depends_on_object, depends_on_type) VALUES (?, ?, ?, ?, ?)",
                    dep
                )
            else:
                cursor.execute(
                    "INSERT INTO VIEW_DEPENDENCIES (dependency_id, view_id, depends_on_schema, depends_on_object, depends_on_type) VALUES (%s, %s, %s, %s, %s)",
                    dep
                )
        
        # Generate some data quality checks
        quality_checks = [
            ('Q001', schema_name, 'customers', 'table', 'row_count', 'Count check', 1000, 900, 'SUCCESS', datetime.now() - timedelta(hours=1)),
            ('Q002', schema_name, 'v_executive_dashboard', 'view', 'freshness', 'Data freshness check', 2, 24, 'SUCCESS', datetime.now() - timedelta(hours=1)),
            ('Q003', schema_name, 'loans', 'table', 'null_check', 'Null value check on customer_id', 0, 0, 'SUCCESS', datetime.now() - timedelta(hours=2)),
            ('Q004', schema_name, 'v_customer_risk_profile', 'view', 'row_count', 'Row count validation', 850, 900, 'WARNING', datetime.now() - timedelta(minutes=30))
        ]
        
        for check in quality_checks:
            if isinstance(self.data_service, LocalDataService):
                cursor.execute(
                    """INSERT OR REPLACE INTO data_quality_checks 
                    (check_id, schema_name, object_name, object_type, check_type, check_result, check_value, threshold_value, status, check_timestamp) 
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    check
                )
            else:
                cursor.execute(
                    """INSERT INTO DATA_QUALITY_CHECKS 
                    (check_id, schema_name, object_name, object_type, check_type, check_result, check_value, threshold_value, status, check_timestamp) 
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                    check
                )
        
        self.data_service.connection.commit()
        print("    Lineage data generated successfully!")
    
    def verify_data(self):
        """Verify data was created correctly"""
        print("\nVerifying data...")
        
        if not self.data_service.connection:
            self.data_service.connect()
        
        tables = ['customers', 'loans', 'deposits', 'transactions', 'jobs', 'job_runs', 'source_files', 'data_views', 'view_dependencies']
        if isinstance(self.data_service, SnowflakeDataService):
            tables = [t.upper() for t in tables]
        
        for table in tables:
            df = self.data_service.execute_query(f"SELECT COUNT(*) as count FROM {table}")
            count = df.iloc[0]['count'] if not df.empty else 0
            print(f"{table}: {count} records")
        
        # Also verify views
        print("\nVerifying views...")
        views = ['v_customer_summary', 'v_executive_dashboard']
        if isinstance(self.data_service, SnowflakeDataService):
            views = [v.upper() for v in views]
        
        for view in views:
            try:
                df = self.data_service.execute_query(f"SELECT COUNT(*) as count FROM {view}")
                count = df.iloc[0]['count'] if not df.empty else 0
                print(f"{view}: {count} records")
            except Exception as e:
                print(f"{view}: Error - {str(e)}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Set up banking database with mock data")
    parser.add_argument('--provider', type=str, default='local', 
                      choices=['local', 'snowflake'],
                      help='Database provider to use (default: local)')
    parser.add_argument('--customers', type=int, default=100,
                      help='Number of customers to generate (default: 100)')
    parser.add_argument('--drop-existing', action='store_true',
                      help='Drop existing tables before creating new ones')
    parser.add_argument('--skip-data', action='store_true',
                      help='Only create tables, skip data generation')
    parser.add_argument('--verify-only', action='store_true',
                      help='Only verify existing data, no creation')
    
    args = parser.parse_args()
    
    # Create setup instance
    setup = DatabaseSetup(args.provider, drop_existing=args.drop_existing)
    
    try:
        if args.verify_only:
            setup.verify_data()
        else:
            # Connect to database
            if not setup.data_service.connect():
                print(f"Failed to connect to {args.provider} database")
                sys.exit(1)
            
            print(f"Connected to {args.provider} database")
            
            # Create tables
            setup.create_tables()
            
            # Generate mock data unless skipped
            if not args.skip_data:
                setup.generate_mock_data(args.customers)
            
            # Verify the setup
            setup.verify_data()
            
    except Exception as e:
        print(f"Error: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        # Clean up
        if setup.data_service:
            setup.data_service.disconnect()
            print("\nDatabase connection closed.")