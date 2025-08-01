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
    
    def __init__(self, provider: str = 'local'):
        self.provider = provider
        self.data_service = DataServiceFactory.create_data_service(provider)
        
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
        
        # Drop existing tables if requested
        if args.drop_existing:
            print("Dropping existing tables...")
            cursor.execute("DROP TABLE IF EXISTS loans")
            cursor.execute("DROP TABLE IF EXISTS deposits")
            cursor.execute("DROP TABLE IF EXISTS customers")
            cursor.execute("DROP TABLE IF EXISTS transactions")
        
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
        
        # Create indexes for better query performance
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_loans_customer ON loans(customer_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_loans_status ON loans(status)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_loans_type ON loans(loan_type)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_deposits_customer ON deposits(customer_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_deposits_type ON deposits(account_type)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_transactions_customer ON transactions(customer_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_transactions_date ON transactions(transaction_date)")
        
        self.data_service.connection.commit()
        print("Tables created successfully!")
    
    def _create_snowflake_tables(self):
        """Create tables for Snowflake"""
        if not self.data_service.connection:
            self.data_service.connect()
        
        cursor = self.data_service.connection.cursor()
        
        # Drop existing tables if requested
        if args.drop_existing:
            print("Dropping existing tables...")
            cursor.execute("DROP TABLE IF EXISTS LOANS")
            cursor.execute("DROP TABLE IF EXISTS DEPOSITS")
            cursor.execute("DROP TABLE IF EXISTS CUSTOMERS")
            cursor.execute("DROP TABLE IF EXISTS TRANSACTIONS")
        
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
        
        self.data_service.connection.commit()
        print("Tables created successfully!")
    
    def generate_mock_data(self, num_customers: int = 1000):
        """Generate mock data for all tables"""
        print(f"Generating mock data for {num_customers} customers...")
        
        if not self.data_service.connection:
            self.data_service.connect()
        
        # Generate customers
        customers = self._generate_customers(num_customers)
        self._insert_customers(customers)
        
        # Generate accounts and loans for customers
        total_loans = 0
        total_deposits = 0
        total_transactions = 0
        
        for customer in customers:
            # Each customer has 1-4 products
            num_products = random.randint(1, 4)
            
            # Generate deposit accounts
            if random.random() < 0.9:  # 90% have at least one deposit account
                num_accounts = random.randint(1, min(3, num_products))
                accounts = self._generate_deposit_accounts(customer, num_accounts)
                self._insert_deposits(accounts)
                total_deposits += len(accounts)
                
                # Generate transactions for each account
                for account in accounts:
                    transactions = self._generate_transactions(customer, account=account)
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
                    transactions = self._generate_transactions(customer, loan=loan)
                    self._insert_transactions(transactions)
                    total_transactions += len(transactions)
        
        print(f"Generated {num_customers} customers")
        print(f"Generated {total_loans} loans")
        print(f"Generated {total_deposits} deposit accounts")
        print(f"Generated {total_transactions} transactions")
        print("Mock data generation complete!")
    
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
                join_days_ago = random.randint(0, 90)  # 0-3 months
            
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
            days_after_join = random.randint(0, (datetime.now().date() - customer['join_date']).days)
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
            days_after_join = random.randint(30, (datetime.now().date() - customer['join_date']).days)
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
                             loan: Dict[str, Any] = None) -> List[Dict[str, Any]]:
        """Generate transactions for accounts or loans"""
        transactions = []
        
        if account:
            # Generate deposit account transactions
            num_transactions = random.randint(5, 50)
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
                
                transaction_id = f"T{account['account_id'][1:]}{i+1:04d}"
                
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
                transaction_id = f"T{loan['loan_id'][1:]}{month+1:04d}"
                
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
        if isinstance(self.data_service, LocalDataService):
            cursor = self.data_service.connection.cursor()
            cursor.executemany(
                """INSERT INTO customers (customer_id, name, email, phone, segment, 
                   join_date, credit_score, annual_income, employment_status, 
                   products_count, total_relationship_value, status)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                [(c['customer_id'], c['name'], c['email'], c['phone'], c['segment'],
                  c['join_date'], c['credit_score'], c['annual_income'], 
                  c['employment_status'], c['products_count'], 
                  c['total_relationship_value'], c['status']) for c in customers]
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
        if isinstance(self.data_service, LocalDataService):
            cursor = self.data_service.connection.cursor()
            cursor.executemany(
                """INSERT INTO deposits (account_id, customer_id, account_type, balance,
                   interest_rate, opened_date, last_transaction_date, status, 
                   minimum_balance, overdraft_limit, date)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                [(d['account_id'], d['customer_id'], d['account_type'], d['balance'],
                  d['interest_rate'], d['opened_date'], d['last_transaction_date'],
                  d['status'], d['minimum_balance'], d['overdraft_limit'], d['date']) 
                 for d in deposits]
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
        if isinstance(self.data_service, LocalDataService):
            cursor = self.data_service.connection.cursor()
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
                 for l in loans]
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
            
        if isinstance(self.data_service, LocalDataService):
            cursor = self.data_service.connection.cursor()
            cursor.executemany(
                """INSERT INTO transactions (transaction_id, account_id, loan_id,
                   customer_id, transaction_type, amount, balance_after, description,
                   category, transaction_date, posted_date)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                [(t['transaction_id'], t['account_id'], t['loan_id'], t['customer_id'],
                  t['transaction_type'], t['amount'], t['balance_after'], t['description'],
                  t['category'], t['transaction_date'], t['posted_date'])
                 for t in transactions]
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
    
    def verify_data(self):
        """Verify data was created correctly"""
        print("\nVerifying data...")
        
        if not self.data_service.connection:
            self.data_service.connect()
        
        tables = ['customers', 'loans', 'deposits', 'transactions']
        if isinstance(self.data_service, SnowflakeDataService):
            tables = [t.upper() for t in tables]
        
        for table in tables:
            df = self.data_service.execute_query(f"SELECT COUNT(*) as count FROM {table}")
            count = df.iloc[0]['count'] if not df.empty else 0
            print(f"{table}: {count} records")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Set up banking database with mock data")
    parser.add_argument('--provider', type=str, default='local', 
                      choices=['local', 'snowflake'],
                      help='Database provider to use (default: local)')
    parser.add_argument('--customers', type=int, default=1000,
                      help='Number of customers to generate (default: 1000)')
    parser.add_argument('--drop-existing', action='store_true',
                      help='Drop existing tables before creating new ones')
    parser.add_argument('--skip-data', action='store_true',
                      help='Only create tables, skip data generation')
    parser.add_argument('--verify-only', action='store_true',
                      help='Only verify existing data, no creation')
    
    args = parser.parse_args()
    
    # Create setup instance
    setup = DatabaseSetup(args.provider)
    
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