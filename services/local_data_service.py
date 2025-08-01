import sqlite3
import pandas as pd
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta
import os
from .data_interface import DataInterface
import random
import json


class LocalDataService(DataInterface):
    """Local SQLite implementation of the data interface"""
    
    def __init__(self, db_path: str = "data/banking.db"):
        self.db_path = db_path
        self.connection = None
        self._ensure_db_directory()
    
    def _ensure_db_directory(self):
        """Ensure the database directory exists"""
        db_dir = os.path.dirname(self.db_path)
        if db_dir and not os.path.exists(db_dir):
            os.makedirs(db_dir)
    
    def connect(self) -> bool:
        """Establish connection to SQLite database"""
        try:
            self.connection = sqlite3.connect(self.db_path)
            self.connection.row_factory = sqlite3.Row
            return True
        except Exception as e:
            print(f"Failed to connect to SQLite: {e}")
            return False
    
    def disconnect(self) -> None:
        """Close SQLite connection"""
        if self.connection:
            self.connection.close()
            self.connection = None
    
    def execute_query(self, query: str, params: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
        """Execute a SQL query and return results as a DataFrame"""
        if not self.connection:
            raise ConnectionError("Not connected to database")
        
        try:
            if params:
                df = pd.read_sql_query(query, self.connection, params=params)
            else:
                df = pd.read_sql_query(query, self.connection)
            return df
        except Exception as e:
            raise Exception(f"Query execution failed: {e}")
    
    def execute_structured_query(self, structured_query: Dict[str, Any]) -> Dict[str, Any]:
        """Execute a structured query and return formatted results"""
        try:
            # Convert structured query to SQL
            sql = self.build_sql_from_structured_query(structured_query)
            
            # Execute query
            df = self.execute_query(sql)
            
            # Get additional sample data if needed
            entity = structured_query.get('entity', 'data')
            sample_sql = f"SELECT * FROM {self._get_table_name(entity)} LIMIT 10"
            sample_df = self.execute_query(sample_sql)
            
            # Format results
            result = {
                "query_executed": structured_query,
                "row_count": len(df),
                "execution_time": "0.025s",
                "data": {
                    "summary_stats": self._format_summary_stats(df, structured_query),
                    "data_points": sample_df.to_dict('records'),
                    "period_label": self._get_period_label(structured_query.get('time_period', {}))
                }
            }
            
            # Add aggregated data if grouping was requested
            if 'aggregations' in structured_query and len(structured_query['aggregations']) > 0:
                result['data']['breakdowns'] = df.to_dict('records')
            
            return result
            
        except Exception as e:
            return {
                "query_executed": structured_query,
                "error": str(e),
                "data": {}
            }
    
    def _get_table_name(self, entity: str) -> str:
        """Map entity to table name"""
        table_map = {
            'loans': 'loans',
            'loan': 'loans',
            'deposits': 'deposits',
            'deposit': 'deposits',
            'customers': 'customers',
            'customer': 'customers'
        }
        return table_map.get(entity.lower(), entity)
    
    def _format_summary_stats(self, df: pd.DataFrame, query: Dict[str, Any]) -> Dict[str, Any]:
        """Format summary statistics from query results"""
        stats = {}
        
        # Extract values from dataframe
        if not df.empty:
            row = df.iloc[0]
            for col in df.columns:
                if col in ['total_count', 'total_amount', 'average_amount', 'default_rate']:
                    value = row[col]
                    if pd.notna(value):
                        stats[col] = float(value) if isinstance(value, (int, float)) else value
        
        # Add entity-specific stats
        entity = query.get('entity', '').lower()
        if entity in ['loan', 'loans'] and 'total_count' in stats:
            stats['total_loans'] = stats.pop('total_count', 0)
            stats['total_value'] = stats.get('total_amount', 0)
            if 'average_amount' in stats:
                stats['average_loan_size'] = stats.pop('average_amount')
        elif entity in ['deposit', 'deposits'] and 'total_count' in stats:
            stats['total_accounts'] = stats.pop('total_count', 0)
            stats['total_deposits'] = stats.get('total_amount', 0)
            if 'average_amount' in stats:
                stats['average_balance'] = stats.pop('average_amount')
        elif entity in ['customer', 'customers'] and 'total_count' in stats:
            stats['total_customers'] = stats.pop('total_count', 0)
        
        return stats
    
    def _get_period_label(self, time_period: Dict[str, Any]) -> str:
        """Get a label for the time period"""
        if isinstance(time_period, dict):
            if 'label' in time_period:
                return time_period['label']
            elif 'quarter' in time_period and 'year' in time_period:
                return f"{time_period['quarter']} {time_period['year']}"
            elif 'start' in time_period and 'end' in time_period:
                return f"{time_period['start']} to {time_period['end']}"
        return "Current Period"
    
    def get_table_schema(self, table_name: str) -> Dict[str, Any]:
        """Get schema information for a specific table"""
        if not self.connection:
            raise ConnectionError("Not connected to database")
        
        cursor = self.connection.cursor()
        cursor.execute(f"PRAGMA table_info({table_name})")
        columns = cursor.fetchall()
        
        schema = {
            "table_name": table_name,
            "columns": []
        }
        
        for col in columns:
            schema["columns"].append({
                "name": col[1],
                "type": col[2],
                "nullable": col[3] == 0,
                "primary_key": col[5] == 1
            })
        
        return schema
    
    def get_available_tables(self) -> List[str]:
        """Get list of available tables"""
        if not self.connection:
            raise ConnectionError("Not connected to database")
        
        cursor = self.connection.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = cursor.fetchall()
        
        return [table[0] for table in tables if not table[0].startswith('sqlite_')]
    
    def validate_connection(self) -> bool:
        """Test if the connection is valid"""
        if not self.connection:
            return False
        
        try:
            cursor = self.connection.cursor()
            cursor.execute("SELECT 1")
            cursor.fetchone()
            return True
        except:
            return False
    
    def get_connection_info(self) -> Dict[str, Any]:
        """Get information about the connection"""
        return {
            "type": "SQLite",
            "database": self.db_path,
            "connected": self.validate_connection(),
            "tables": self.get_available_tables() if self.validate_connection() else []
        }
    
    def initialize_sample_data(self):
        """Initialize the database with sample banking data"""
        if not self.connection:
            raise ConnectionError("Not connected to database")
        
        cursor = self.connection.cursor()
        
        # Create tables
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS loans (
                loan_id TEXT PRIMARY KEY,
                customer_id TEXT,
                loan_type TEXT,
                amount REAL,
                interest_rate REAL,
                term_months INTEGER,
                origination_date DATE,
                status TEXT,
                payment_amount REAL,
                remaining_balance REAL,
                date DATE
            )
        """)
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS deposits (
                account_id TEXT PRIMARY KEY,
                customer_id TEXT,
                account_type TEXT,
                balance REAL,
                interest_rate REAL,
                opened_date DATE,
                status TEXT,
                date DATE
            )
        """)
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS customers (
                customer_id TEXT PRIMARY KEY,
                name TEXT,
                segment TEXT,
                join_date DATE,
                credit_score INTEGER,
                annual_income REAL,
                products_count INTEGER,
                total_relationship_value REAL,
                status TEXT
            )
        """)
        
        # Generate sample data
        self._generate_sample_customers(cursor, 1000)
        self._generate_sample_loans(cursor, 2000)
        self._generate_sample_deposits(cursor, 3000)
        
        self.connection.commit()
    
    def _generate_sample_customers(self, cursor, count: int):
        """Generate sample customer data"""
        segments = ['high_value', 'growth', 'maintain', 'at_risk']
        
        for i in range(count):
            customer_id = f"C{i:05d}"
            name = f"Customer {i}"
            segment = random.choice(segments)
            join_date = (datetime.now() - timedelta(days=random.randint(0, 3650))).date()
            credit_score = random.randint(300, 850)
            annual_income = random.randint(30000, 300000)
            products_count = random.randint(1, 5)
            total_value = random.randint(10000, 1000000)
            
            cursor.execute("""
                INSERT OR REPLACE INTO customers 
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'active')
            """, (customer_id, name, segment, join_date, credit_score, 
                  annual_income, products_count, total_value))
    
    def _generate_sample_loans(self, cursor, count: int):
        """Generate sample loan data"""
        loan_types = ['mortgage', 'auto', 'personal', 'business']
        statuses = ['current', 'current', 'current', 'late', 'default', 'paid_off']
        
        # Generate data for different time periods
        for i in range(count):
            loan_id = f"L{i:05d}"
            customer_id = f"C{random.randint(0, 999):05d}"
            loan_type = random.choice(loan_types)
            
            # Set amount based on loan type
            if loan_type == 'mortgage':
                amount = random.randint(100000, 800000)
                rate = random.uniform(3.0, 5.0)
                term = random.choice([180, 360])
            elif loan_type == 'auto':
                amount = random.randint(10000, 80000)
                rate = random.uniform(4.0, 8.0)
                term = random.choice([36, 48, 60, 72])
            elif loan_type == 'personal':
                amount = random.randint(1000, 50000)
                rate = random.uniform(6.0, 15.0)
                term = random.choice([12, 24, 36, 48])
            else:  # business
                amount = random.randint(50000, 500000)
                rate = random.uniform(5.0, 10.0)
                term = random.choice([36, 60, 84])
            
            # Generate dates - some in Q3 2024, some in Q3 2025
            if i % 2 == 0:
                # Q3 2025 data
                origination_date = datetime(2025, random.randint(7, 9), random.randint(1, 28)).date()
            else:
                # Q3 2024 data
                origination_date = datetime(2024, random.randint(7, 9), random.randint(1, 28)).date()
            
            status = random.choice(statuses)
            payment = amount * (rate/100/12) / (1 - (1 + rate/100/12)**(-term))
            remaining = amount * random.uniform(0.3, 1.0) if status != 'paid_off' else 0
            
            cursor.execute("""
                INSERT OR REPLACE INTO loans 
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (loan_id, customer_id, loan_type, amount, rate, term,
                  origination_date, status, payment, remaining, origination_date))
    
    def _generate_sample_deposits(self, cursor, count: int):
        """Generate sample deposit data"""
        account_types = ['checking', 'savings', 'cd', 'money_market']
        
        for i in range(count):
            account_id = f"A{i:05d}"
            customer_id = f"C{random.randint(0, 999):05d}"
            account_type = random.choice(account_types)
            
            # Set balance and rate based on account type
            if account_type == 'checking':
                balance = random.randint(100, 25000)
                rate = 0.01
            elif account_type == 'savings':
                balance = random.randint(500, 50000)
                rate = random.uniform(0.1, 2.0)
            elif account_type == 'cd':
                balance = random.randint(1000, 100000)
                rate = random.uniform(2.0, 5.0)
            else:  # money_market
                balance = random.randint(2500, 100000)
                rate = random.uniform(1.0, 3.0)
            
            # Generate dates
            if i % 2 == 0:
                # Recent data
                opened_date = (datetime.now() - timedelta(days=random.randint(0, 365))).date()
            else:
                # Older data
                opened_date = (datetime.now() - timedelta(days=random.randint(365, 1825))).date()
            
            cursor.execute("""
                INSERT OR REPLACE INTO deposits 
                VALUES (?, ?, ?, ?, ?, ?, 'active', ?)
            """, (account_id, customer_id, account_type, balance, rate,
                  opened_date, opened_date))