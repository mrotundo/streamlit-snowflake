#!/usr/bin/env python3
"""
Simplified database setup script without pandas dependency
Creates tables and generates mock data for local SQLite database
"""

import os
import sys
import sqlite3
import random
from datetime import datetime, timedelta

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

def create_database():
    """Create SQLite database and tables"""
    db_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'data', 'banking.db')
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
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
    
    # Create simple indexes
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_customers_segment ON customers(segment)")
    
    print("Tables created successfully!")
    
    # Generate sample data
    print("Generating sample data...")
    
    # Sample customers
    segments = ['high_value', 'growth', 'maintain', 'at_risk', 'new']
    employment_statuses = ['employed', 'self_employed', 'retired', 'student']
    
    for i in range(100):
        customer_id = f"C{i+1:06d}"
        name = f"Customer {i+1}"
        email = f"customer{i+1}@email.com"
        phone = f"+1{random.randint(2000000000, 9999999999)}"
        segment = random.choice(segments)
        join_date = (datetime.now() - timedelta(days=random.randint(0, 1095))).date()
        credit_score = random.randint(580, 850)
        annual_income = random.randint(30000, 250000)
        employment_status = random.choice(employment_statuses)
        
        cursor.execute("""
            INSERT OR REPLACE INTO customers 
            (customer_id, name, email, phone, segment, join_date, credit_score, 
             annual_income, employment_status, products_count, total_relationship_value, status)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (customer_id, name, email, phone, segment, join_date, credit_score, 
              annual_income, employment_status, 0, 0, 'active'))
    
    conn.commit()
    
    # Verify data
    cursor.execute("SELECT COUNT(*) FROM customers")
    count = cursor.fetchone()[0]
    print(f"Created {count} customers")
    
    conn.close()
    print("\nDatabase setup complete!")
    print(f"Database location: {db_path}")

if __name__ == "__main__":
    try:
        create_database()
    except Exception as e:
        print(f"Error: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)