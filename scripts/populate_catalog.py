#!/usr/bin/env python3
"""
Script to populate data catalog metadata for banking views
This populates the catalog tables with metadata about views and their columns
"""

import os
import sys
import argparse
import uuid
from datetime import datetime

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.settings import Settings
from services.data_factory import DataServiceFactory


class CatalogPopulator:
    """Populates data catalog metadata"""
    
    def __init__(self, provider: str = 'local'):
        self.provider = provider
        self.data_service = DataServiceFactory.create_data_service(provider)
        
    def populate_catalog(self):
        """Populate all catalog metadata"""
        print(f"\nPopulating data catalog for {self.provider} database...")
        
        if not self.data_service.connection:
            self.data_service.connect()
            
        try:
            # Clear existing catalog data
            self._clear_catalog_data()
            
            # Populate views metadata
            self._populate_views_metadata()
            
            # Populate columns metadata
            self._populate_columns_metadata()
            
            # Populate examples
            self._populate_examples()
            
            # Populate metrics (sample metrics)
            self._populate_metrics()
            
            self.data_service.connection.commit()
            print("\nData catalog populated successfully!")
            
        except Exception as e:
            print(f"Error populating catalog: {e}")
            if self.data_service.connection:
                self.data_service.connection.rollback()
            raise
            
    def _clear_catalog_data(self):
        """Clear existing catalog data"""
        print("  - Clearing existing catalog data...")
        cursor = self.data_service.connection.cursor()
        
        tables = ['data_catalog_examples', 'data_catalog_metrics', 
                  'data_catalog_columns', 'data_catalog_views']
        
        for table in tables:
            if self.provider == 'snowflake':
                cursor.execute(f"DELETE FROM {table.upper()}")
            else:
                cursor.execute(f"DELETE FROM {table}")
                
    def _populate_views_metadata(self):
        """Populate view-level metadata"""
        print("  - Populating views metadata...")
        cursor = self.data_service.connection.cursor()
        
        views_metadata = [
            {
                'view_name': 'v_executive_dashboard',
                'view_type': 'Executive Summary',
                'business_description': 'High-level dashboard for executive leadership showing key banking metrics including customer counts, portfolio values, and risk indicators',
                'technical_description': 'Aggregates data from customers, loans, deposits, and transactions tables with real-time calculations of portfolio metrics',
                'owner': 'Analytics Team',
                'data_domain': 'Executive Reporting',
                'refresh_frequency': 'Real-time',
                'row_count': 1
            },
            {
                'view_name': 'v_customer_summary',
                'view_type': 'Customer Analytics',
                'business_description': 'Comprehensive view of customer profiles including demographics, product holdings, and relationship value',
                'technical_description': 'Joins customer master data with product tables to create 360-degree customer view',
                'owner': 'Customer Intelligence Team',
                'data_domain': 'Customer Analytics',
                'refresh_frequency': 'Real-time',
                'row_count': 100
            },
            {
                'view_name': 'v_loan_portfolio',
                'view_type': 'Product Analytics',
                'business_description': 'Detailed loan portfolio analysis including performance metrics, risk indicators, and payment status',
                'technical_description': 'Aggregates loan data with calculated fields for portfolio health metrics',
                'owner': 'Credit Risk Team',
                'data_domain': 'Credit Risk',
                'refresh_frequency': 'Real-time',
                'row_count': 150
            },
            {
                'view_name': 'v_deposit_summary',
                'view_type': 'Product Analytics',
                'business_description': 'Deposit account analysis showing balances, account types, and customer distribution',
                'technical_description': 'Summarizes deposit accounts with balance calculations and account status',
                'owner': 'Deposit Products Team',
                'data_domain': 'Deposit Analytics',
                'refresh_frequency': 'Real-time',
                'row_count': 200
            },
            {
                'view_name': 'v_risk_analytics',
                'view_type': 'Risk Analytics',
                'business_description': 'Risk assessment view combining credit risk, concentration risk, and portfolio risk metrics',
                'technical_description': 'Complex risk calculations using loans, customers, and transaction data',
                'owner': 'Risk Management Team',
                'data_domain': 'Risk Management',
                'refresh_frequency': 'Real-time',
                'row_count': 1
            },
            {
                'view_name': 'v_customer_products',
                'view_type': 'Relationship Analytics',
                'business_description': 'Cross-product holdings by customer showing all banking relationships',
                'technical_description': 'Union of loan and deposit products grouped by customer',
                'owner': 'Product Management Team',
                'data_domain': 'Product Analytics',
                'refresh_frequency': 'Real-time',
                'row_count': 350
            }
        ]
        
        for view in views_metadata:
            view_id = str(uuid.uuid4())
            
            if self.provider == 'snowflake':
                cursor.execute("""
                    INSERT INTO DATA_CATALOG_VIEWS 
                    (catalog_view_id, view_name, view_type, business_description, 
                     technical_description, owner, data_domain, refresh_frequency, 
                     last_refreshed, row_count)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP(), %s)
                """, (
                    view_id, view['view_name'], view['view_type'], 
                    view['business_description'], view['technical_description'],
                    view['owner'], view['data_domain'], view['refresh_frequency'],
                    view['row_count']
                ))
            else:
                cursor.execute("""
                    INSERT INTO data_catalog_views 
                    (catalog_view_id, view_name, view_type, business_description, 
                     technical_description, owner, data_domain, refresh_frequency, 
                     last_refreshed, row_count)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, ?)
                """, (
                    view_id, view['view_name'], view['view_type'], 
                    view['business_description'], view['technical_description'],
                    view['owner'], view['data_domain'], view['refresh_frequency'],
                    view['row_count']
                ))
                
    def _populate_columns_metadata(self):
        """Populate column-level metadata"""
        print("  - Populating columns metadata...")
        cursor = self.data_service.connection.cursor()
        
        # Define columns for each view
        columns_metadata = {
            'v_executive_dashboard': [
                {
                    'column_name': 'total_customers',
                    'column_type': 'INTEGER',
                    'column_order': 1,
                    'is_nullable': False,
                    'business_description': 'Total number of active customers in the bank',
                    'technical_description': 'COUNT of customers where status = active',
                    'data_classification': 'Public',
                    'example_values': '85, 90, 95'
                },
                {
                    'column_name': 'active_loans',
                    'column_type': 'INTEGER',
                    'column_order': 2,
                    'is_nullable': False,
                    'business_description': 'Number of loans currently in active status',
                    'technical_description': 'COUNT of loans where status IN (current, late)',
                    'data_classification': 'Public',
                    'example_values': '120, 125, 130'
                },
                {
                    'column_name': 'total_loan_value',
                    'column_type': 'DECIMAL(15,2)',
                    'column_order': 3,
                    'is_nullable': False,
                    'business_description': 'Total outstanding loan portfolio value',
                    'technical_description': 'SUM of remaining_balance from loans table',
                    'data_classification': 'Confidential',
                    'example_values': '5000000.00, 5250000.00'
                },
                {
                    'column_name': 'total_deposits',
                    'column_type': 'DECIMAL(15,2)',
                    'column_order': 4,
                    'is_nullable': False,
                    'business_description': 'Total deposit balance across all accounts',
                    'technical_description': 'SUM of balance from deposits table',
                    'data_classification': 'Confidential',
                    'example_values': '8000000.00, 8500000.00'
                },
                {
                    'column_name': 'avg_credit_score',
                    'column_type': 'DECIMAL(5,2)',
                    'column_order': 5,
                    'is_nullable': True,
                    'business_description': 'Average credit score of all customers',
                    'technical_description': 'AVG of credit_score from customers table',
                    'data_classification': 'Restricted',
                    'example_values': '720.50, 725.00, 730.25'
                }
            ],
            'v_customer_summary': [
                {
                    'column_name': 'customer_id',
                    'column_type': 'VARCHAR(20)',
                    'column_order': 1,
                    'is_nullable': False,
                    'is_primary_key': True,
                    'business_description': 'Unique identifier for each customer',
                    'technical_description': 'Primary key from customers table',
                    'data_classification': 'Confidential',
                    'example_values': 'CUST001, CUST002, CUST003'
                },
                {
                    'column_name': 'name',
                    'column_type': 'VARCHAR(100)',
                    'column_order': 2,
                    'is_nullable': False,
                    'business_description': 'Full name of the customer',
                    'technical_description': 'Customer name from customers table',
                    'data_classification': 'PII',
                    'example_values': 'John Smith, Jane Doe'
                },
                {
                    'column_name': 'segment',
                    'column_type': 'VARCHAR(20)',
                    'column_order': 3,
                    'is_nullable': True,
                    'business_description': 'Customer segmentation category',
                    'technical_description': 'Business-defined customer segment',
                    'data_classification': 'Internal',
                    'example_values': 'Premium, Standard, Basic',
                    'valid_values': 'Premium, Standard, Basic'
                },
                {
                    'column_name': 'total_products',
                    'column_type': 'INTEGER',
                    'column_order': 4,
                    'is_nullable': False,
                    'business_description': 'Total number of banking products held by customer',
                    'technical_description': 'COUNT of loans + deposits per customer',
                    'data_classification': 'Internal',
                    'example_values': '1, 2, 3, 4'
                },
                {
                    'column_name': 'total_relationship_value',
                    'column_type': 'DECIMAL(15,2)',
                    'column_order': 5,
                    'is_nullable': False,
                    'business_description': 'Total value of all customer relationships',
                    'technical_description': 'SUM of loan balances + deposit balances',
                    'data_classification': 'Confidential',
                    'example_values': '50000.00, 150000.00, 250000.00'
                }
            ],
            'v_loan_portfolio': [
                {
                    'column_name': 'loan_id',
                    'column_type': 'VARCHAR(20)',
                    'column_order': 1,
                    'is_nullable': False,
                    'is_primary_key': True,
                    'business_description': 'Unique identifier for each loan',
                    'technical_description': 'Primary key from loans table',
                    'data_classification': 'Confidential',
                    'example_values': 'LOAN001, LOAN002'
                },
                {
                    'column_name': 'customer_name',
                    'column_type': 'VARCHAR(100)',
                    'column_order': 2,
                    'is_nullable': False,
                    'business_description': 'Name of the loan holder',
                    'technical_description': 'Joined from customers.name',
                    'data_classification': 'PII',
                    'example_values': 'John Smith, Jane Doe'
                },
                {
                    'column_name': 'loan_type',
                    'column_type': 'VARCHAR(20)',
                    'column_order': 3,
                    'is_nullable': False,
                    'business_description': 'Type of loan product',
                    'technical_description': 'Loan product category',
                    'data_classification': 'Public',
                    'example_values': 'Mortgage, Auto, Personal',
                    'valid_values': 'Mortgage, Auto, Personal'
                },
                {
                    'column_name': 'status',
                    'column_type': 'VARCHAR(20)',
                    'column_order': 4,
                    'is_nullable': False,
                    'business_description': 'Current status of the loan',
                    'technical_description': 'Loan payment status',
                    'data_classification': 'Internal',
                    'example_values': 'Current, Late, Default',
                    'valid_values': 'Current, Late, Default, Closed'
                },
                {
                    'column_name': 'risk_score',
                    'column_type': 'DECIMAL(5,2)',
                    'column_order': 5,
                    'is_nullable': True,
                    'business_description': 'Calculated risk score for the loan',
                    'technical_description': 'Risk calculation based on payment history and credit score',
                    'data_classification': 'Restricted',
                    'example_values': '0.05, 0.15, 0.85'
                }
            ]
        }
        
        # Insert column metadata
        for view_name, columns in columns_metadata.items():
            for col in columns:
                column_id = str(uuid.uuid4())
                
                if self.provider == 'snowflake':
                    cursor.execute("""
                        INSERT INTO DATA_CATALOG_COLUMNS
                        (catalog_column_id, view_name, column_name, column_type,
                         column_order, is_nullable, is_primary_key, business_description,
                         technical_description, data_classification, example_values, valid_values)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, (
                        column_id, view_name, col['column_name'], col['column_type'],
                        col['column_order'], col['is_nullable'], 
                        col.get('is_primary_key', False), col['business_description'],
                        col['technical_description'], col['data_classification'],
                        col['example_values'], col.get('valid_values')
                    ))
                else:
                    cursor.execute("""
                        INSERT INTO data_catalog_columns
                        (catalog_column_id, view_name, column_name, column_type,
                         column_order, is_nullable, is_primary_key, business_description,
                         technical_description, data_classification, example_values, valid_values)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        column_id, view_name, col['column_name'], col['column_type'],
                        col['column_order'], col['is_nullable'], 
                        col.get('is_primary_key', False), col['business_description'],
                        col['technical_description'], col['data_classification'],
                        col['example_values'], col.get('valid_values')
                    ))
                    
    def _populate_examples(self):
        """Populate query examples"""
        print("  - Populating query examples...")
        cursor = self.data_service.connection.cursor()
        
        examples = [
            {
                'view_name': 'v_executive_dashboard',
                'example_type': 'Basic Query',
                'example_query': 'SELECT * FROM v_executive_dashboard',
                'example_description': 'Retrieve all executive dashboard metrics',
                'business_context': 'Used for daily executive briefings',
                'created_by': 'Analytics Team'
            },
            {
                'view_name': 'v_customer_summary',
                'example_type': 'Filtering',
                'example_query': "SELECT * FROM v_customer_summary WHERE segment = 'Premium' AND total_products > 2",
                'example_description': 'Find premium customers with multiple products',
                'business_context': 'Identify high-value customers for targeted campaigns',
                'created_by': 'Marketing Team'
            },
            {
                'view_name': 'v_loan_portfolio',
                'example_type': 'Risk Analysis',
                'example_query': "SELECT loan_type, COUNT(*) as count, AVG(risk_score) as avg_risk FROM v_loan_portfolio WHERE status = 'Late' GROUP BY loan_type",
                'example_description': 'Analyze risk by loan type for late payments',
                'business_context': 'Monthly risk assessment report',
                'created_by': 'Risk Team'
            },
            {
                'view_name': 'v_customer_summary',
                'example_type': 'Top Customers',
                'example_query': 'SELECT TOP 10 * FROM v_customer_summary ORDER BY total_relationship_value DESC',
                'example_description': 'Find top 10 customers by relationship value',
                'business_context': 'VIP customer identification',
                'created_by': 'Relationship Management'
            }
        ]
        
        for example in examples:
            example_id = str(uuid.uuid4())
            
            if self.provider == 'snowflake':
                cursor.execute("""
                    INSERT INTO DATA_CATALOG_EXAMPLES
                    (example_id, view_name, example_type, example_query,
                     example_description, business_context, created_by)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, (
                    example_id, example['view_name'], example['example_type'],
                    example['example_query'], example['example_description'],
                    example['business_context'], example['created_by']
                ))
            else:
                cursor.execute("""
                    INSERT INTO data_catalog_examples
                    (example_id, view_name, example_type, example_query,
                     example_description, business_context, created_by)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (
                    example_id, example['view_name'], example['example_type'],
                    example['example_query'], example['example_description'],
                    example['business_context'], example['created_by']
                ))
                
    def _populate_metrics(self):
        """Populate sample metrics"""
        print("  - Populating sample metrics...")
        cursor = self.data_service.connection.cursor()
        
        # Sample metrics
        metrics = [
            {
                'view_name': 'v_executive_dashboard',
                'column_name': 'total_customers',
                'metric_name': 'growth_rate',
                'metric_value': '5.2%',
                'metric_date': datetime.now().date()
            },
            {
                'view_name': 'v_executive_dashboard',
                'column_name': 'avg_credit_score',
                'metric_name': 'monthly_average',
                'metric_value': '725.5',
                'metric_date': datetime.now().date()
            },
            {
                'view_name': 'v_loan_portfolio',
                'column_name': None,
                'metric_name': 'default_rate',
                'metric_value': '2.1%',
                'metric_date': datetime.now().date()
            },
            {
                'view_name': 'v_customer_summary',
                'column_name': None,
                'metric_name': 'avg_products_per_customer',
                'metric_value': '2.3',
                'metric_date': datetime.now().date()
            }
        ]
        
        for metric in metrics:
            metric_id = str(uuid.uuid4())
            
            if self.provider == 'snowflake':
                cursor.execute("""
                    INSERT INTO DATA_CATALOG_METRICS
                    (metric_id, view_name, column_name, metric_name,
                     metric_value, metric_date)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, (
                    metric_id, metric['view_name'], metric['column_name'],
                    metric['metric_name'], metric['metric_value'],
                    metric['metric_date']
                ))
            else:
                cursor.execute("""
                    INSERT INTO data_catalog_metrics
                    (metric_id, view_name, column_name, metric_name,
                     metric_value, metric_date)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (
                    metric_id, metric['view_name'], metric['column_name'],
                    metric['metric_name'], metric['metric_value'],
                    metric['metric_date']
                ))


def main():
    parser = argparse.ArgumentParser(description='Populate data catalog metadata')
    parser.add_argument(
        '--provider', 
        choices=['local', 'snowflake'], 
        default='local',
        help='Data provider to use (default: local)'
    )
    
    args = parser.parse_args()
    
    try:
        populator = CatalogPopulator(args.provider)
        populator.populate_catalog()
        
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()