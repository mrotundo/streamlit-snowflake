# Database Setup Scripts

This directory contains scripts for setting up and managing the banking database.

## setup_database.py

This script creates database tables and generates realistic mock banking data for testing and development.

### Features

- Creates all necessary tables (customers, loans, deposits, transactions)
- Generates realistic mock data with proper relationships
- Supports both SQLite (local) and Snowflake databases
- Configurable number of customers and related data
- Data verification after generation

### Usage

```bash
# Basic usage - creates local SQLite database with 1000 customers
python scripts/setup_database.py

# Create Snowflake database with 5000 customers
python scripts/setup_database.py --provider snowflake --customers 5000

# Drop existing tables and recreate
python scripts/setup_database.py --drop-existing

# Only create tables without data
python scripts/setup_database.py --skip-data

# Verify existing data
python scripts/setup_database.py --verify-only
```

### Command Line Options

- `--provider`: Database provider (`local` or `snowflake`, default: `local`)
- `--customers`: Number of customers to generate (default: 1000)
- `--drop-existing`: Drop existing tables before creating new ones
- `--skip-data`: Only create tables, skip data generation
- `--verify-only`: Only verify existing data counts

### Generated Data

The script generates:

1. **Customers**: With realistic profiles including:
   - Customer segments (high_value, growth, maintain, at_risk, new)
   - Credit scores based on segment
   - Annual income ranges
   - Employment status
   - Join dates

2. **Deposit Accounts**: Multiple account types per customer:
   - Checking accounts
   - Savings accounts
   - Certificates of Deposit (CD)
   - Money market accounts
   - Realistic balances based on customer segment

3. **Loans**: Various loan products:
   - Mortgages
   - Auto loans
   - Personal loans
   - Business loans
   - Appropriate interest rates based on credit score
   - Realistic payment histories

4. **Transactions**: Historical transaction data:
   - Deposit and withdrawal transactions
   - Loan payments
   - Various transaction categories
   - Realistic transaction patterns

### Data Relationships

- Each customer has 1-4 financial products
- 90% of customers have at least one deposit account
- 60% of customers have at least one loan
- Transaction history is generated for all accounts and loans
- All foreign key relationships are properly maintained

### Database Schema

The script creates the following tables:

- `customers`: Customer profiles and demographics
- `deposits`: Deposit account information
- `loans`: Loan account details
- `transactions`: Transaction history for all accounts

### Examples

```bash
# Quick setup for development
python scripts/setup_database.py --customers 100

# Full production-like dataset
python scripts/setup_database.py --customers 10000 --drop-existing

# Snowflake setup (requires credentials in .env)
python scripts/setup_database.py --provider snowflake --customers 5000
```

### Notes

- The local SQLite database is created at `data/banking.db`
- Snowflake connection requires proper credentials in your `.env` file
- Generated data includes realistic patterns (e.g., high-value customers have better credit scores and larger balances)
- Transaction dates are generated to provide historical data for analytics