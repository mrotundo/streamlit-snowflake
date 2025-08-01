# Pandas Segmentation Fault Workaround

## Issue Description

When running `setup_database.py`, a segmentation fault occurs when importing pandas on macOS 11.7.10 with Python 3.11.5. This appears to be a compatibility issue between the pandas/numpy binary wheels and the system libraries.

## Error Details

```bash
/bin/bash: line 1: 20273 Segmentation fault: 11  python3 scripts/setup_database.py
```

The segmentation fault specifically occurs when:
- Importing pandas (even with different versions tried: 2.3.1, 2.0.3, 1.5.3)
- NumPy imports successfully, but pandas fails
- This happens in the virtual environment with Python 3.11.5

## Root Cause

The issue is likely due to:
1. Binary wheel incompatibility with macOS 11.7.10 system libraries
2. Potential architecture mismatch (though system shows x86_64)
3. C library dependencies that pandas relies on

## Workaround Solution

Created `scripts/setup_database_workaround.py` that:
1. Removes pandas dependency entirely
2. Uses only standard Python libraries (sqlite3, datetime, random)
3. Creates all necessary tables and mock data
4. Maintains the same database schema as the original script

### Usage

```bash
# Basic usage
python scripts/setup_database_workaround.py

# With options
python scripts/setup_database_workaround.py --customers 1000 --drop-existing

# Verify only
python scripts/setup_database_workaround.py --verify-only
```

### Features Implemented

- All core tables: customers, loans, deposits, transactions
- Data lineage tables: jobs, job_runs, source_files, etc.
- Basic views: v_customer_summary, v_loan_portfolio, v_deposit_summary, v_customer_products
- Mock data generation with realistic relationships
- Proper foreign key constraints and indexes

### Limitations

The workaround script doesn't include:
1. Complex view creation (Level 2 and 3 views) - these could be added if needed
2. Full transaction generation - simplified for the workaround
3. Snowflake support - only SQLite is supported

## Long-term Solutions

1. **Rebuild pandas from source**: This was attempted but takes too long and may still fail
2. **Use conda instead of pip**: Conda often has better-compiled binaries for macOS
3. **Downgrade Python**: Use Python 3.9 or 3.10 which have better pandas compatibility
4. **Use Docker**: Run the setup in a containerized environment
5. **Fix the virtual environment**: Recreate it with specific versions known to work

## Application Impact

The application should still work normally as:
1. The database schema is identical
2. Mock data follows the same patterns
3. The LocalDataService will work with the created SQLite database

However, any code that directly uses pandas DataFrames will still face the segmentation fault issue. The application may need modifications to handle data without pandas or use alternative data processing libraries.

## Next Steps

1. Test if the Streamlit application runs successfully with the created database
2. Consider implementing a pandas-free version of LocalDataService if needed
3. Investigate alternative data processing libraries (polars, duckdb, etc.)
4. Report the issue to pandas/numpy maintainers with system details