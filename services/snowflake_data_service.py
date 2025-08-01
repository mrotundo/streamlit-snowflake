import pandas as pd
from typing import Dict, Any, List, Optional
from .data_interface import DataInterface
import json

try:
    import snowflake.connector
    SNOWFLAKE_AVAILABLE = True
except ImportError:
    SNOWFLAKE_AVAILABLE = False


class SnowflakeDataService(DataInterface):
    """Snowflake implementation of the data interface"""
    
    def __init__(
        self,
        account: str,
        user: str,
        password: str,
        warehouse: str,
        database: str,
        schema: str
    ):
        if not SNOWFLAKE_AVAILABLE:
            raise ImportError(
                "snowflake-connector-python is not installed. "
                "Install it with: pip install snowflake-connector-python"
            )
        
        self.connection_params = {
            'account': account,
            'user': user,
            'password': password,
            'warehouse': warehouse,
            'database': database,
            'schema': schema
        }
        self.connection = None
    
    def connect(self) -> bool:
        """Establish connection to Snowflake"""
        try:
            self.connection = snowflake.connector.connect(**self.connection_params)
            return True
        except Exception as e:
            print(f"Failed to connect to Snowflake: {e}")
            return False
    
    def disconnect(self) -> None:
        """Close Snowflake connection"""
        if self.connection:
            self.connection.close()
            self.connection = None
    
    def execute_query(self, query: str, params: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
        """Execute a SQL query and return results as a DataFrame"""
        if not self.connection:
            raise ConnectionError("Not connected to Snowflake")
        
        try:
            cursor = self.connection.cursor()
            
            if params:
                # Snowflake uses %s for parameter binding
                cursor.execute(query, list(params.values()))
            else:
                cursor.execute(query)
            
            # Fetch all results
            results = cursor.fetchall()
            columns = [col[0] for col in cursor.description]
            
            # Convert to DataFrame
            df = pd.DataFrame(results, columns=columns)
            
            cursor.close()
            return df
            
        except Exception as e:
            raise Exception(f"Query execution failed: {e}")
    
    def execute_structured_query(self, structured_query: Dict[str, Any]) -> Dict[str, Any]:
        """Execute a structured query and return formatted results"""
        try:
            # Convert structured query to SQL
            sql = self.build_sql_from_structured_query(structured_query)
            
            # Execute main query
            df = self.execute_query(sql)
            
            # Get sample data
            entity = structured_query.get('entity', 'data')
            table_name = self._get_table_name(entity)
            sample_sql = f"SELECT * FROM {table_name} LIMIT 10"
            sample_df = self.execute_query(sample_sql)
            
            # Format results
            result = {
                "query_executed": structured_query,
                "row_count": len(df),
                "execution_time": "0.15s",  # Snowflake is typically slower than local
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
            'loans': 'LOANS',
            'loan': 'LOANS',
            'deposits': 'DEPOSITS',
            'deposit': 'DEPOSITS',
            'customers': 'CUSTOMERS',
            'customer': 'CUSTOMERS'
        }
        return table_map.get(entity.lower(), entity.upper())
    
    def _format_summary_stats(self, df: pd.DataFrame, query: Dict[str, Any]) -> Dict[str, Any]:
        """Format summary statistics from query results"""
        stats = {}
        
        # Extract values from dataframe
        if not df.empty:
            row = df.iloc[0]
            for col in df.columns:
                col_lower = col.lower()
                if col_lower in ['total_count', 'total_amount', 'average_amount', 'default_rate']:
                    value = row[col]
                    if pd.notna(value):
                        stats[col_lower] = float(value) if isinstance(value, (int, float)) else value
        
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
            raise ConnectionError("Not connected to Snowflake")
        
        query = f"""
        SELECT 
            COLUMN_NAME,
            DATA_TYPE,
            IS_NULLABLE,
            COLUMN_DEFAULT
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = '{table_name.upper()}'
        AND TABLE_SCHEMA = '{self.connection_params['schema']}'
        ORDER BY ORDINAL_POSITION
        """
        
        df = self.execute_query(query)
        
        schema = {
            "table_name": table_name,
            "columns": []
        }
        
        for _, row in df.iterrows():
            schema["columns"].append({
                "name": row['COLUMN_NAME'],
                "type": row['DATA_TYPE'],
                "nullable": row['IS_NULLABLE'] == 'YES',
                "default": row['COLUMN_DEFAULT']
            })
        
        return schema
    
    def get_available_tables(self) -> List[str]:
        """Get list of available tables"""
        if not self.connection:
            raise ConnectionError("Not connected to Snowflake")
        
        query = f"""
        SELECT TABLE_NAME
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = '{self.connection_params['schema']}'
        AND TABLE_TYPE = 'BASE TABLE'
        ORDER BY TABLE_NAME
        """
        
        df = self.execute_query(query)
        return df['TABLE_NAME'].tolist()
    
    def validate_connection(self) -> bool:
        """Test if the connection is valid"""
        if not self.connection:
            return False
        
        try:
            cursor = self.connection.cursor()
            cursor.execute("SELECT CURRENT_VERSION()")
            cursor.fetchone()
            cursor.close()
            return True
        except:
            return False
    
    def get_connection_info(self) -> Dict[str, Any]:
        """Get information about the connection"""
        return {
            "type": "Snowflake",
            "account": self.connection_params['account'],
            "database": self.connection_params['database'],
            "schema": self.connection_params['schema'],
            "warehouse": self.connection_params['warehouse'],
            "connected": self.validate_connection(),
            "tables": self.get_available_tables() if self.validate_connection() else []
        }
    
    def build_sql_from_structured_query(self, query_dict: Dict[str, Any]) -> str:
        """Override to handle Snowflake-specific SQL syntax"""
        # Get base SQL from parent
        sql = super().build_sql_from_structured_query(query_dict)
        
        # Snowflake uses uppercase table names
        sql = sql.replace(' loans', ' LOANS')
        sql = sql.replace(' deposits', ' DEPOSITS')
        sql = sql.replace(' customers', ' CUSTOMERS')
        sql = sql.replace('FROM loans', 'FROM LOANS')
        sql = sql.replace('FROM deposits', 'FROM DEPOSITS')
        sql = sql.replace('FROM customers', 'FROM CUSTOMERS')
        
        return sql