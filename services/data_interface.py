from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional
import pandas as pd


class DataInterface(ABC):
    """Abstract base class for data services (local or cloud)"""
    
    @abstractmethod
    def connect(self) -> bool:
        """Establish connection to the data source"""
        pass
    
    @abstractmethod
    def disconnect(self) -> None:
        """Close connection to the data source"""
        pass
    
    @abstractmethod
    def execute_query(self, query: str, params: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
        """Execute a SQL query and return results as a DataFrame"""
        pass
    
    @abstractmethod
    def execute_structured_query(self, structured_query: Dict[str, Any]) -> Dict[str, Any]:
        """Execute a structured query (from agents) and return formatted results"""
        pass
    
    @abstractmethod
    def get_table_schema(self, table_name: str) -> Dict[str, Any]:
        """Get schema information for a specific table"""
        pass
    
    @abstractmethod
    def get_available_tables(self) -> List[str]:
        """Get list of available tables in the data source"""
        pass
    
    @abstractmethod
    def validate_connection(self) -> bool:
        """Test if the connection is valid and working"""
        pass
    
    @abstractmethod
    def get_connection_info(self) -> Dict[str, Any]:
        """Get information about the current connection"""
        pass
    
    def build_sql_from_structured_query(self, query_dict: Dict[str, Any]) -> str:
        """Convert structured query to SQL - can be overridden by implementations"""
        entity = query_dict.get('entity', 'data')
        filters = query_dict.get('filters', {})
        metrics = query_dict.get('metrics', ['count'])
        aggregations = query_dict.get('aggregations', [])
        time_period = query_dict.get('time_period', {})
        
        # Map entity to table name
        table_map = {
            'loans': 'loans',
            'loan': 'loans',
            'deposits': 'deposits',
            'deposit': 'deposits',
            'customers': 'customers',
            'customer': 'customers'
        }
        table_name = table_map.get(entity.lower(), entity)
        
        # Build SELECT clause
        select_parts = []
        for metric in metrics:
            if metric == 'count':
                select_parts.append(f"COUNT(*) as total_count")
            elif metric == 'sum':
                select_parts.append(f"SUM(amount) as total_amount")
            elif metric == 'average':
                select_parts.append(f"AVG(amount) as average_amount")
            elif metric == 'default_rate' and table_name == 'loans':
                select_parts.append(f"SUM(CASE WHEN status = 'default' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as default_rate")
        
        # Build WHERE clause
        where_parts = []
        for key, value in filters.items():
            if isinstance(value, str):
                where_parts.append(f"{key} = '{value}'")
            else:
                where_parts.append(f"{key} = {value}")
        
        # Handle time period
        if isinstance(time_period, dict):
            if 'start' in time_period and 'end' in time_period:
                where_parts.append(f"date >= '{time_period['start']}' AND date <= '{time_period['end']}'")
        
        # Build GROUP BY clause
        group_by_parts = []
        if 'by_category' in aggregations and table_name == 'loans':
            group_by_parts.append('loan_type')
            select_parts.insert(0, 'loan_type')
        elif 'by_type' in aggregations and table_name == 'deposits':
            group_by_parts.append('account_type')
            select_parts.insert(0, 'account_type')
        
        # Construct SQL
        sql = f"SELECT {', '.join(select_parts)} FROM {table_name}"
        if where_parts:
            sql += f" WHERE {' AND '.join(where_parts)}"
        if group_by_parts:
            sql += f" GROUP BY {', '.join(group_by_parts)}"
        
        return sql