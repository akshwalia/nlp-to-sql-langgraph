from typing import Dict, List, Any, Tuple, Optional
from sqlalchemy import text
import pandas as pd
import logging

logger = logging.getLogger(__name__)


class QueryExecutor:
    """
    Handles SQL query execution with support for both connection pooling and direct connections
    """
    
    def __init__(self, engine, connection_manager=None, workspace_id=None):
        """
        Initialize the query executor
        
        Args:
            engine: SQLAlchemy engine instance
            connection_manager: Optional connection manager instance
            workspace_id: Workspace ID for connection pooling
        """
        self.engine = engine
        self.connection_manager = connection_manager
        self.workspace_id = workspace_id
    
    def execute_query(self, query: str) -> Tuple[bool, Optional[List[Dict[str, Any]]], Optional[str]]:
        """
        Execute a SQL query and return the results
        
        Args:
            query: SQL query to execute
            
        Returns:
            Tuple of (success, results, error_message)
        """
        try:
            # Use connection pool if available, otherwise fall back to engine
            if self.connection_manager and self.workspace_id:
                with self.connection_manager.get_connection(self.workspace_id) as conn:
                    return self._execute_with_connection(query, conn)
            else:
                # Fallback to SQLAlchemy engine
                with self.engine.connect() as connection:
                    return self._execute_with_sqlalchemy(query, connection)
                
        except Exception as e:
            logger.error(f"Error executing query: {e}")
            return False, None, str(e)
    
    def _execute_with_connection(self, query: str, conn) -> Tuple[bool, Optional[List[Dict[str, Any]]], Optional[str]]:
        """
        Execute query using psycopg2 connection
        
        Args:
            query: SQL query to execute
            conn: Database connection
            
        Returns:
            Tuple of (success, results, error_message)
        """
        cursor = conn.cursor()
        try:
            cursor.execute(query)
            
            # Check if query returns rows
            if cursor.description:
                # Get column names
                columns = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                
                # Convert to list of dictionaries
                data = []
                for row in rows:
                    row_dict = {}
                    for idx, column in enumerate(columns):
                        value = row[idx]
                        # Convert non-serializable types to strings for JSON compatibility
                        if isinstance(value, (pd.Timestamp, pd.Timedelta)):
                            value = str(value)
                        row_dict[column] = value
                    data.append(row_dict)
                
                # Commit the transaction for write operations
                if self._is_write_operation(query):
                    conn.commit()
                
                return True, data, None
            else:
                # For non-SELECT queries (INSERT, UPDATE, DELETE), return rowcount
                affected_rows = cursor.rowcount
                
                # Commit the transaction for write operations
                if self._is_write_operation(query):
                    conn.commit()
                
                return True, affected_rows, None
                
        except Exception as e:
            logger.error(f"Error executing query with connection: {e}")
            return False, None, str(e)
        finally:
            cursor.close()
    
    def _execute_with_sqlalchemy(self, query: str, connection) -> Tuple[bool, Optional[List[Dict[str, Any]]], Optional[str]]:
        """
        Execute query using SQLAlchemy connection
        
        Args:
            query: SQL query to execute
            connection: SQLAlchemy connection
            
        Returns:
            Tuple of (success, results, error_message)
        """
        try:
            result = connection.execute(text(query))
            
            if result.returns_rows:
                # Convert result to a list of dictionaries
                columns = result.keys()
                data = []
                for row in result:
                    row_dict = {}
                    for idx, column in enumerate(columns):
                        value = row[idx]
                        # Convert non-serializable types to strings for JSON compatibility
                        if isinstance(value, (pd.Timestamp, pd.Timedelta)):
                            value = str(value)
                        row_dict[column] = value
                    data.append(row_dict)
                
                # Commit for write operations
                if self._is_write_operation(query):
                    connection.commit()
                
                return True, data, None
            else:
                # For non-SELECT queries, return rowcount
                affected_rows = result.rowcount
                
                # Commit for write operations
                if self._is_write_operation(query):
                    connection.commit()
                
                return True, affected_rows, None
                
        except Exception as e:
            logger.error(f"Error executing query with SQLAlchemy: {e}")
            return False, None, str(e)
    
    def _is_write_operation(self, query: str) -> bool:
        """
        Check if a query is a write operation that needs to be committed
        
        Args:
            query: SQL query string
            
        Returns:
            True if it's a write operation, False otherwise
        """
        query_upper = query.strip().upper()
        write_operations = [
            'INSERT', 'UPDATE', 'DELETE', 'ALTER', 'DROP', 'CREATE', 
            'TRUNCATE', 'MERGE', 'RENAME'
        ]
        return any(query_upper.startswith(op) for op in write_operations)
    
    def execute_multiple_queries(self, queries: List[str]) -> List[Tuple[bool, Optional[List[Dict[str, Any]]], Optional[str]]]:
        """
        Execute multiple queries individually (not in a transaction)
        
        Args:
            queries: List of SQL queries to execute
            
        Returns:
            List of tuples (success, results, error_message) for each query
        """
        results = []
        for query in queries:
            result = self.execute_query(query)
            results.append(result)
        return results
    
    def test_connection(self) -> bool:
        """
        Test if the database connection is working
        
        Returns:
            True if connection is working, False otherwise
        """
        try:
            success, result, error = self.execute_query("SELECT 1")
            return success and result is not None
        except Exception as e:
            logger.error(f"Connection test failed: {e}")
            return False
    
    def get_connection_info(self) -> Dict[str, Any]:
        """
        Get information about the current connection setup
        
        Returns:
            Dictionary with connection information
        """
        return {
            "has_connection_manager": self.connection_manager is not None,
            "has_workspace_id": self.workspace_id is not None,
            "engine_url": str(self.engine.url) if self.engine else None,
            "connection_test": self.test_connection()
        } 