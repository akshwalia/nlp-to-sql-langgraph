from typing import Dict, List, Any, Tuple, Optional
from sqlalchemy import text
import pandas as pd
import logging
import time

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
        logger.info(f"Executing SQL query for workspace: {self.workspace_id}")
        logger.debug(f"Query: {query[:200]}{'...' if len(query) > 200 else ''}")
        
        try:
            # Use connection pool if available, otherwise fall back to engine
            if self.connection_manager and self.workspace_id:
                logger.debug(f"Using connection pool for workspace: {self.workspace_id}")
                with self.connection_manager.get_connection(self.workspace_id) as conn:
                    return self._execute_with_connection(query, conn)
            else:
                logger.debug("Using SQLAlchemy engine (no connection pool)")
                # Fallback to SQLAlchemy engine
                with self.engine.connect() as connection:
                    return self._execute_with_sqlalchemy(query, connection)
                
        except Exception as e:
            logger.error(f"Error executing query: {e}")
            return False, None, str(e)
    
    def _execute_with_connection(self, query: str, conn) -> Tuple[bool, Optional[List[Dict[str, Any]]], Optional[str]]:
        """
        Execute query using connection pool connection
        
        Args:
            query: SQL query to execute
            conn: Database connection from pool
            
        Returns:
            Tuple of (success, results, error_message)
        """
        logger.debug("Executing query with connection pool")
        
        try:
            cursor = conn.cursor()
            logger.debug("Database cursor created")
            
            # Check if this is a write operation
            is_write = self._is_write_operation(query)
            logger.debug(f"Query type: {'WRITE' if is_write else 'READ'}")
            
            # Execute query
            start_time = time.time()
            cursor.execute(query)
            execution_time = time.time() - start_time
            
            if is_write:
                logger.info(f"Write operation completed in {execution_time:.3f}s")
                affected_rows = cursor.rowcount
                logger.debug(f"Affected rows: {affected_rows}")
                
                # For write operations, return success with affected row count
                return True, [{"affected_rows": affected_rows}], None
            else:
                # For read operations, fetch results
                logger.debug("Fetching query results")
                results = []
                rows = cursor.fetchall()
                
                if rows:
                    # Get column names
                    column_names = [desc[0] for desc in cursor.description]
                    logger.debug(f"Result columns: {column_names}")
                    
                    # Convert rows to dictionaries
                    for row in rows:
                        row_dict = {}
                        for i, value in enumerate(row):
                            row_dict[column_names[i]] = value
                        results.append(row_dict)
                
                logger.info(f"Read operation completed in {execution_time:.3f}s, returned {len(results)} rows")
                return True, results, None
                
        except Exception as e:
            logger.error(f"Error executing query with connection pool: {e}")
            return False, None, str(e)
        finally:
            if 'cursor' in locals():
                cursor.close()
                logger.debug("Database cursor closed")
    
    def _execute_with_sqlalchemy(self, query: str, connection) -> Tuple[bool, Optional[List[Dict[str, Any]]], Optional[str]]:
        """
        Execute query using SQLAlchemy connection
        
        Args:
            query: SQL query to execute
            connection: SQLAlchemy connection
            
        Returns:
            Tuple of (success, results, error_message)
        """
        logger.debug("Executing query with SQLAlchemy")
        
        try:
            # Check if this is a write operation
            is_write = self._is_write_operation(query)
            logger.debug(f"Query type: {'WRITE' if is_write else 'READ'}")
            
            # Execute query
            start_time = time.time()
            result = connection.execute(text(query))
            execution_time = time.time() - start_time
            
            if is_write:
                logger.info(f"Write operation completed in {execution_time:.3f}s")
                affected_rows = result.rowcount
                logger.debug(f"Affected rows: {affected_rows}")
                
                return True, [{"affected_rows": affected_rows}], None
            else:
                # For read operations, fetch results
                logger.debug("Fetching query results")
                results = []
                rows = result.fetchall()
                
                if rows:
                    # Get column names
                    column_names = list(result.keys())
                    logger.debug(f"Result columns: {column_names}")
                    
                    # Convert rows to dictionaries
                    for row in rows:
                        row_dict = {}
                        for i, value in enumerate(row):
                            row_dict[column_names[i]] = value
                        results.append(row_dict)
                
                logger.info(f"Read operation completed in {execution_time:.3f}s, returned {len(results)} rows")
                return True, results, None
                
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