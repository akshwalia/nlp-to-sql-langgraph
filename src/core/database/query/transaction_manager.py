from typing import Dict, List, Any, Tuple, Optional
from sqlalchemy import text
import pandas as pd
import logging
import time

logger = logging.getLogger(__name__)


class TransactionManager:
    """
    Handles transaction-based query execution with rollback support
    """
    
    def __init__(self, engine, connection_manager=None, workspace_id=None):
        """
        Initialize the transaction manager
        
        Args:
            engine: SQLAlchemy engine instance
            connection_manager: Optional connection manager instance
            workspace_id: Workspace ID for connection pooling
        """
        self.engine = engine
        self.connection_manager = connection_manager
        self.workspace_id = workspace_id
    
    def execute_query_with_transaction(self, queries: List[str]) -> Tuple[bool, List[Any], Optional[str]]:
        """
        Execute multiple queries in a single transaction with rollback support
        
        Args:
            queries: List of SQL queries to execute in transaction
            
        Returns:
            Tuple of (success, results_list, error_message)
        """
        logger.info(f"Starting transaction execution for workspace: {self.workspace_id}")
        logger.debug(f"Number of queries in transaction: {len(queries)}")
        
        results = []
        
        try:
            # Use connection pool if available, otherwise fall back to engine
            if self.connection_manager and self.workspace_id:
                logger.debug(f"Using connection pool for transaction in workspace: {self.workspace_id}")
                with self.connection_manager.get_connection(self.workspace_id) as conn:
                    return self._execute_transaction_with_connection(queries, conn)
            else:
                logger.debug("Using SQLAlchemy engine for transaction (no connection pool)")
                # Fallback to SQLAlchemy engine with transaction
                with self.engine.begin() as transaction:
                    return self._execute_transaction_with_sqlalchemy(queries, transaction)
                
        except Exception as e:
            logger.error(f"Error executing transaction: {e}")
            return False, [], str(e)
    
    def _execute_transaction_with_connection(self, queries: List[str], conn) -> Tuple[bool, List[Any], Optional[str]]:
        """
        Execute transaction using connection pool connection
        
        Args:
            queries: List of SQL queries to execute
            conn: Database connection from pool
            
        Returns:
            Tuple of (success, results_list, error_message)
        """
        logger.debug("Starting transaction with connection pool")
        
        results = []
        cursor = None
        
        try:
            cursor = conn.cursor()
            logger.debug("Database cursor created for transaction")
            
            # Start transaction
            logger.debug("Starting database transaction")
            
            for i, query in enumerate(queries, 1):
                logger.debug(f"Executing query {i}/{len(queries)} in transaction")
                logger.debug(f"Query: {query[:200]}{'...' if len(query) > 200 else ''}")
                
                # Execute each query
                start_time = time.time()
                cursor.execute(query)
                execution_time = time.time() - start_time
                
                # Check if query returns rows
                if cursor.description:
                    # Get column names
                    columns = [desc[0] for desc in cursor.description]
                    rows = cursor.fetchall()
                    
                    # Convert to list of dictionaries
                    query_results = []
                    for row in rows:
                        row_dict = {}
                        for idx, column in enumerate(columns):
                            value = row[idx]
                            # Convert non-serializable types to strings for JSON compatibility
                            if isinstance(value, (pd.Timestamp, pd.Timedelta)):
                                value = str(value)
                            row_dict[column] = value
                        query_results.append(row_dict)
                    
                    results.append(query_results)
                    logger.debug(f"Query {i} completed in {execution_time:.3f}s, returned {len(query_results)} rows")
                else:
                    # For non-SELECT queries, return rowcount
                    affected_rows = cursor.rowcount
                    results.append([{"affected_rows": affected_rows}])
                    logger.debug(f"Query {i} completed in {execution_time:.3f}s, affected {affected_rows} rows")
            
            # Commit transaction
            logger.info("Committing transaction")
            conn.commit()
            
            logger.info(f"Transaction completed successfully with {len(queries)} queries")
            return True, results, None
            
        except Exception as e:
            logger.error(f"Error in transaction, rolling back: {e}")
            try:
                if conn:
                    conn.rollback()
                    logger.info("Transaction rolled back successfully")
            except Exception as rollback_error:
                logger.error(f"Error during rollback: {rollback_error}")
            
            return False, [], str(e)
            
        finally:
            if cursor:
                cursor.close()
                logger.debug("Database cursor closed")
    
    def _execute_transaction_with_sqlalchemy(self, queries: List[str], transaction) -> Tuple[bool, List[Any], Optional[str]]:
        """
        Execute transaction using SQLAlchemy
        
        Args:
            queries: List of SQL queries to execute
            transaction: SQLAlchemy transaction
            
        Returns:
            Tuple of (success, results_list, error_message)
        """
        logger.debug("Starting transaction with SQLAlchemy")
        
        results = []
        
        try:
            for i, query in enumerate(queries, 1):
                logger.debug(f"Executing query {i}/{len(queries)} in transaction")
                logger.debug(f"Query: {query[:200]}{'...' if len(query) > 200 else ''}")
                
                # Execute each query
                start_time = time.time()
                result = transaction.execute(text(query))
                execution_time = time.time() - start_time
                
                if result.returns_rows:
                    # Convert result to a list of dictionaries
                    columns = result.keys()
                    query_results = []
                    for row in result:
                        row_dict = {}
                        for idx, column in enumerate(columns):
                            value = row[idx]
                            # Convert non-serializable types to strings for JSON compatibility
                            if isinstance(value, (pd.Timestamp, pd.Timedelta)):
                                value = str(value)
                            row_dict[column] = value
                        query_results.append(row_dict)
                    
                    results.append(query_results)
                    logger.debug(f"Query {i} completed in {execution_time:.3f}s, returned {len(query_results)} rows")
                else:
                    # For non-SELECT queries, return rowcount
                    affected_rows = result.rowcount
                    results.append([{"affected_rows": affected_rows}])
                    logger.debug(f"Query {i} completed in {execution_time:.3f}s, affected {affected_rows} rows")
            
            logger.info(f"Transaction completed successfully with {len(queries)} queries")
            return True, results, None
            
        except Exception as e:
            logger.error(f"Error in transaction, will be rolled back: {e}")
            # SQLAlchemy will automatically rollback on exception
            return False, [], str(e)
    
    def execute_batch_with_savepoints(self, query_batches: List[List[str]]) -> List[Tuple[bool, List[Any], Optional[str]]]:
        """
        Execute multiple batches of queries with savepoints
        Each batch is executed in its own savepoint, allowing partial rollback
        
        Args:
            query_batches: List of query batches, each batch is a list of queries
            
        Returns:
            List of results for each batch
        """
        batch_results = []
        
        try:
            if self.connection_manager and self.workspace_id:
                with self.connection_manager.get_connection(self.workspace_id) as conn:
                    conn.autocommit = False
                    cursor = conn.cursor()
                    
                    try:
                        for batch_idx, batch in enumerate(query_batches):
                            savepoint_name = f"sp_batch_{batch_idx}"
                            cursor.execute(f"SAVEPOINT {savepoint_name}")
                            
                            try:
                                # Execute this batch
                                batch_result = self._execute_batch_queries(batch, cursor)
                                batch_results.append((True, batch_result, None))
                                
                            except Exception as e:
                                # Rollback to savepoint on batch failure
                                cursor.execute(f"ROLLBACK TO SAVEPOINT {savepoint_name}")
                                batch_results.append((False, [], f"Batch {batch_idx + 1} failed: {str(e)}"))
                                logger.error(f"Batch {batch_idx + 1} failed, rolled back to savepoint: {e}")
                        
                        # Commit all successful batches
                        conn.commit()
                        cursor.close()
                        
                    except Exception as e:
                        conn.rollback()
                        cursor.close()
                        logger.error(f"Transaction failed: {e}")
                        
            else:
                # Fallback: execute each batch as separate transaction
                for batch in query_batches:
                    batch_result = self.execute_query_with_transaction(batch)
                    batch_results.append(batch_result)
                    
        except Exception as e:
            logger.error(f"Error executing batch with savepoints: {e}")
            batch_results.append((False, [], str(e)))
        
        return batch_results
    
    def _execute_batch_queries(self, queries: List[str], cursor) -> List[Dict[str, Any]]:
        """
        Execute a batch of queries using the provided cursor
        
        Args:
            queries: List of SQL queries
            cursor: Database cursor
            
        Returns:
            List of query results
        """
        results = []
        
        for i, query in enumerate(queries):
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
                
                results.append({
                    "query_number": i + 1,
                    "sql": query,
                    "success": True,
                    "results": data,
                    "affected_rows": len(data),
                    "error": None
                })
            else:
                # For non-SELECT queries, return rowcount
                affected_rows = cursor.rowcount
                results.append({
                    "query_number": i + 1,
                    "sql": query,
                    "success": True,
                    "results": [],
                    "affected_rows": affected_rows,
                    "error": None
                })
        
        return results 