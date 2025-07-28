import time
import threading
import sqlite3
import os
from typing import Dict, Optional, Any
from contextlib import contextmanager
from queue import Queue, Empty
import logging

logger = logging.getLogger(__name__)


class SQLiteConnectionPool:
    """
    Simple connection pool for SQLite databases
    """
    
    def __init__(self, db_path: str, max_connections: int = 10):
        self.db_path = db_path
        self.max_connections = max_connections
        self._pool = Queue(maxsize=max_connections)
        self._created_connections = 0
        self._lock = threading.RLock()
        
        # Ensure database directory exists
        os.makedirs(os.path.dirname(db_path) if os.path.dirname(db_path) else '.', exist_ok=True)
    
    def _create_connection(self):
        """Create a new SQLite connection"""
        conn = sqlite3.connect(
            self.db_path,
            check_same_thread=False,  # Allow sharing between threads
            timeout=30.0  # 30 second timeout for database locks
        )
        # Enable foreign key constraints
        conn.execute("PRAGMA foreign_keys = ON")
        # Set row factory for dict-like access
        conn.row_factory = sqlite3.Row
        return conn
    
    def get_connection(self):
        """Get a connection from the pool"""
        try:
            # Try to get an existing connection from the pool
            conn = self._pool.get_nowait()
            # Test if connection is still valid
            try:
                conn.execute("SELECT 1")
                return conn
            except sqlite3.Error:
                # Connection is invalid, create a new one
                conn.close()
        except Empty:
            pass
        
        # Create a new connection if pool is empty or connection was invalid
        with self._lock:
            if self._created_connections < self.max_connections:
                self._created_connections += 1
                return self._create_connection()
            else:
                # Wait for a connection to become available
                try:
                    return self._pool.get(timeout=10)
                except Empty:
                    raise Exception("Connection pool exhausted")
    
    def put_connection(self, conn):
        """Return a connection to the pool"""
        if conn and not conn.in_transaction:
            try:
                self._pool.put_nowait(conn)
            except:
                # Pool is full, close the connection
                conn.close()
                with self._lock:
                    self._created_connections -= 1
        else:
            # Close invalid or transaction-active connections
            if conn:
                conn.close()
                with self._lock:
                    self._created_connections -= 1
    
    def close_all(self):
        """Close all connections in the pool"""
        while True:
            try:
                conn = self._pool.get_nowait()
                conn.close()
            except Empty:
                break
        with self._lock:
            self._created_connections = 0


class ConnectionPoolManager:
    """
    Core connection pool management for workspaces - SQLite version
    """
    
    def __init__(self, min_connections: int = 1, max_connections: int = 10):
        self.min_connections = min_connections
        self.max_connections = max_connections
        
        # Dictionary to store connection pools for each workspace
        # Format: {workspace_id: {'pool': pool_object, 'last_used': timestamp, 'db_config': config}}
        self.workspace_pools: Dict[str, Dict[str, Any]] = {}
        
        # Lock for thread-safe operations
        self._lock = threading.RLock()
    
    def create_pool(self, workspace_id: str, db_config: Dict[str, Any]) -> bool:
        """
        Create a connection pool for a workspace
        
        Args:
            workspace_id: Unique identifier for the workspace
            db_config: Database configuration dict with keys: db_path (required), others optional
            
        Returns:
            bool: True if pool created successfully, False otherwise
        """
        with self._lock:
            try:
                # Close existing pool if it exists
                if workspace_id in self.workspace_pools:
                    self.close_pool(workspace_id)
                
                # Get database path from config
                db_path = db_config.get('db_path')
                if not db_path:
                    # Fallback: construct path from db_name
                    db_name = db_config.get('db_name', 'database')
                    db_path = f"./data/{db_name}.db"
                
                # Create connection pool
                connection_pool = SQLiteConnectionPool(db_path, self.max_connections)
                
                # Test the pool by getting a connection
                test_conn = connection_pool.get_connection()
                if test_conn:
                    cursor = test_conn.cursor()
                    cursor.execute("SELECT 1")
                    cursor.fetchone()
                    cursor.close()
                    connection_pool.put_connection(test_conn)
                
                # Store pool info
                self.workspace_pools[workspace_id] = {
                    'pool': connection_pool,
                    'last_used': time.time(),
                    'db_config': db_config.copy()
                }
                
                logger.info(f"Created SQLite connection pool for workspace {workspace_id} at {db_path}")
                return True
                
            except Exception as e:
                logger.error(f"Failed to create connection pool for workspace {workspace_id}: {e}")
                return False
    
    @contextmanager
    def get_connection(self, workspace_id: str):
        """
        Get a database connection from the workspace pool using context manager
        
        Args:
            workspace_id: Unique identifier for the workspace
            
        Yields:
            sqlite3.Connection: Database connection
            
        Raises:
            Exception: If workspace pool doesn't exist or connection fails
        """
        if workspace_id not in self.workspace_pools:
            raise Exception(f"No connection pool found for workspace {workspace_id}")
        
        connection = None
        try:
            with self._lock:
                pool_info = self.workspace_pools[workspace_id]
                pool_obj = pool_info['pool']
                pool_info['last_used'] = time.time()  # Update last used time
            
            # Get connection from pool
            connection = pool_obj.get_connection()
            
            if connection is None:
                raise Exception(f"Failed to get connection from pool for workspace {workspace_id}")
            
            yield connection
            
        except Exception as e:
            logger.error(f"Error getting connection for workspace {workspace_id}: {e}")
            if connection:
                try:
                    # Put connection back to pool even if it's problematic
                    pool_obj.put_connection(connection)
                except Exception as put_error:
                    logger.error(f"Error putting connection back to pool: {put_error}")
            raise
        finally:
            if connection:
                try:
                    pool_obj.put_connection(connection)
                except Exception as put_error:
                    logger.error(f"Error putting connection back to pool: {put_error}")
    
    def close_pool(self, workspace_id: str) -> bool:
        """
        Close and remove a workspace connection pool
        
        Args:
            workspace_id: Unique identifier for the workspace
            
        Returns:
            bool: True if pool closed successfully, False otherwise
        """
        with self._lock:
            if workspace_id not in self.workspace_pools:
                logger.warning(f"No connection pool found for workspace {workspace_id}")
                return False
            
            try:
                pool_info = self.workspace_pools[workspace_id]
                pool_obj = pool_info['pool']
                pool_obj.close_all()
                del self.workspace_pools[workspace_id]
                logger.info(f"Closed connection pool for workspace {workspace_id}")
                return True
            except Exception as e:
                logger.error(f"Error closing pool for workspace {workspace_id}: {e}")
                return False
    
    def refresh_pool(self, workspace_id: str) -> bool:
        """
        Refresh a workspace connection pool by recreating it
        
        Args:
            workspace_id: Unique identifier for the workspace
            
        Returns:
            bool: True if pool refreshed successfully, False otherwise
        """
        with self._lock:
            if workspace_id not in self.workspace_pools:
                logger.warning(f"No connection pool found for workspace {workspace_id}")
                return False
            
            try:
                # Get the current config
                pool_info = self.workspace_pools[workspace_id]
                db_config = pool_info['db_config']
                
                # Recreate the pool
                return self.create_pool(workspace_id, db_config)
                
            except Exception as e:
                logger.error(f"Error refreshing pool for workspace {workspace_id}: {e}")
                return False
    
    def get_pool_info(self, workspace_id: str) -> Optional[Dict[str, Any]]:
        """
        Get information about a workspace pool
        
        Args:
            workspace_id: Unique identifier for the workspace
            
        Returns:
            Optional[Dict[str, Any]]: Pool information or None if not found
        """
        with self._lock:
            if workspace_id not in self.workspace_pools:
                return None
            
            pool_info = self.workspace_pools[workspace_id]
            return {
                'last_used': pool_info['last_used'],
                'db_config': pool_info['db_config'].copy()
            }
    
    def get_all_pools_info(self) -> Dict[str, Dict[str, Any]]:
        """
        Get information about all workspace pools
        
        Returns:
            Dict[str, Dict[str, Any]]: Dictionary mapping workspace IDs to pool information
        """
        with self._lock:
            all_info = {}
            for workspace_id, pool_info in self.workspace_pools.items():
                all_info[workspace_id] = {
                    'last_used': pool_info['last_used'],
                    'db_config': pool_info['db_config'].copy()
                }
            return all_info
    
    def close_all_pools(self):
        """Close all workspace connection pools"""
        with self._lock:
            workspace_ids = list(self.workspace_pools.keys())
            for workspace_id in workspace_ids:
                self.close_pool(workspace_id)
    
    def has_pool(self, workspace_id: str) -> bool:
        """
        Check if a workspace has a connection pool
        
        Args:
            workspace_id: Unique identifier for the workspace
            
        Returns:
            bool: True if pool exists, False otherwise
        """
        with self._lock:
            return workspace_id in self.workspace_pools 