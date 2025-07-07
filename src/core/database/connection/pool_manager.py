import time
import threading
from typing import Dict, Optional, Any
import psycopg2
from psycopg2 import pool
from contextlib import contextmanager
import logging

logger = logging.getLogger(__name__)


class ConnectionPoolManager:
    """
    Core connection pool management for workspaces
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
            db_config: Database configuration dict with keys: host, port, db_name, username, password
            
        Returns:
            bool: True if pool created successfully, False otherwise
        """
        with self._lock:
            try:
                # Close existing pool if it exists
                if workspace_id in self.workspace_pools:
                    self.close_pool(workspace_id)
                
                # Create new connection pool
                connection_params = {
                    'minconn': self.min_connections,
                    'maxconn': self.max_connections,
                    'host': db_config['host'],
                    'port': db_config['port'],
                    'database': db_config['db_name'],
                    'user': db_config['username'],
                    'password': db_config['password'],
                    'connect_timeout': 10
                }
                
                # Add SSL configuration for production
                if db_config.get('sslmode'):
                    connection_params['sslmode'] = db_config['sslmode']
                elif db_config['host'] not in ['localhost', '127.0.0.1']:
                    # Default to requiring SSL for remote connections
                    connection_params['sslmode'] = 'prefer'
                
                connection_pool = psycopg2.pool.ThreadedConnectionPool(**connection_params)
                
                # Test the pool by getting a connection
                test_conn = connection_pool.getconn()
                if test_conn:
                    cursor = test_conn.cursor()
                    cursor.execute("SELECT 1")
                    cursor.fetchone()
                    cursor.close()
                    connection_pool.putconn(test_conn)
                
                # Store pool info
                self.workspace_pools[workspace_id] = {
                    'pool': connection_pool,
                    'last_used': time.time(),
                    'db_config': db_config.copy()
                }
                
                logger.info(f"Created connection pool for workspace {workspace_id}")
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
            psycopg2.connection: Database connection
            
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
            connection = pool_obj.getconn()
            
            if connection is None:
                raise Exception(f"Failed to get connection from pool for workspace {workspace_id}")
            
            # Test connection
            if connection.closed:
                raise Exception("Connection is closed")
            
            yield connection
            
        except Exception as e:
            logger.error(f"Error getting connection for workspace {workspace_id}: {e}")
            if connection:
                try:
                    # Put connection back to pool even if it's problematic
                    pool_obj.putconn(connection)
                except Exception as put_error:
                    logger.error(f"Error putting connection back to pool: {put_error}")
            raise
        finally:
            if connection:
                try:
                    pool_obj.putconn(connection)
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
                pool_obj.closeall()
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