import time
from typing import Dict, Optional, Any
from contextlib import contextmanager
import logging

from .pool_manager import ConnectionPoolManager
from .cleanup_manager import CleanupManager
from .workspace_manager import WorkspaceManager

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DatabaseConnectionManager:
    """
    Main interface for database connection management
    Combines pool management, cleanup, and workspace operations
    """
    
    def __init__(self, 
                 min_connections: int = 1, 
                 max_connections: int = 10,
                 inactivity_timeout: int = 600):  # 10 minutes in seconds
        """
        Initialize the database connection manager
        
        Args:
            min_connections: Minimum connections per pool
            max_connections: Maximum connections per pool
            inactivity_timeout: Timeout for inactive connections in seconds
        """
        self.min_connections = min_connections
        self.max_connections = max_connections
        self.inactivity_timeout = inactivity_timeout
        
        # Initialize component managers
        self.pool_manager = ConnectionPoolManager(min_connections, max_connections)
        self.cleanup_manager = CleanupManager(self.pool_manager, inactivity_timeout)
        self.workspace_manager = WorkspaceManager(self.pool_manager)
    
    def create_workspace_pool(self, workspace_id: str, db_config: Dict[str, Any], analyze_schema: bool = True) -> bool:
        """
        Create a workspace with database connection and optional schema analysis
        
        Args:
            workspace_id: Unique identifier for the workspace
            db_config: Database configuration dict with keys: host, port, db_name, username, password
            analyze_schema: Whether to analyze the database schema immediately
            
        Returns:
            bool: True if workspace created successfully, False otherwise
        """
        return self.workspace_manager.create_workspace(workspace_id, db_config, analyze_schema)
    
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
        with self.pool_manager.get_connection(workspace_id) as connection:
            yield connection
    
    def close_workspace_pool(self, workspace_id: str) -> bool:
        """
        Close a workspace and clean up resources
        
        Args:
            workspace_id: Unique identifier for the workspace
            
        Returns:
            bool: True if workspace closed successfully, False otherwise
        """
        return self.workspace_manager.close_workspace(workspace_id)
    
    def get_database_analyzer(self, workspace_id: str):
        """
        Get the database analyzer for a workspace
        
        Args:
            workspace_id: Unique identifier for the workspace
            
        Returns:
            DatabaseAnalyzer: Database analyzer instance
            
        Raises:
            Exception: If workspace doesn't exist
        """
        return self.workspace_manager.get_database_analyzer(workspace_id)
    
    def is_schema_analyzed(self, workspace_id: str) -> bool:
        """
        Check if schema has been analyzed for a workspace
        
        Args:
            workspace_id: Unique identifier for the workspace
            
        Returns:
            bool: True if schema analyzed, False otherwise
        """
        return self.workspace_manager.is_schema_analyzed(workspace_id)
    
    def ensure_schema_analyzed(self, workspace_id: str) -> bool:
        """
        Ensure that schema is analyzed for a workspace
        
        Args:
            workspace_id: Unique identifier for the workspace
            
        Returns:
            bool: True if schema is analyzed or analysis successful, False otherwise
        """
        return self.workspace_manager.ensure_schema_analyzed(workspace_id)
    
    def get_workspace_status(self, workspace_id: str) -> Optional[Dict[str, Any]]:
        """
        Get comprehensive status information for a workspace
        
        Args:
            workspace_id: Unique identifier for the workspace
            
        Returns:
            Optional[Dict[str, Any]]: Status information or None if workspace doesn't exist
        """
        return self.workspace_manager.get_workspace_status(workspace_id)
    
    def get_all_workspace_status(self) -> Dict[str, Dict[str, Any]]:
        """
        Get status information for all workspaces
        
        Returns:
            Dict[str, Dict[str, Any]]: Dictionary mapping workspace IDs to status information
        """
        return self.workspace_manager.get_all_workspace_status()
    
    def refresh_connection_pool(self, workspace_id: str) -> bool:
        """
        Refresh a workspace by recreating its connection pool
        
        Args:
            workspace_id: Unique identifier for the workspace
            
        Returns:
            bool: True if workspace refreshed successfully, False otherwise
        """
        return self.workspace_manager.refresh_workspace(workspace_id)
    
    def shutdown(self):
        """Shutdown the connection manager and clean up resources"""
        try:
            # Stop cleanup manager
            self.cleanup_manager.stop_cleanup()
            
            # Close all pools
            self.pool_manager.close_all_pools()
            
            logger.info("Database connection manager shut down successfully")
            
        except Exception as e:
            logger.error(f"Error during shutdown: {e}")
    
    def __del__(self):
        """Cleanup when the manager is destroyed"""
        try:
            self.shutdown()
        except Exception as e:
            logger.error(f"Error in destructor: {e}")


# Legacy function for backward compatibility
def cleanup_db_connections():
    """
    Standalone function to cleanup database connections
    Used for emergency cleanup scenarios
    """
    from .cleanup_manager import cleanup_db_connections
    cleanup_db_connections()


# Create a global instance for backward compatibility
db_connection_manager = DatabaseConnectionManager()

# Expose the cleanup function
__all__ = ['DatabaseConnectionManager', 'db_connection_manager', 'cleanup_db_connections'] 