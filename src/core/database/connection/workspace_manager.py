import time
from typing import Dict, Optional, Any
import logging

logger = logging.getLogger(__name__)


class WorkspaceManager:
    """
    Manages workspace-specific database operations and metadata
    """
    
    def __init__(self, pool_manager):
        """
        Initialize the workspace manager
        
        Args:
            pool_manager: ConnectionPoolManager instance
        """
        self.pool_manager = pool_manager
        
        # Dictionary to store workspace metadata
        # Format: {workspace_id: {'db_analyzer': analyzer, 'schema_analyzed': bool, 'schema_info': dict}}
        self.workspace_metadata: Dict[str, Dict[str, Any]] = {}
    
    def create_workspace(self, workspace_id: str, db_config: Dict[str, Any], analyze_schema: bool = True) -> bool:
        """
        Create a workspace with database connection and optional schema analysis
        
        Args:
            workspace_id: Unique identifier for the workspace
            db_config: Database configuration dict
            analyze_schema: Whether to analyze the database schema immediately
            
        Returns:
            bool: True if workspace created successfully, False otherwise
        """
        try:
            # Create connection pool
            if not self.pool_manager.create_pool(workspace_id, db_config):
                return False
            
            # Create database analyzer
            from src.core.database.analysis import DatabaseAnalyzer
            db_analyzer = DatabaseAnalyzer(
                db_config['db_name'],
                db_config['username'],
                db_config['password'],
                db_config['host'],
                db_config['port'],
                connection_manager=self.pool_manager,
                workspace_id=workspace_id
            )
            
            # Store workspace metadata
            self.workspace_metadata[workspace_id] = {
                'db_analyzer': db_analyzer,
                'schema_analyzed': False,
                'schema_info': None
            }
            
            # Analyze schema if requested
            if analyze_schema:
                try:
                    logger.info(f"Analyzing database schema for workspace {workspace_id}")
                    schema_info = db_analyzer.analyze_schema()
                    self.workspace_metadata[workspace_id]['schema_analyzed'] = True
                    self.workspace_metadata[workspace_id]['schema_info'] = schema_info
                    logger.info(f"Schema analysis completed for workspace {workspace_id}")
                except Exception as e:
                    logger.error(f"Error analyzing schema for workspace {workspace_id}: {e}")
                    # Don't fail the workspace creation if schema analysis fails
            
            logger.info(f"Created workspace {workspace_id}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to create workspace {workspace_id}: {e}")
            return False
    
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
        if workspace_id not in self.workspace_metadata:
            raise Exception(f"Workspace {workspace_id} not found")
        
        return self.workspace_metadata[workspace_id]['db_analyzer']
    
    def is_schema_analyzed(self, workspace_id: str) -> bool:
        """
        Check if schema has been analyzed for a workspace
        
        Args:
            workspace_id: Unique identifier for the workspace
            
        Returns:
            bool: True if schema analyzed, False otherwise
        """
        if workspace_id not in self.workspace_metadata:
            return False
        
        return self.workspace_metadata[workspace_id]['schema_analyzed']
    
    def ensure_schema_analyzed(self, workspace_id: str) -> bool:
        """
        Ensure that schema is analyzed for a workspace
        
        Args:
            workspace_id: Unique identifier for the workspace
            
        Returns:
            bool: True if schema is analyzed or analysis successful, False otherwise
        """
        if workspace_id not in self.workspace_metadata:
            logger.error(f"Workspace {workspace_id} not found")
            return False
        
        if self.workspace_metadata[workspace_id]['schema_analyzed']:
            return True
        
        try:
            logger.info(f"Analyzing schema for workspace {workspace_id}")
            db_analyzer = self.workspace_metadata[workspace_id]['db_analyzer']
            schema_info = db_analyzer.analyze_schema()
            
            self.workspace_metadata[workspace_id]['schema_analyzed'] = True
            self.workspace_metadata[workspace_id]['schema_info'] = schema_info
            
            logger.info(f"Schema analysis completed for workspace {workspace_id}")
            return True
            
        except Exception as e:
            logger.error(f"Error analyzing schema for workspace {workspace_id}: {e}")
            return False
    
    def get_workspace_status(self, workspace_id: str) -> Optional[Dict[str, Any]]:
        """
        Get comprehensive status information for a workspace
        
        Args:
            workspace_id: Unique identifier for the workspace
            
        Returns:
            Optional[Dict[str, Any]]: Status information or None if workspace doesn't exist
        """
        if workspace_id not in self.workspace_metadata:
            return None
        
        # Get pool info
        pool_info = self.pool_manager.get_pool_info(workspace_id)
        if not pool_info:
            return None
        
        metadata = self.workspace_metadata[workspace_id]
        
        status = {
            'workspace_id': workspace_id,
            'connection_active': True,
            'last_used': pool_info['last_used'],
            'db_config': {
                'host': pool_info['db_config']['host'],
                'port': pool_info['db_config']['port'],
                'db_name': pool_info['db_config']['db_name'],
                'username': pool_info['db_config']['username']
                # Don't include password in status for security
            },
            'schema_analyzed': metadata['schema_analyzed'],
            'schema_info': metadata['schema_info']
        }
        
        return status
    
    def get_all_workspace_status(self) -> Dict[str, Dict[str, Any]]:
        """
        Get status information for all workspaces
        
        Returns:
            Dict[str, Dict[str, Any]]: Dictionary mapping workspace IDs to status information
        """
        all_status = {}
        for workspace_id in self.workspace_metadata:
            status = self.get_workspace_status(workspace_id)
            if status:
                all_status[workspace_id] = status
        return all_status
    
    def close_workspace(self, workspace_id: str) -> bool:
        """
        Close a workspace and clean up resources
        
        Args:
            workspace_id: Unique identifier for the workspace
            
        Returns:
            bool: True if workspace closed successfully, False otherwise
        """
        try:
            # Close connection pool
            pool_closed = self.pool_manager.close_pool(workspace_id)
            
            # Remove metadata
            if workspace_id in self.workspace_metadata:
                del self.workspace_metadata[workspace_id]
            
            logger.info(f"Closed workspace {workspace_id}")
            return pool_closed
            
        except Exception as e:
            logger.error(f"Error closing workspace {workspace_id}: {e}")
            return False
    
    def refresh_workspace(self, workspace_id: str) -> bool:
        """
        Refresh a workspace by recreating its connection pool
        
        Args:
            workspace_id: Unique identifier for the workspace
            
        Returns:
            bool: True if workspace refreshed successfully, False otherwise
        """
        try:
            # Refresh connection pool
            pool_refreshed = self.pool_manager.refresh_pool(workspace_id)
            
            if pool_refreshed and workspace_id in self.workspace_metadata:
                # Reset schema analysis flag to force re-analysis if needed
                self.workspace_metadata[workspace_id]['schema_analyzed'] = False
                self.workspace_metadata[workspace_id]['schema_info'] = None
            
            logger.info(f"Refreshed workspace {workspace_id}")
            return pool_refreshed
            
        except Exception as e:
            logger.error(f"Error refreshing workspace {workspace_id}: {e}")
            return False
    
    def has_workspace(self, workspace_id: str) -> bool:
        """
        Check if a workspace exists
        
        Args:
            workspace_id: Unique identifier for the workspace
            
        Returns:
            bool: True if workspace exists, False otherwise
        """
        return workspace_id in self.workspace_metadata 