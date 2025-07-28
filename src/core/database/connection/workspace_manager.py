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
        logger.info(f"Creating workspace: {workspace_id}")
        if 'db_path' in db_config:
            logger.debug(f"SQLite database config: {db_config['db_path']}")
        else:
            logger.debug(f"Legacy database config: {db_config.get('db_name', 'unknown')}@{db_config.get('host', 'unknown')}:{db_config.get('port', 'unknown')}")
        
        try:
            # Create connection pool
            logger.info(f"Creating connection pool for workspace: {workspace_id}")
            if not self.pool_manager.create_pool(workspace_id, db_config):
                logger.error(f"Failed to create connection pool for workspace: {workspace_id}")
                return False
            
            logger.debug(f"Connection pool created successfully for workspace: {workspace_id}")
            
            # Create database analyzer
            logger.info(f"Initializing database analyzer for workspace: {workspace_id}")
            from src.core.database.analysis import DatabaseAnalyzer
            
            # Handle both SQLite and legacy PostgreSQL configurations
            if 'db_path' in db_config:
                # SQLite configuration
                db_analyzer = DatabaseAnalyzer(
                    db_path=db_config['db_path'],
                    db_name=db_config.get('db_name', 'PBTest'),
                    username=db_config.get('username'),
                    password=db_config.get('password'),
                    host=db_config.get('host'),
                    port=db_config.get('port')
                )
            else:
                # Legacy PostgreSQL configuration (backward compatibility)
                db_path = f"./data/{db_config.get('db_name', 'PBTest')}.db"
                logger.warning(f"Legacy PostgreSQL config detected, converting to SQLite: {db_path}")
                db_analyzer = DatabaseAnalyzer(
                    db_path=db_path,
                    db_name=db_config['db_name'],
                    username=db_config.get('username'),
                    password=db_config.get('password'),
                    host=db_config.get('host'),
                    port=db_config.get('port')
                )
            
            # Initialize workspace metadata
            self.workspace_metadata[workspace_id] = {
                'db_analyzer': db_analyzer,
                'schema_analyzed': False,
                'schema_info': None,
                'created_at': time.time(),
                'db_config': db_config
            }
            
            logger.debug(f"Database analyzer initialized for workspace: {workspace_id}")
            
            # Analyze schema if requested
            if analyze_schema:
                logger.info(f"Starting schema analysis for workspace: {workspace_id}")
                if self.ensure_schema_analyzed(workspace_id):
                    logger.info(f"Schema analysis completed for workspace: {workspace_id}")
                else:
                    logger.warning(f"Schema analysis failed for workspace: {workspace_id}")
            else:
                logger.info(f"Schema analysis skipped for workspace: {workspace_id}")
            
            logger.info(f"Workspace created successfully: {workspace_id}")
            return True
            
        except Exception as e:
            logger.error(f"Error creating workspace {workspace_id}: {e}")
            # Clean up on failure
            if workspace_id in self.workspace_metadata:
                del self.workspace_metadata[workspace_id]
            self.pool_manager.close_pool(workspace_id)
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
        Ensure the database schema is analyzed for a workspace
        
        Args:
            workspace_id: Workspace ID
            
        Returns:
            bool: True if schema is analyzed, False otherwise
        """
        logger.info(f"Ensuring schema analysis for workspace: {workspace_id}")
        
        if workspace_id not in self.workspace_metadata:
            logger.error(f"Workspace {workspace_id} not found for schema analysis")
            return False
        
        metadata = self.workspace_metadata[workspace_id]
        
        # Check if already analyzed
        if metadata['schema_analyzed']:
            logger.debug(f"Schema already analyzed for workspace: {workspace_id}")
            return True
        
        try:
            logger.info(f"Starting schema analysis for workspace: {workspace_id}")
            db_analyzer = metadata['db_analyzer']
            
            # Perform schema analysis
            schema_info = db_analyzer.analyze_schema()
            
            # Update metadata
            metadata['schema_analyzed'] = True
            metadata['schema_info'] = schema_info
            metadata['schema_analyzed_at'] = time.time()
            
            logger.info(f"Schema analysis completed successfully for workspace: {workspace_id}")
            logger.debug(f"Schema contains {len(schema_info.get('tables', {}))} tables")
            
            return True
            
        except Exception as e:
            logger.error(f"Error analyzing schema for workspace {workspace_id}: {e}")
            return False
    
    def get_workspace_status(self, workspace_id: str) -> Optional[Dict[str, Any]]:
        """
        Get the status of a workspace
        
        Args:
            workspace_id: Workspace ID
            
        Returns:
            Dictionary with workspace status or None if not found
        """
        logger.debug(f"Getting status for workspace: {workspace_id}")
        
        if workspace_id not in self.workspace_metadata:
            logger.warning(f"Workspace {workspace_id} not found")
            return None
        
        metadata = self.workspace_metadata[workspace_id]
        
        # Check connection pool status (with fallback for compatibility)
        try:
            pool_status = self.pool_manager.get_pool_status(workspace_id)
        except AttributeError:
            # Fallback if get_pool_status doesn't exist
            try:
                pool_exists = workspace_id in self.pool_manager.pools
            except AttributeError:
                # Even more basic fallback if pools attribute doesn't exist
                pool_exists = True  # Assume pool exists if workspace was created successfully
            
            pool_status = {
                'pool_exists': pool_exists,
                'status': 'active' if pool_exists else 'inactive'
            }
        
        status = {
            'workspace_id': workspace_id,
            'schema_analyzed': metadata['schema_analyzed'],
            'created_at': metadata.get('created_at', 0),
            'schema_analyzed_at': metadata.get('schema_analyzed_at', 0),
            'pool_status': pool_status,
            'db_config': {
                'db_path': metadata['db_config'].get('db_path'),
                'db_name': metadata['db_config'].get('db_name'),
                'host': metadata['db_config'].get('host'),
                'port': metadata['db_config'].get('port'),
                'db_type': 'sqlite' if 'db_path' in metadata['db_config'] else 'legacy'
            }
        }
        
        # Add schema summary if available
        if metadata['schema_analyzed'] and metadata['schema_info']:
            schema_summary = metadata['schema_info'].get('summary', {})
            status['schema_summary'] = schema_summary
        
        logger.debug(f"Workspace status retrieved for: {workspace_id}")
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
        Close a workspace and cleanup resources
        
        Args:
            workspace_id: Workspace ID
            
        Returns:
            bool: True if closed successfully, False otherwise
        """
        logger.info(f"Closing workspace: {workspace_id}")
        
        try:
            # Close connection pool
            logger.debug(f"Closing connection pool for workspace: {workspace_id}")
            pool_closed = self.pool_manager.close_pool(workspace_id)
            
            # Remove workspace metadata
            if workspace_id in self.workspace_metadata:
                logger.debug(f"Removing metadata for workspace: {workspace_id}")
                del self.workspace_metadata[workspace_id]
            
            if pool_closed:
                logger.info(f"Workspace closed successfully: {workspace_id}")
            else:
                logger.warning(f"Issues closing connection pool for workspace: {workspace_id}")
            
            return pool_closed
            
        except Exception as e:
            logger.error(f"Error closing workspace {workspace_id}: {e}")
            return False
    
    def refresh_workspace(self, workspace_id: str) -> bool:
        """
        Refresh workspace by re-analyzing the schema
        
        Args:
            workspace_id: Workspace ID
            
        Returns:
            bool: True if refreshed successfully, False otherwise
        """
        logger.info(f"Refreshing workspace: {workspace_id}")
            
        if workspace_id not in self.workspace_metadata:
            logger.error(f"Workspace {workspace_id} not found for refresh")
            return False
        
        try:
            # Reset schema analysis flag
            logger.debug(f"Resetting schema analysis flag for workspace: {workspace_id}")
            self.workspace_metadata[workspace_id]['schema_analyzed'] = False
            self.workspace_metadata[workspace_id]['schema_info'] = None
            
            # Re-analyze schema
            logger.info(f"Re-analyzing schema for workspace: {workspace_id}")
            success = self.ensure_schema_analyzed(workspace_id)
            
            if success:
                logger.info(f"Workspace refreshed successfully: {workspace_id}")
            else:
                logger.error(f"Failed to refresh workspace: {workspace_id}")
            
            return success
            
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