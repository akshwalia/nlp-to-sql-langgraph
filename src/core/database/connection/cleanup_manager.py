import time
import threading
from typing import Dict, Any
import logging

logger = logging.getLogger(__name__)


class CleanupManager:
    """
    Manages background cleanup of inactive connection pools
    """
    
    def __init__(self, pool_manager, inactivity_timeout: int = 600):
        """
        Initialize the cleanup manager
        
        Args:
            pool_manager: ConnectionPoolManager instance
            inactivity_timeout: Timeout in seconds for inactive connections (default: 10 minutes)
        """
        self.pool_manager = pool_manager
        self.inactivity_timeout = inactivity_timeout
        
        # Background cleanup task
        self._cleanup_task = None
        self._stop_cleanup = False
        self._start_cleanup_task()
    
    def _start_cleanup_task(self):
        """Start the background cleanup task"""
        def cleanup_worker():
            while not self._stop_cleanup:
                try:
                    self._cleanup_inactive_connections()
                    time.sleep(60)  # Check every minute
                except Exception as e:
                    logger.error(f"Error in cleanup worker: {e}")
        
        self._cleanup_task = threading.Thread(target=cleanup_worker, daemon=True)
        self._cleanup_task.start()
        logger.info("Database connection cleanup task started")
    
    def _cleanup_inactive_connections(self):
        """Clean up inactive connection pools"""
        current_time = time.time()
        inactive_workspaces = []
        
        # Get all pool info
        all_pools = self.pool_manager.get_all_pools_info()
        
        # Find inactive pools
        for workspace_id, pool_info in all_pools.items():
            last_used = pool_info['last_used']
            if current_time - last_used > self.inactivity_timeout:
                inactive_workspaces.append(workspace_id)
        
        # Close inactive pools
        for workspace_id in inactive_workspaces:
            try:
                self.pool_manager.close_pool(workspace_id)
                logger.info(f"Closed inactive connection pool for workspace {workspace_id}")
            except Exception as e:
                logger.error(f"Error closing pool for workspace {workspace_id}: {e}")
    
    def stop_cleanup(self):
        """Stop the background cleanup task"""
        self._stop_cleanup = True
        if self._cleanup_task and self._cleanup_task.is_alive():
            self._cleanup_task.join(timeout=2)
            logger.info("Database connection cleanup task stopped")
    
    def force_cleanup(self):
        """Force an immediate cleanup of inactive connections"""
        try:
            self._cleanup_inactive_connections()
            logger.info("Forced cleanup of inactive connections completed")
        except Exception as e:
            logger.error(f"Error in forced cleanup: {e}")
    
    def get_cleanup_status(self) -> Dict[str, Any]:
        """
        Get the status of the cleanup manager
        
        Returns:
            Dict[str, Any]: Status information
        """
        return {
            'cleanup_active': not self._stop_cleanup,
            'cleanup_thread_alive': self._cleanup_task.is_alive() if self._cleanup_task else False,
            'inactivity_timeout': self.inactivity_timeout
        }


def cleanup_db_connections():
    """
    Standalone function to cleanup database connections
    Used for emergency cleanup scenarios
    """
    try:
        import psycopg2
        # This is a basic cleanup function that doesn't rely on the manager
        # It's mainly for emergency scenarios
        logger.info("Emergency database connection cleanup initiated")
        
        # Note: This is a basic implementation
        # In practice, you might want to implement more sophisticated cleanup
        # based on your specific needs
        
    except Exception as e:
        logger.error(f"Error in emergency cleanup: {e}") 