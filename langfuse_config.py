"""
Langfuse Configuration and Setup
Provides observability and monitoring for AI operations
"""

import os
from typing import Optional, Dict, Any
from dotenv import load_dotenv
from langfuse import Langfuse, observe
import logging

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class LangfuseManager:
    """Manages Langfuse initialization and configuration"""
    
    def __init__(self):
        self.langfuse: Optional[Langfuse] = None
        self.is_enabled = False
        
        # Initialize Langfuse if credentials are available
        self._initialize()
    
    def _initialize(self):
        """Initialize Langfuse with environment variables"""
        try:
            # Get Langfuse credentials from environment
            secret_key = os.getenv("LANGFUSE_SECRET_KEY")
            public_key = os.getenv("LANGFUSE_PUBLIC_KEY")
            host = os.getenv("LANGFUSE_HOST", "https://cloud.langfuse.com")
            
            if not secret_key or not public_key:
                logger.warning(
                    "Langfuse credentials not found. Set LANGFUSE_SECRET_KEY and LANGFUSE_PUBLIC_KEY "
                    "environment variables to enable observability. Running without Langfuse."
                )
                return
            
            # Initialize Langfuse client
            self.langfuse = Langfuse(
                secret_key=secret_key,
                public_key=public_key,
                host=host
            )
            
            self.is_enabled = True
            logger.info("Langfuse initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize Langfuse: {e}")
            self.is_enabled = False
    
    def get_langfuse_client(self) -> Optional[Langfuse]:
        """Get the Langfuse client"""
        return self.langfuse if self.is_enabled else None
    
    def create_trace(self, name: str, **kwargs) -> Optional[Any]:
        """Create a new trace"""
        if not self.is_enabled:
            return None
        
        try:
            return self.langfuse.trace(name=name, **kwargs)
        except Exception as e:
            logger.error(f"Failed to create trace: {e}")
            return None
    
    def update_trace(self, trace_id: str, **kwargs):
        """Update an existing trace"""
        if not self.is_enabled:
            return
        
        try:
            self.langfuse.trace(id=trace_id, **kwargs)
        except Exception as e:
            logger.error(f"Failed to update trace: {e}")
    
    def create_generation(self, trace_id: str, name: str, **kwargs) -> Optional[Any]:
        """Create a generation within a trace"""
        if not self.is_enabled:
            return None
        
        try:
            return self.langfuse.generation(
                trace_id=trace_id,
                name=name,
                **kwargs
            )
        except Exception as e:
            logger.error(f"Failed to create generation: {e}")
            return None
    
    def score_trace(self, trace_id: str, name: str, value: float, **kwargs):
        """Add a score to a trace"""
        if not self.is_enabled:
            return
        
        try:
            self.langfuse.score(
                trace_id=trace_id,
                name=name,
                value=value,
                **kwargs
            )
        except Exception as e:
            logger.error(f"Failed to score trace: {e}")
    
    def flush(self):
        """Flush pending events to Langfuse"""
        if self.is_enabled and self.langfuse:
            try:
                self.langfuse.flush()
            except Exception as e:
                logger.error(f"Failed to flush Langfuse events: {e}")


# Global Langfuse manager instance
langfuse_manager = LangfuseManager()


def get_langfuse_callback():
    """Helper function to get Langfuse callback handler - Not available in Langfuse 3.x"""
    # Note: In Langfuse 3.x, LangChain integration works differently
    # Use the @observe decorator instead
    return None


def create_langfuse_trace(name: str, user_id: str = None, session_id: str = None, **metadata):
    """Helper function to create a Langfuse trace"""
    trace_data = {"name": name}
    
    if user_id:
        trace_data["user_id"] = user_id
    if session_id:
        trace_data["session_id"] = session_id
    if metadata:
        trace_data["metadata"] = metadata
    
    return langfuse_manager.create_trace(**trace_data)


def observe_function(name: str = None):
    """Decorator to observe function calls with Langfuse"""
    def decorator(func):
        if langfuse_manager.is_enabled:
            return observe(name=name or func.__name__)(func)
        else:
            # Return the original function if Langfuse is not enabled
            return func
    return decorator


# Cleanup function for graceful shutdown
def cleanup_langfuse():
    """Cleanup Langfuse resources"""
    langfuse_manager.flush() 