"""
Simplified database package for hardcoded PBTest database access
"""
import logging
from .analysis.single_table_analyzer import SingleTableAnalyzer

logger = logging.getLogger(__name__)

# Hardcoded database configuration
HARDCODED_DB_CONFIG = {
    "db_name": "PBTest",
    "username": "postgres",  # Will be overridden by env vars if available
    "password": "Arjit#195",  # Will be overridden by env vars if available
    "host": "localhost",     # Will be overridden by env vars if available
    "port": "5432",          # Will be overridden by env vars if available
    "table_name": "IT_Professional_Services",
    "schema_name": "public"
}


class SimplifiedDatabaseAnalyzer:
    """
    Simplified database analyzer for PBTest database and IT_Professional_Services
    - Hardcoded configuration for single database/table setup
    - No workspace management or connection pools
    """
    
    def __init__(self, enum_threshold: int = 50):
        """
        Initialize with hardcoded PBTest configuration
        
        Args:
            enum_threshold: Maximum number of unique values to treat as enum (default: 50)
        """
        import os
        from dotenv import load_dotenv
        
        load_dotenv()
        
        # Use environment variables if available, otherwise use hardcoded values
        self.db_name = "PBTest"
        self.username = os.getenv("DB_USERNAME", HARDCODED_DB_CONFIG["username"])
        self.password = os.getenv("DB_PASSWORD", HARDCODED_DB_CONFIG["password"])
        self.host = os.getenv("DB_HOST", HARDCODED_DB_CONFIG["host"])
        self.port = os.getenv("DB_PORT", HARDCODED_DB_CONFIG["port"])
        self.table_name = "IT_Professional_Services"
        self.schema_name = "public"
        self.enum_threshold = enum_threshold
        
        logger.info(f"Initialized SimplifiedDatabaseAnalyzer for {self.db_name}.{self.schema_name}.{self.table_name}")
        logger.info(f"Enum detection threshold: {self.enum_threshold} unique values")
        
        # Initialize the single table analyzer
        self.analyzer = SingleTableAnalyzer(
            db_name=self.db_name,
            username=self.username,
            password=self.password,
            host=self.host,
            port=self.port,
            table_name=self.table_name,
            schema_name=self.schema_name,
            output_file=f"{self.table_name}_analysis.txt",
            enum_threshold=self.enum_threshold
        )
        
        # Perform initial analysis
        self.schema_info = None
        self.llm_context = None
        self._analyze_table()
    
    def _analyze_table(self):
        """Analyze the hardcoded table"""
        try:
            result = self.analyzer.analyze_table(save_to_file=True)
            if result.get("success"):
                self.schema_info = result
                self.llm_context = self.analyzer.get_llm_context()
                return True
            else:
                print(f"Failed to analyze table: {result.get('error')}")
                return False
        except Exception as e:
            print(f"Error during table analysis: {e}")
            return False
    
    def get_schema_context(self) -> str:
        """Get the LLM-ready schema context"""
        if not self.llm_context:
            self._analyze_table()
        return self.llm_context or "No schema context available"
    
    def get_rich_schema_context(self) -> str:
        """Alias for get_schema_context for backward compatibility"""
        return self.get_schema_context()
    
    def analyze_schema(self) -> dict:
        """Return the schema analysis results"""
        if not self.schema_info:
            self._analyze_table()
        return self.schema_info or {}
    
    def get_table_info(self) -> dict:
        """Get table information"""
        return self.schema_info or {}
    
    def refresh_schema_context(self) -> bool:
        """Refresh the schema analysis"""
        return self._analyze_table()
    
    def test_connection(self) -> bool:
        """Test database connection"""
        try:
            # Try to analyze the table to test connection
            result = self.analyzer.analyze_table(save_to_file=False)
            return result.get("success", False)
        except Exception:
            return False
    
    def execute_query(self, query: str):
        """Execute a query - simplified version"""
        try:
            with self.analyzer.engine.connect() as connection:
                from sqlalchemy import text
                result = connection.execute(text(query))
                
                # Check if query returns rows
                if result.returns_rows:
                    rows = []
                    for row in result:
                        row_dict = {}
                        for idx, column in enumerate(result.keys()):
                            row_dict[column] = row[idx]
                        rows.append(row_dict)
                    return True, rows, None
                else:
                    # For non-SELECT queries, return rowcount
                    return True, [{"affected_rows": result.rowcount}], None
                    
        except Exception as e:
            return False, None, str(e)


# Create a singleton instance for global use
_analyzer_instance = None

def get_database_analyzer() -> 'SimplifiedDatabaseAnalyzer':
    """Get the global database analyzer instance"""
    global _analyzer_instance
    if _analyzer_instance is None:
        _analyzer_instance = SimplifiedDatabaseAnalyzer(
            enum_threshold=50  # Detect columns with 50 or fewer unique values as enums
        )
    return _analyzer_instance


# For backward compatibility - export the main classes
DatabaseAnalyzer = SimplifiedDatabaseAnalyzer

__all__ = ['SingleTableAnalyzer', 'SimplifiedDatabaseAnalyzer', 'DatabaseAnalyzer', 'get_database_analyzer'] 