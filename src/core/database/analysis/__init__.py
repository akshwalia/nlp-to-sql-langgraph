<<<<<<< HEAD
"""
Simplified database analysis package focused on single table analysis
"""
=======
import os
import pandas as pd
from sqlalchemy import create_engine, inspect, MetaData, text
from sqlalchemy.ext.automap import automap_base
from typing import Dict, List, Any, Tuple, Optional
import sqlite3
import warnings
import logging
>>>>>>> c5063088d3be74295c4fd89780135628a9cb5739

from .single_table_analyzer import SingleTableAnalyzer

<<<<<<< HEAD
# Export only the components used in the analytical approach
__all__ = ['SingleTableAnalyzer'] 
=======
# Suppress SQLAlchemy warnings for unrecognized column types
warnings.filterwarnings("ignore", "Did not recognize type", module="sqlalchemy")

logger = logging.getLogger(__name__)


class DatabaseAnalyzer:
    """
    Main database analyzer that combines all analysis functionality
    """
    
    def __init__(
        self,
        db_path: str,
        db_name: str = None,
        username: str = None,
        password: str = None,
        host: str = None,
        port: str = None,
        connection_manager=None,
        workspace_id: str = None
    ):
        """
        Initialize the database analyzer for SQLite
        
        Args:
            db_path: Path to SQLite database file
            db_name: Legacy parameter (ignored for SQLite compatibility)
            username: Legacy parameter (ignored for SQLite compatibility)
            password: Legacy parameter (ignored for SQLite compatibility)
            host: Legacy parameter (ignored for SQLite compatibility)
            port: Legacy parameter (ignored for SQLite compatibility)
            connection_manager: Optional connection manager instance
            workspace_id: Workspace ID for connection pooling
        """
        self.db_path = db_path
        # Legacy attributes for backward compatibility
        self.db_name = db_name or os.path.basename(db_path).replace('.db', '')
        self.username = username
        self.password = password
        self.host = host
        self.port = port
        
        # Ensure database directory exists
        os.makedirs(os.path.dirname(db_path) if os.path.dirname(db_path) else '.', exist_ok=True)
        
        self.connection_string = f"sqlite:///{db_path}"
        self.engine = create_engine(
            self.connection_string,
            pool_pre_ping=True,
            connect_args={"check_same_thread": False}
        )
        self.metadata = MetaData()
        self.inspector = inspect(self.engine)
        self.schema_info = None
        
        # Connection pooling support
        self.connection_manager = connection_manager
        self.workspace_id = workspace_id
        
        # Initialize component analyzers
        self.schema_analyzer = SchemaAnalyzer(self.engine, self.inspector)
        self.schema_analyzer.set_db_name(db_name)
        self.table_analyzer = TableAnalyzer(self.engine, self.inspector)
        self.relationship_analyzer = RelationshipAnalyzer(self.inspector)
        
        # Initialize query components
        self.query_executor = QueryExecutor(self.engine, connection_manager, workspace_id)
        self.transaction_manager = TransactionManager(self.engine, connection_manager, workspace_id)
        self.schema_updater = SchemaUpdater(self.engine, self.inspector, self.table_analyzer, self.relationship_analyzer)
        
    def get_connection(self):
        """Get a database connection, either from pool or direct"""
        if self.connection_manager and self.workspace_id:
            return self.connection_manager.get_connection(self.workspace_id)
        else:
            # Fallback to direct SQLite connection
            return sqlite3.connect(
                self.db_path,
                check_same_thread=False,
                timeout=30.0
            )
        
    def analyze_schema(self) -> Dict[str, Any]:
        """
        Analyze the database schema and return detailed information
        
        Returns:
            Dictionary containing schema information
        """
        self.schema_info = self.schema_analyzer.analyze_schema(
            self.table_analyzer, 
            self.relationship_analyzer
        )
        return self.schema_info
    
    def get_rich_schema_context(self) -> str:
        """
        Get a rich, detailed context about the database schema for the AI
        
        Returns:
            String with rich schema context
        """
        if not self.schema_info:
            self.analyze_schema()
            
        return self.schema_analyzer.get_rich_schema_context(self.schema_info)
    
    def execute_query(self, query: str) -> Tuple[bool, Optional[List[Dict[str, Any]]], Optional[str]]:
        """
        Execute a SQL query and return the results
        
        Args:
            query: SQL query to execute
            
        Returns:
            Tuple of (success, results, error_message)
        """
        return self.query_executor.execute_query(query)

    def execute_query_with_transaction(self, queries: List[str]) -> Tuple[bool, List[Any], Optional[str]]:
        """
        Execute multiple queries in a single transaction with rollback support
        
        Args:
            queries: List of SQL queries to execute in transaction
            
        Returns:
            Tuple of (success, results_list, error_message)
        """
        result = self.transaction_manager.execute_query_with_transaction(queries)
        
        # Check if any schema-changing operations were performed
        if result[0]:  # If transaction was successful
            schema_changed = self.schema_updater.detect_schema_changes(queries)
            if schema_changed and self.schema_info:
                self.schema_info = self.schema_updater.update_schema_from_queries(queries, self.schema_info)
        
        return result
    
    def refresh_schema_for_table(self, table_name: str, schema_name: str = "public") -> bool:
        """
        Refresh schema information for a specific table
        
        Args:
            table_name: Name of the table to refresh
            schema_name: Schema name (defaults to 'public')
            
        Returns:
            True if successful, False otherwise
        """
        if not self.schema_info:
            logger.warning("Schema info not available, running full schema analysis")
            self.analyze_schema()
            return True
        
        return self.schema_updater.refresh_schema_for_table(table_name, schema_name, self.schema_info)
    
    def get_table_info(self, table_name: str, schema_name: str = "public") -> Dict[str, Any]:
        """
        Get detailed information about a specific table
        
        Args:
            table_name: Name of the table
            schema_name: Schema name (defaults to 'public')
            
        Returns:
            Dictionary with table details
        """
        return self.table_analyzer.refresh_table_info(table_name, schema_name)
    
    def get_table_relationships(self, table_name: str, schema_name: str = "public") -> Dict[str, List[Dict[str, Any]]]:
        """
        Get all relationships for a specific table
        
        Args:
            table_name: Name of the table
            schema_name: Schema name (defaults to 'public')
            
        Returns:
            Dictionary with 'outgoing' and 'incoming' relationships
        """
        return self.relationship_analyzer.get_table_relationships(table_name, schema_name)
    
    def get_relationship_graph(self, schema_names: List[str] = ["public"]) -> Dict[str, Any]:
        """
        Get a graph representation of table relationships
        
        Args:
            schema_names: List of schema names to analyze
            
        Returns:
            Dictionary with nodes (tables) and edges (relationships)
        """
        return self.relationship_analyzer.get_relationship_graph(schema_names)
    
    def find_related_tables(self, table_name: str, schema_name: str = "public", max_depth: int = 2) -> List[str]:
        """
        Find all tables related to a given table within a specified depth
        
        Args:
            table_name: Starting table name
            schema_name: Schema name (defaults to 'public')
            max_depth: Maximum relationship depth to explore
            
        Returns:
            List of related table names
        """
        return self.relationship_analyzer.find_related_tables(table_name, schema_name, max_depth)
    
    def get_schema_change_summary(self, queries: List[str]) -> Dict[str, Any]:
        """
        Get a summary of schema changes that would be made by the queries
        
        Args:
            queries: List of SQL queries to analyze
            
        Returns:
            Dictionary summarizing the schema changes
        """
        return self.schema_updater.get_schema_change_summary(queries)
    
    def execute_batch_with_savepoints(self, query_batches: List[List[str]]) -> List[Tuple[bool, List[Any], Optional[str]]]:
        """
        Execute multiple batches of queries with savepoints
        
        Args:
            query_batches: List of query batches
            
        Returns:
            List of results for each batch
        """
        return self.transaction_manager.execute_batch_with_savepoints(query_batches)
    
    def test_connection(self) -> bool:
        """
        Test if the database connection is working
        
        Returns:
            True if connection is working, False otherwise
        """
        return self.query_executor.test_connection()
    
    def get_connection_info(self) -> Dict[str, Any]:
        """
        Get information about the current connection setup
        
        Returns:
            Dictionary with connection information
        """
        return self.query_executor.get_connection_info()


# For backward compatibility
if __name__ == "__main__":
    # Example usage
    from dotenv import load_dotenv
    
    load_dotenv()
    db_path = os.getenv("DB_PATH", "./data/PBTest.db")
    
    # Legacy PostgreSQL parameters (for backward compatibility)
    db_name = os.getenv("DB_NAME", "PBTest")
    username = os.getenv("DB_USERNAME", "")
    password = os.getenv("DB_PASSWORD", "")
    host = os.getenv("DB_HOST", "")
    port = os.getenv("DB_PORT", "")
    
    analyzer = DatabaseAnalyzer(db_path, db_name, username, password, host, port)
    schema = analyzer.analyze_schema()
    
    # Print schema summary
    print(analyzer.get_rich_schema_context())
    
    # Example query
    with analyzer.engine.connect() as connection:
        result = connection.execute(text("SELECT schema_name FROM information_schema.schemata WHERE schema_name NOT IN ('information_schema', 'pg_catalog')"))
        schema_names = [row[0] for row in result]
        print("\nAvailable schemas:")
        for schema_name in schema_names:
            print(f"- {schema_name}")
        
        print("\nTables in database:")
        for schema_name in schema_names:
            result = connection.execute(text(f"SELECT table_name FROM information_schema.tables WHERE table_schema = '{schema_name}'"))
            for row in result:
                print(f"- {schema_name}.{row[0]}")

    # Example of SingleTableAnalyzer usage
    print("\n" + "="*80)
    print("SINGLE TABLE ANALYZER EXAMPLE")
    print("="*80)
    
    # Initialize single table analyzer
    single_analyzer = SingleTableAnalyzer(
        db_path=db_path,
        table_name="IT_Professional_Services",
        schema_name=None  # SQLite doesn't use schemas
    )
    
    # Perform analysis
    result = single_analyzer.analyze_table()
    
    if result.get("success"):
        print("Single table analysis completed successfully!")
        print(f"Analysis saved to: {single_analyzer.output_file}")
        print("\nSummary:")
        print(single_analyzer.get_analysis_summary())
    else:
        print(f"Single table analysis failed: {result.get('error')}")
    
    # Show LLM context
    print("\nLLM Context Preview:")
    print(single_analyzer.get_llm_context()[:500] + "...")

# Export both analyzers
__all__ = ['DatabaseAnalyzer', 'SingleTableAnalyzer'] 
>>>>>>> c5063088d3be74295c4fd89780135628a9cb5739
