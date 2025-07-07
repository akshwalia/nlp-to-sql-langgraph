from typing import Dict, List, Any
import re
import logging

logger = logging.getLogger(__name__)


class SchemaUpdater:
    """
    Handles detection and processing of schema changes from SQL queries
    """
    
    def __init__(self, engine, inspector, table_analyzer, relationship_analyzer):
        """
        Initialize the schema updater
        
        Args:
            engine: SQLAlchemy engine instance
            inspector: SQLAlchemy inspector instance
            table_analyzer: TableAnalyzer instance
            relationship_analyzer: RelationshipAnalyzer instance
        """
        self.engine = engine
        self.inspector = inspector
        self.table_analyzer = table_analyzer
        self.relationship_analyzer = relationship_analyzer
    
    def detect_schema_changes(self, queries: List[str]) -> bool:
        """
        Detect if any of the queries contain schema-changing operations
        
        Args:
            queries: List of SQL queries to check
            
        Returns:
            True if schema changes are detected, False otherwise
        """
        schema_change_keywords = [
            'CREATE TABLE', 'DROP TABLE', 'ALTER TABLE',
            'CREATE INDEX', 'DROP INDEX', 'CREATE UNIQUE INDEX',
            'TRUNCATE TABLE', 'RENAME TABLE',
            'ADD COLUMN', 'DROP COLUMN', 'ALTER COLUMN',
            'ADD CONSTRAINT', 'DROP CONSTRAINT',
            'CREATE SCHEMA', 'DROP SCHEMA'
        ]
        
        for query in queries:
            query_upper = query.upper().strip()
            if any(keyword in query_upper for keyword in schema_change_keywords):
                logger.info(f"Schema change detected in query: {query[:100]}...")
                return True
        
        return False
    
    def update_schema_from_queries(self, queries: List[str], schema_info: Dict[str, Any]) -> Dict[str, Any]:
        """
        Update schema information based on the executed queries
        
        Args:
            queries: List of SQL queries that were executed
            schema_info: Current schema information to update
            
        Returns:
            Updated schema information
        """
        try:
            for query in queries:
                if self.detect_schema_changes([query]):
                    self._process_schema_change_query(query, schema_info)
            
            logger.info("Schema information updated successfully")
            return schema_info
            
        except Exception as e:
            logger.error(f"Error updating schema from queries: {e}")
            return schema_info
    
    def _process_schema_change_query(self, query: str, schema_info: Dict[str, Any]) -> None:
        """
        Process a single schema-changing query
        
        Args:
            query: SQL query that changes schema
            schema_info: Schema information to update
        """
        query_upper = query.upper().strip()
        
        # Handle CREATE TABLE
        if 'CREATE TABLE' in query_upper:
            self._handle_create_table(query, schema_info)
        
        # Handle DROP TABLE
        elif 'DROP TABLE' in query_upper:
            self._handle_drop_table(query, schema_info)
        
        # Handle ALTER TABLE
        elif 'ALTER TABLE' in query_upper:
            self._handle_alter_table(query, schema_info)
        
        # Handle CREATE INDEX
        elif 'CREATE INDEX' in query_upper:
            self._handle_create_index(query, schema_info)
        
        # Handle DROP INDEX
        elif 'DROP INDEX' in query_upper:
            self._handle_drop_index(query, schema_info)
        
        # Handle TRUNCATE TABLE
        elif 'TRUNCATE TABLE' in query_upper:
            self._handle_truncate_table(query, schema_info)
    
    def _handle_create_table(self, query: str, schema_info: Dict[str, Any]) -> None:
        """Handle CREATE TABLE query"""
        try:
            # Extract table name from query
            match = re.search(r'CREATE TABLE\s+(?:IF NOT EXISTS\s+)?(?:(\w+)\.)?(\w+)', query, re.IGNORECASE)
            if match:
                schema_name = match.group(1) or 'public'
                table_name = match.group(2)
                
                # Get fresh table info from database
                with self.engine.connect() as connection:
                    table_info = self.table_analyzer.get_table_info(table_name, connection, schema_name)
                    
                    # Add to schema info
                    qualified_name = f"{schema_name}.{table_name}"
                    schema_info["tables"][qualified_name] = table_info
                    schema_info["tables"][table_name] = table_info
                    
                    logger.info(f"Added new table to schema: {qualified_name}")
        except Exception as e:
            logger.error(f"Error handling CREATE TABLE: {e}")
    
    def _handle_drop_table(self, query: str, schema_info: Dict[str, Any]) -> None:
        """Handle DROP TABLE query"""
        try:
            match = re.search(r'DROP TABLE\s+(?:IF EXISTS\s+)?(?:(\w+)\.)?(\w+)', query, re.IGNORECASE)
            if match:
                schema_name = match.group(1) or 'public'
                table_name = match.group(2)
                
                # Remove from schema info
                qualified_name = f"{schema_name}.{table_name}"
                if qualified_name in schema_info["tables"]:
                    del schema_info["tables"][qualified_name]
                if table_name in schema_info["tables"]:
                    del schema_info["tables"][table_name]
                
                # Remove related foreign keys
                schema_info["relationships"] = self.relationship_analyzer.remove_table_relationships(
                    table_name, schema_name, schema_info["relationships"]
                )
                
                logger.info(f"Removed table from schema: {qualified_name}")
        except Exception as e:
            logger.error(f"Error handling DROP TABLE: {e}")
    
    def _handle_alter_table(self, query: str, schema_info: Dict[str, Any]) -> None:
        """Handle ALTER TABLE query"""
        try:
            # Extract table name
            match = re.search(r'ALTER TABLE\s+(?:(\w+)\.)?(\w+)', query, re.IGNORECASE)
            if match:
                schema_name = match.group(1) or 'public'
                table_name = match.group(2)
                qualified_name = f"{schema_name}.{table_name}"
                
                # For ALTER TABLE, refresh the specific table info
                if qualified_name in schema_info["tables"] or table_name in schema_info["tables"]:
                    with self.engine.connect() as connection:
                        updated_table_info = self.table_analyzer.get_table_info(table_name, connection, schema_name)
                        schema_info["tables"][qualified_name] = updated_table_info
                        schema_info["tables"][table_name] = updated_table_info
                        
                        logger.info(f"Updated table schema: {qualified_name}")
                        
                        # If it's adding/dropping foreign keys, update relationships
                        if 'FOREIGN KEY' in query.upper() or 'DROP CONSTRAINT' in query.upper():
                            # Re-analyze relationships for this schema
                            schema_info["relationships"] = self.relationship_analyzer.analyze_relationships([schema_name])
        except Exception as e:
            logger.error(f"Error handling ALTER TABLE: {e}")
    
    def _handle_create_index(self, query: str, schema_info: Dict[str, Any]) -> None:
        """Handle CREATE INDEX query"""
        try:
            # Extract table name from CREATE INDEX
            match = re.search(r'ON\s+(?:(\w+)\.)?(\w+)', query, re.IGNORECASE)
            if match:
                schema_name = match.group(1) or 'public'
                table_name = match.group(2)
                qualified_name = f"{schema_name}.{table_name}"
                
                # Refresh indexes for this table
                if qualified_name in schema_info["tables"] or table_name in schema_info["tables"]:
                    new_indexes = self.inspector.get_indexes(table_name, schema=schema_name)
                    schema_info["tables"][qualified_name]["indexes"] = new_indexes
                    schema_info["tables"][table_name]["indexes"] = new_indexes
                    
                    logger.info(f"Updated indexes for table: {qualified_name}")
        except Exception as e:
            logger.error(f"Error handling CREATE INDEX: {e}")
    
    def _handle_drop_index(self, query: str, schema_info: Dict[str, Any]) -> None:
        """Handle DROP INDEX query"""
        try:
            # For DROP INDEX, we need to find which table it belongs to
            # This is more complex as we need to identify the table from the index name
            # For now, we'll refresh all table indexes in the schema
            
            # Extract schema if specified
            match = re.search(r'DROP INDEX\s+(?:IF EXISTS\s+)?(?:(\w+)\.)?(\w+)', query, re.IGNORECASE)
            if match:
                schema_name = match.group(1) or 'public'
                index_name = match.group(2)
                
                # Refresh indexes for all tables in the schema
                # This is less efficient but ensures consistency
                for table_key, table_info in schema_info["tables"].items():
                    if table_info.get("schema") == schema_name:
                        table_name = table_key.split(".")[-1]
                        try:
                            new_indexes = self.inspector.get_indexes(table_name, schema=schema_name)
                            table_info["indexes"] = new_indexes
                        except Exception as e:
                            logger.error(f"Error refreshing indexes for table {table_name}: {e}")
                
                logger.info(f"Refreshed indexes for schema: {schema_name}")
        except Exception as e:
            logger.error(f"Error handling DROP INDEX: {e}")
    
    def _handle_truncate_table(self, query: str, schema_info: Dict[str, Any]) -> None:
        """Handle TRUNCATE TABLE query"""
        try:
            match = re.search(r'TRUNCATE TABLE\s+(?:(\w+)\.)?(\w+)', query, re.IGNORECASE)
            if match:
                schema_name = match.group(1) or 'public'
                table_name = match.group(2)
                qualified_name = f"{schema_name}.{table_name}"
                
                # Update row count to 0 and clear sample data
                if qualified_name in schema_info["tables"]:
                    schema_info["tables"][qualified_name]["row_count"] = 0
                    schema_info["tables"][qualified_name]["sample_data"] = None
                if table_name in schema_info["tables"]:
                    schema_info["tables"][table_name]["row_count"] = 0
                    schema_info["tables"][table_name]["sample_data"] = None
                
                logger.info(f"Updated row count for truncated table: {qualified_name}")
        except Exception as e:
            logger.error(f"Error handling TRUNCATE TABLE: {e}")
    
    def refresh_schema_for_table(self, table_name: str, schema_name: str, schema_info: Dict[str, Any]) -> bool:
        """
        Refresh schema information for a specific table
        
        Args:
            table_name: Name of the table to refresh
            schema_name: Schema name (defaults to 'public')
            schema_info: Schema information to update
            
        Returns:
            True if successful, False otherwise
        """
        try:
            with self.engine.connect() as connection:
                table_info = self.table_analyzer.get_table_info(table_name, connection, schema_name)
                
                qualified_name = f"{schema_name}.{table_name}"
                schema_info["tables"][qualified_name] = table_info
                schema_info["tables"][table_name] = table_info
                
                logger.info(f"Refreshed schema for table: {qualified_name}")
                return True
        except Exception as e:
            logger.error(f"Error refreshing schema for table {table_name}: {e}")
            return False
    
    def get_schema_change_summary(self, queries: List[str]) -> Dict[str, Any]:
        """
        Get a summary of schema changes that would be made by the queries
        
        Args:
            queries: List of SQL queries to analyze
            
        Returns:
            Dictionary summarizing the schema changes
        """
        changes = {
            "tables_created": [],
            "tables_dropped": [],
            "tables_altered": [],
            "indexes_created": [],
            "indexes_dropped": [],
            "tables_truncated": [],
            "total_changes": 0
        }
        
        for query in queries:
            query_upper = query.upper().strip()
            
            if 'CREATE TABLE' in query_upper:
                match = re.search(r'CREATE TABLE\s+(?:IF NOT EXISTS\s+)?(?:(\w+)\.)?(\w+)', query, re.IGNORECASE)
                if match:
                    schema_name = match.group(1) or 'public'
                    table_name = match.group(2)
                    changes["tables_created"].append(f"{schema_name}.{table_name}")
            
            elif 'DROP TABLE' in query_upper:
                match = re.search(r'DROP TABLE\s+(?:IF EXISTS\s+)?(?:(\w+)\.)?(\w+)', query, re.IGNORECASE)
                if match:
                    schema_name = match.group(1) or 'public'
                    table_name = match.group(2)
                    changes["tables_dropped"].append(f"{schema_name}.{table_name}")
            
            elif 'ALTER TABLE' in query_upper:
                match = re.search(r'ALTER TABLE\s+(?:(\w+)\.)?(\w+)', query, re.IGNORECASE)
                if match:
                    schema_name = match.group(1) or 'public'
                    table_name = match.group(2)
                    changes["tables_altered"].append(f"{schema_name}.{table_name}")
            
            elif 'CREATE INDEX' in query_upper:
                match = re.search(r'ON\s+(?:(\w+)\.)?(\w+)', query, re.IGNORECASE)
                if match:
                    schema_name = match.group(1) or 'public'
                    table_name = match.group(2)
                    changes["indexes_created"].append(f"{schema_name}.{table_name}")
            
            elif 'DROP INDEX' in query_upper:
                match = re.search(r'DROP INDEX\s+(?:IF EXISTS\s+)?(?:(\w+)\.)?(\w+)', query, re.IGNORECASE)
                if match:
                    schema_name = match.group(1) or 'public'
                    index_name = match.group(2)
                    changes["indexes_dropped"].append(f"{schema_name}.{index_name}")
            
            elif 'TRUNCATE TABLE' in query_upper:
                match = re.search(r'TRUNCATE TABLE\s+(?:(\w+)\.)?(\w+)', query, re.IGNORECASE)
                if match:
                    schema_name = match.group(1) or 'public'
                    table_name = match.group(2)
                    changes["tables_truncated"].append(f"{schema_name}.{table_name}")
        
        changes["total_changes"] = (
            len(changes["tables_created"]) + len(changes["tables_dropped"]) + 
            len(changes["tables_altered"]) + len(changes["indexes_created"]) + 
            len(changes["indexes_dropped"]) + len(changes["tables_truncated"])
        )
        
        return changes 