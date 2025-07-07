from typing import Dict, List, Any
from sqlalchemy import text
import logging

logger = logging.getLogger(__name__)


class SchemaAnalyzer:
    """
    Handles database schema analysis and context generation
    """
    
    def __init__(self, engine, inspector):
        """
        Initialize the schema analyzer
        
        Args:
            engine: SQLAlchemy engine instance
            inspector: SQLAlchemy inspector instance
        """
        self.engine = engine
        self.inspector = inspector
    
    def analyze_schema(self, table_analyzer, relationship_analyzer) -> Dict[str, Any]:
        """
        Analyze the database schema and return detailed information
        
        Args:
            table_analyzer: TableAnalyzer instance
            relationship_analyzer: RelationshipAnalyzer instance
            
        Returns:
            Dictionary containing schema information
        """
        logger.info("Analyzing database schema...")
        schema = {}
        
        # Get all schemas
        with self.engine.connect() as connection:
            result = connection.execute(text("SELECT schema_name FROM information_schema.schemata WHERE schema_name NOT IN ('information_schema', 'pg_catalog')"))
            schema_names = [row[0] for row in result]
        
        schema["schemas"] = schema_names
        schema["tables"] = {}
        
        # Table statistics
        with self.engine.connect() as connection:
            for schema_name in schema_names:
                table_names = self.inspector.get_table_names(schema=schema_name)
                for table_name in table_names:
                    table_info = table_analyzer.get_table_info(table_name, connection, schema_name)
                    # Store with schema-qualified name
                    schema["tables"][f"{schema_name}.{table_name}"] = table_info
                    
                    # For backward compatibility, also store with just table name if it doesn't exist yet
                    # This ensures older code that access tables by just name continues to work
                    if table_name not in schema["tables"]:
                        schema["tables"][table_name] = table_info
        
        # Analyze relationships
        schema["relationships"] = relationship_analyzer.analyze_relationships(schema_names)
        
        # Generate schema summary
        schema["summary"] = self._generate_schema_summary(schema)
        
        return schema
    
    def _generate_schema_summary(self, schema: Dict) -> str:
        """
        Generate a summary of the schema for the AI context
        
        Args:
            schema: The complete schema information
            
        Returns:
            A string summary of the schema
        """
        summary_parts = ["DATABASE SCHEMA SUMMARY:", ""]
        
        # Table summary
        db_name = getattr(self, 'db_name', 'database')
        summary_parts.append(f"Database: {db_name}")
        
        # Get list of unique tables (avoid duplicates from schema qualified and non-qualified names)
        seen_tables = set()
        unique_tables = []
        
        # First add tables without schema prefix
        for table_name, table_info in schema['tables'].items():
            if "." not in table_name:
                unique_tables.append((table_name, table_info))
                seen_tables.add(table_name.split(".")[-1])
        
        # Then add qualified tables that haven't been seen yet
        for table_name, table_info in schema['tables'].items():
            if "." in table_name:
                simple_name = table_name.split(".")[-1]
                if simple_name not in seen_tables:
                    unique_tables.append((table_name, table_info))
                    seen_tables.add(simple_name)
        
        summary_parts.append(f"Total tables: {len(unique_tables)}")
        summary_parts.append("")
        
        # Tables and columns
        summary_parts.append("TABLES:")
        for table_name, table_info in unique_tables:
            # Include schema name if available in the table_info
            display_name = table_name
            if "schema" in table_info and "." not in table_name:
                display_name = f"{table_info['schema']}.{table_name}"
                
            summary_parts.append(f"\nTable: {display_name} ({table_info['row_count']} rows)")
            
            # Primary key
            if table_info["primary_key"]:
                summary_parts.append(f"Primary Key: {', '.join(table_info['primary_key'])}")
            
            # Foreign keys
            if table_info["foreign_keys"]:
                for fk in table_info["foreign_keys"]:
                    source_cols = ', '.join(fk['constrained_columns'])
                    target_cols = ', '.join(fk['referred_columns'])
                    referred_table = fk['referred_table']
                    if 'referred_schema' in fk and fk['referred_schema'] != 'public':
                        referred_table = f"{fk['referred_schema']}.{referred_table}"
                    summary_parts.append(f"Foreign Key: {source_cols} -> {referred_table}({target_cols})")
            
            # Columns
            summary_parts.append("Columns:")
            for column in table_info["columns"]:
                nullable = "" if column["nullable"] else "NOT NULL"
                summary_parts.append(f"  - {column['name']} ({column['type']}) {nullable}")
                
                # Add stats if available
                if column["stats"]:
                    stats = column["stats"]
                    if "min" in stats and "max" in stats:
                        summary_parts.append(f"    Range: {stats['min']} to {stats['max']}")
                    if "distinct_count" in stats:
                        summary_parts.append(f"    Distinct values: {stats['distinct_count']}")
                    if "null_percentage" in stats and stats["null_percentage"] > 0:
                        summary_parts.append(f"    Null values: {stats['null_percentage']:.1f}%")
            
            # Sample data hint
            if table_info["sample_data"]:
                summary_parts.append("  (Sample data available)")
        
        # Relationships summary
        if schema['relationships']:
            summary_parts.append("\nRELATIONSHIPS:")
            for rel in schema['relationships']:
                source = f"{rel['source_schema']}.{rel['source_table']}({', '.join(rel['source_columns'])})"
                target = f"{rel['target_schema']}.{rel['target_table']}({', '.join(rel['target_columns'])})"
                summary_parts.append(f"  - {source} -> {target}")
        
        return "\n".join(summary_parts)
    
    def get_rich_schema_context(self, schema_info: Dict) -> str:
        """
        Get a rich, detailed context about the database schema for the AI
        
        Args:
            schema_info: Schema information dictionary
            
        Returns:
            String with rich schema context
        """
        context_parts = [schema_info["summary"], ""]
        
        # Get list of unique table references (avoid duplicates from schema qualified and non-qualified names)
        # Prioritize unqualified table names for backward compatibility
        seen_tables = set()
        unique_tables = []
        
        # First add tables without schema prefix
        for table_name, table_info in schema_info["tables"].items():
            if "." not in table_name:
                unique_tables.append((table_name, table_info))
                seen_tables.add(table_name.split(".")[-1])
        
        # Then add qualified tables that haven't been seen yet
        for table_name, table_info in schema_info["tables"].items():
            if "." in table_name:
                simple_name = table_name.split(".")[-1]
                if simple_name not in seen_tables:
                    unique_tables.append((table_name, table_info))
                    seen_tables.add(simple_name)
        
        # Add detailed table information
        context_parts.append("DETAILED TABLE INFORMATION:")
        for table_name, table_info in unique_tables:
            context_parts.append(f"\n--- Table: {table_name} ---")
            
            # Basic info
            context_parts.append(f"Rows: {table_info['row_count']}")
            
            # Sample data for context
            if table_info["sample_data"]:
                context_parts.append("Sample data:")
                for i, row in enumerate(table_info["sample_data"][:3]):  # Show first 3 rows
                    context_parts.append(f"  Row {i+1}: {row}")
            
            # Column details with statistics
            context_parts.append("Column details:")
            for column in table_info["columns"]:
                col_desc = f"  {column['name']} ({column['type']})"
                if not column["nullable"]:
                    col_desc += " NOT NULL"
                if column["primary_key"]:
                    col_desc += " PRIMARY KEY"
                
                # Add statistics if available
                if column["stats"]:
                    stats = column["stats"]
                    stat_parts = []
                    if "min" in stats and "max" in stats:
                        stat_parts.append(f"range: {stats['min']}-{stats['max']}")
                    if "distinct_count" in stats:
                        stat_parts.append(f"distinct: {stats['distinct_count']}")
                    if "null_percentage" in stats and stats["null_percentage"] > 0:
                        stat_parts.append(f"null: {stats['null_percentage']:.1f}%")
                    if "top_values" in stats:
                        top_vals = [f"{v['value']}({v['count']})" for v in stats["top_values"][:3]]
                        stat_parts.append(f"top values: {', '.join(top_vals)}")
                    
                    if stat_parts:
                        col_desc += f" [{', '.join(stat_parts)}]"
                
                context_parts.append(col_desc)
        
        return "\n".join(context_parts)
    
    def set_db_name(self, db_name: str):
        """Set the database name for context generation"""
        self.db_name = db_name 