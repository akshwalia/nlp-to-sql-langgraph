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
        logger.info("Starting comprehensive database schema analysis...")
        schema = {}
        
        # Get all schemas
        logger.info("Retrieving schema names from database...")
        with self.engine.connect() as connection:
            logger.debug("Executing query to get schema names")
            result = connection.execute(text("SELECT schema_name FROM information_schema.schemata WHERE schema_name NOT IN ('information_schema', 'pg_catalog')"))
            schema_names = [row[0] for row in result]
            logger.info(f"Found {len(schema_names)} schemas: {schema_names}")
        
        schema["schemas"] = schema_names
        schema["tables"] = {}
        
        # Table statistics
        logger.info("Analyzing tables and gathering statistics...")
        with self.engine.connect() as connection:
            total_tables = 0
            total_rows = 0
            
            for schema_name in schema_names:
                logger.info(f"Analyzing schema: {schema_name}")
                
                table_names = self.inspector.get_table_names(schema=schema_name)
                logger.debug(f"Found {len(table_names)} tables in schema {schema_name}: {table_names}")
                
                total_tables += len(table_names)
                
                for table_name in table_names:
                    logger.debug(f"Getting table info for: {schema_name}.{table_name}")
                    
                    try:
                        table_info = table_analyzer.get_table_info(table_name, connection, schema_name)
                        schema["tables"][f"{schema_name}.{table_name}"] = table_info
                        
                        # Add to total row count
                        row_count = table_info.get('row_count', 0)
                        if isinstance(row_count, int):
                            total_rows += row_count
                        
                        logger.debug(f"Successfully analyzed table: {schema_name}.{table_name} ({row_count} rows)")
                    except Exception as e:
                        logger.error(f"Error analyzing table {schema_name}.{table_name}: {e}")
                        schema["tables"][f"{schema_name}.{table_name}"] = {"error": str(e)}
                
                logger.info(f"Completed analysis for schema: {schema_name}")
        
        # Analyze relationships
        logger.info("Analyzing table relationships...")
        try:
            relationships = relationship_analyzer.analyze_relationships(schema_names)
            schema["relationships"] = relationships
            logger.info(f"Found {len(relationships)} relationships across all schemas")
        except Exception as e:
            logger.error(f"Error analyzing relationships: {e}")
            schema["relationships"] = []
        
        # Generate summary statistics
        logger.info("Generating schema summary statistics...")
        successful_tables = len([t for t in schema["tables"].values() if "error" not in t])
        failed_tables = len([t for t in schema["tables"].values() if "error" in t])
        
        schema["summary"] = {
            "total_schemas": len(schema_names),
            "total_tables": total_tables,
            "successful_analyses": successful_tables,
            "failed_analyses": failed_tables,
            "total_rows": total_rows,
            "total_relationships": len(relationships)
        }
        
        logger.info(f"Schema analysis completed successfully!")
        logger.info(f"Summary: {successful_tables}/{total_tables} tables analyzed, {total_rows} total rows, {len(relationships)} relationships")
        
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
        context_parts = [self._generate_schema_summary(schema_info), ""]
        
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
            
            # Column details with statistics and description metadata
            context_parts.append("Column details:")
            
            # Get description table info if available
            description_table = table_info.get("description_table")
            
            for column in table_info["columns"]:
                col_desc = f"  {column['name']} ({column['type']})"
                if not column["nullable"]:
                    col_desc += " NOT NULL"
                if column["primary_key"]:
                    col_desc += " PRIMARY KEY"
                
                # Add description metadata if available
                if description_table and column['name'] in description_table["columns"]:
                    desc_info = description_table["columns"][column['name']]
                    
                    # Add common name if different from column name
                    if desc_info["common_name"] and desc_info["common_name"] != column['name']:
                        col_desc += f" ('{desc_info['common_name']}')"
                    
                    # Add importance indicators
                    importance_flags = []
                    if desc_info["must_have"]:
                        importance_flags.append("MUST_HAVE")
                    if desc_info["is_important"]:
                        importance_flags.append("IMPORTANT")
                    if desc_info["mandatory_entity"]:
                        importance_flags.append("MANDATORY")
                    
                    if importance_flags:
                        col_desc += f" [{', '.join(importance_flags)}]"
                    
                    # Add description
                    if desc_info["description"]:
                        col_desc += f" - {desc_info['description']}"
                
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
                        col_desc += f" (Stats: {', '.join(stat_parts)})"
                
                context_parts.append(col_desc)
            
            # Add description table summary if available
            if description_table:
                context_parts.append(f"\nColumn Priority Information:")
                if description_table["must_have_columns"]:
                    context_parts.append(f"  MUST_HAVE columns: {', '.join(description_table['must_have_columns'])}")
                if description_table["important_columns"]:
                    context_parts.append(f"  IMPORTANT columns: {', '.join(description_table['important_columns'])}")
                if description_table["mandatory_entity_columns"]:
                    context_parts.append(f"  MANDATORY columns: {', '.join(description_table['mandatory_entity_columns'])}")
                
                context_parts.append(f"  Note: When there's ambiguity between columns, prefer MUST_HAVE columns over others.")
        
        return "\n".join(context_parts)
    
    def set_db_name(self, db_name: str):
        """Set the database name for context generation"""
        self.db_name = db_name 