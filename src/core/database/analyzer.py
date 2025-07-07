import os
import pandas as pd
from sqlalchemy import create_engine, inspect, MetaData, text
from sqlalchemy.ext.automap import automap_base
from typing import Dict, List, Any, Tuple, Optional
import psycopg2
import warnings

# Suppress SQLAlchemy warnings for unrecognized column types
warnings.filterwarnings("ignore", "Did not recognize type", module="sqlalchemy")


class DatabaseAnalyzer:
    """
    Analyzes a PostgreSQL database schema and provides detailed information
    about tables, columns, relationships, and data distributions
    """
    
    def __init__(
        self,
        db_name: str,
        username: str,
        password: str,
        host: str = "localhost",
        port: str = "5432",
        connection_manager=None,
        workspace_id: str = None
    ):
        """
        Initialize the database analyzer
        
        Args:
            db_name: PostgreSQL database name
            username: PostgreSQL username
            password: PostgreSQL password
            host: PostgreSQL host
            port: PostgreSQL port
            connection_manager: Optional connection manager instance
            workspace_id: Workspace ID for connection pooling
        """
        self.db_name = db_name
        self.username = username
        self.password = password
        self.host = host
        self.port = port
        self.connection_string = f"postgresql://{username}:{password}@{host}:{port}/{db_name}"
        self.engine = create_engine(self.connection_string)
        self.metadata = MetaData()
        self.inspector = inspect(self.engine)
        self.schema_info = None
        
        # Connection pooling support
        self.connection_manager = connection_manager
        self.workspace_id = workspace_id
        
    def get_connection(self):
        """Get a database connection, either from pool or direct"""
        if self.connection_manager and self.workspace_id:
            return self.connection_manager.get_connection(self.workspace_id)
        else:
            # Fallback to direct connection
            return psycopg2.connect(
                host=self.host,
                port=self.port,
                database=self.db_name,
                user=self.username,
                password=self.password
            )
        
    def analyze_schema(self) -> Dict[str, Any]:
        """
        Analyze the database schema and return detailed information
        
        Returns:
            Dictionary containing schema information
        """
        print("Analyzing database schema...")
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
                    table_info = self._get_table_info(table_name, connection, schema_name)
                    # Store with schema-qualified name
                    schema["tables"][f"{schema_name}.{table_name}"] = table_info
                    
                    # For backward compatibility, also store with just table name if it doesn't exist yet
                    # This ensures older code that access tables by just name continues to work
                    if table_name not in schema["tables"]:
                        schema["tables"][table_name] = table_info
        
        # Analyze relationships
        schema["relationships"] = self._analyze_relationships(schema_names)
        
        # Generate schema summary
        schema["summary"] = self._generate_schema_summary(schema)
        
        self.schema_info = schema
        return schema
    
    def _get_table_info(self, table_name: str, connection, schema_name: str = "public") -> Dict[str, Any]:
        """
        Get detailed information about a table
        
        Args:
            table_name: Name of the table
            connection: Database connection
            schema_name: Schema name (defaults to 'public')
            
        Returns:
            Dictionary with table details
        """
        table_info = {
            "schema": schema_name,
            "columns": [],
            "primary_key": self.inspector.get_pk_constraint(table_name, schema=schema_name).get('constrained_columns', []),
            "foreign_keys": [],
            "indexes": self.inspector.get_indexes(table_name, schema=schema_name),
            "row_count": 0,
            "sample_data": None
        }
        
        # Get column information
        for column in self.inspector.get_columns(table_name, schema=schema_name):
            column_info = {
                "name": column["name"],
                "type": str(column["type"]),
                "nullable": column.get("nullable", True),
                "default": str(column.get("default", "")),
                "primary_key": column["name"] in table_info["primary_key"],
                "stats": {}
            }
            
            table_info["columns"].append(column_info)
        
        # Get foreign keys
        for fk in self.inspector.get_foreign_keys(table_name, schema=schema_name):
            table_info["foreign_keys"].append({
                "constrained_columns": fk["constrained_columns"],
                "referred_table": fk["referred_table"],
                "referred_schema": fk.get("referred_schema", schema_name),
                "referred_columns": fk["referred_columns"]
            })
        
        # Get row count
        try:
            result = connection.execute(text(f"SELECT COUNT(*) FROM \"{schema_name}\".\"{table_name}\""))
            table_info["row_count"] = result.scalar()
        except Exception as e:
            print(f"Error getting row count for {schema_name}.{table_name}: {e}")
            table_info["row_count"] = "Error"
        
        # Get sample data (first 5 rows)
        try:
            result = connection.execute(text(f"SELECT * FROM \"{schema_name}\".\"{table_name}\" LIMIT 5"))
            rows = []
            for row in result:
                # Convert row to dictionary properly
                row_dict = {}
                for idx, column in enumerate(result.keys()):
                    row_dict[column] = row[idx]
                rows.append(row_dict)
            
            if rows:
                table_info["sample_data"] = rows
        except Exception as e:
            print(f"Error getting sample data for {schema_name}.{table_name}: {e}")
        
        # Get column statistics
        if table_info["row_count"] and table_info["row_count"] != "Error" and table_info["row_count"] > 0:
            self._analyze_column_statistics(table_name, table_info, connection, schema_name)
        
        return table_info
    
    def _analyze_column_statistics(self, table_name: str, table_info: Dict, connection, schema_name: str = "public") -> None:
        """
        Analyze statistics for columns in a table
        
        Args:
            table_name: Name of the table
            table_info: Table information dictionary to update
            connection: Database connection
            schema_name: Schema name (defaults to 'public')
        """
        for i, column in enumerate(table_info["columns"]):
            col_name = column["name"]
            col_type = column["type"].lower()
            
            # Skip BLOB, JSON, etc.
            if any(t in col_type for t in ["blob", "bytea", "json", "xml"]):
                continue
            
            stats = {}
            
            try:
                # Analyze based on column type
                if any(t in col_type for t in ["int", "double", "float", "numeric", "decimal"]):
                    # Numeric column analysis
                    result = connection.execute(text(
                        f"SELECT MIN(\"{col_name}\"), MAX(\"{col_name}\"), AVG(\"{col_name}\"), "
                        f"PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY \"{col_name}\") "
                        f"FROM \"{schema_name}\".\"{table_name}\" WHERE \"{col_name}\" IS NOT NULL"
                    ))
                    row = result.first()
                    if row:
                        stats["min"] = row[0]
                        stats["max"] = row[1]
                        stats["avg"] = row[2]
                        stats["median"] = row[3]
                
                # Analyze null percentage for all columns
                result = connection.execute(text(
                    f"SELECT (COUNT(*) - COUNT(\"{col_name}\")) * 100.0 / COUNT(*) "
                    f"FROM \"{schema_name}\".\"{table_name}\""
                ))
                null_percentage = result.scalar() or 0
                stats["null_percentage"] = null_percentage
                
                # For categorical columns, get distinct value count and top 5 values
                if any(t in col_type for t in ["char", "text", "enum", "bool"]) or "date" in col_type:
                    # Get distinct value count
                    result = connection.execute(text(
                        f"SELECT COUNT(DISTINCT \"{col_name}\") FROM \"{schema_name}\".\"{table_name}\""
                    ))
                    distinct_count = result.scalar() or 0
                    stats["distinct_count"] = distinct_count
                    
                    # Get top 5 most frequent values if not too many distinct values
                    if distinct_count > 0 and distinct_count < 1000:
                        result = connection.execute(text(
                            f"SELECT \"{col_name}\", COUNT(*) as count "
                            f"FROM \"{schema_name}\".\"{table_name}\" "
                            f"WHERE \"{col_name}\" IS NOT NULL "
                            f"GROUP BY \"{col_name}\" "
                            f"ORDER BY count DESC "
                            f"LIMIT 5"
                        ))
                        top_values = []
                        for row in result:
                            top_values.append({"value": str(row[0]), "count": row[1]})
                        stats["top_values"] = top_values
                
                # Update column info with statistics
                table_info["columns"][i]["stats"] = stats
                
            except Exception as e:
                print(f"Error analyzing statistics for {schema_name}.{table_name}.{col_name}: {e}")
    
    def _analyze_relationships(self, schema_names: List[str] = ["public"]) -> List[Dict[str, Any]]:
        """
        Analyze relationships between tables in the database
        
        Args:
            schema_names: List of schema names to analyze
            
        Returns:
            List of relationships
        """
        relationships = []
        
        # Collect all foreign keys
        for schema_name in schema_names:
            for table_name in self.inspector.get_table_names(schema=schema_name):
                for fk in self.inspector.get_foreign_keys(table_name, schema=schema_name):
                    relationship = {
                        "source_schema": schema_name,
                        "source_table": table_name,
                        "source_columns": fk["constrained_columns"],
                        "target_schema": fk.get("referred_schema", schema_name),
                        "target_table": fk["referred_table"],
                        "target_columns": fk["referred_columns"],
                        "name": fk.get("name")
                    }
                    relationships.append(relationship)
        
        return relationships
    
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
        summary_parts.append(f"Database: {self.db_name}")
        
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
    
    def get_rich_schema_context(self) -> str:
        """
        Get a rich, detailed context about the database schema for the AI
        
        Returns:
            String with rich schema context
        """
        if not self.schema_info:
            self.analyze_schema()
            
        context_parts = [self.schema_info["summary"], ""]
        
        # Get list of unique table references (avoid duplicates from schema qualified and non-qualified names)
        # Prioritize unqualified table names for backward compatibility
        seen_tables = set()
        unique_tables = []
        
        # First add tables without schema prefix
        for table_name, table_info in self.schema_info["tables"].items():
            if "." not in table_name:
                unique_tables.append((table_name, table_info))
                seen_tables.add(table_name.split(".")[-1])
        
        # Then add qualified tables that haven't been seen yet
        for table_name, table_info in self.schema_info["tables"].items():
            if "." in table_name:
                simple_name = table_name.split(".")[-1]
                if simple_name not in seen_tables:
                    unique_tables.append((table_name, table_info))
                    seen_tables.add(simple_name)
        
        # Add sample data context for small tables
        context_parts.append("SAMPLE DATA:")
        for table_name, table_info in unique_tables:
            if table_info["sample_data"] and len(table_info["sample_data"]) > 0:
                context_parts.append(f"\nTable: {table_name} (sample rows):")
                df = pd.DataFrame(table_info["sample_data"])
                sample_str = df.to_string(index=False)
                if len(sample_str) < 1000:  # Only include if not too large
                    context_parts.append(sample_str)
                else:
                    # Just show the columns and a few rows
                    context_parts.append(str(df.head(2)))
        
        # Add common query patterns based on schema
        context_parts.append("\nCOMMON QUERY PATTERNS:")
        
        # Find tables that look like transaction tables (likely have date and numeric columns)
        transaction_tables = []
        for table_name, table_info in unique_tables:
            col_types = [col["type"].lower() for col in table_info["columns"]]
            has_date = any("date" in t or "time" in t for t in col_types)
            has_numeric = any(t in "numeric decimal float double int" for t in " ".join(col_types))
            if has_date and has_numeric:
                transaction_tables.append(table_name)
        
        if transaction_tables:
            context_parts.append("\n- Time series queries (for tables with date/time columns):")
            for table in transaction_tables:
                context_parts.append(f"  * Aggregate {table} data by date periods")
        
        # Look for tables with potential hierarchical relationships
        if len(self.schema_info["relationships"]) > 0:
            context_parts.append("\n- Joining related tables:")
            for rel in self.schema_info["relationships"]:
                context_parts.append(
                    f"  * Join {rel['source_schema']}.{rel['source_table']} with {rel['target_schema']}.{rel['target_table']} on "
                    f"{rel['source_schema']}.{rel['source_table']}.{rel['source_columns'][0]} = "
                    f"{rel['target_schema']}.{rel['target_table']}.{rel['target_columns'][0]}"
                )
        
        return "\n".join(context_parts)
    
    def execute_query(self, query: str) -> Tuple[bool, Optional[List[Dict[str, Any]]], Optional[str]]:
        """
        Execute a SQL query and return the results
        
        Args:
            query: SQL query to execute
            
        Returns:
            Tuple of (success, results, error_message)
        """
        try:
            # Use connection pool if available, otherwise fall back to engine
            if self.connection_manager and self.workspace_id:
                with self.connection_manager.get_connection(self.workspace_id) as conn:
                    cursor = conn.cursor()
                    cursor.execute(query)
                    
                    # Check if query returns rows
                    if cursor.description:
                        # Get column names
                        columns = [desc[0] for desc in cursor.description]
                        rows = cursor.fetchall()
                        
                        # Convert to list of dictionaries
                        data = []
                        for row in rows:
                            row_dict = {}
                            for idx, column in enumerate(columns):
                                value = row[idx]
                                # Convert non-serializable types to strings for JSON compatibility
                                if isinstance(value, (pd.Timestamp, pd.Timedelta)):
                                    value = str(value)
                                row_dict[column] = value
                            data.append(row_dict)
                        
                        # Commit the transaction for write operations
                        if query.strip().upper().startswith(('INSERT', 'UPDATE', 'DELETE', 'ALTER', 'DROP', 'CREATE', 'TRUNCATE', 'MERGE', 'RENAME')):
                            conn.commit()
                        
                        cursor.close()
                        return True, data, None
                    else:
                        # For non-SELECT queries (INSERT, UPDATE, DELETE), return rowcount
                        affected_rows = cursor.rowcount
                        
                        # Commit the transaction for write operations
                        if query.strip().upper().startswith(('INSERT', 'UPDATE', 'DELETE', 'ALTER', 'DROP', 'CREATE', 'TRUNCATE', 'MERGE', 'RENAME')):
                            conn.commit()
                        
                        cursor.close()
                        return True, affected_rows, None
            else:
                # Fallback to SQLAlchemy engine
                with self.engine.connect() as connection:
                    result = connection.execute(text(query))
                    
                    if result.returns_rows:
                        # Convert result to a list of dictionaries
                        columns = result.keys()
                        data = []
                        for row in result:
                            row_dict = {}
                            for idx, column in enumerate(columns):
                                value = row[idx]
                                # Convert non-serializable types to strings for JSON compatibility
                                if isinstance(value, (pd.Timestamp, pd.Timedelta)):
                                    value = str(value)
                                row_dict[column] = value
                            data.append(row_dict)
                        
                        # Commit for write operations
                        if query.strip().upper().startswith(('INSERT', 'UPDATE', 'DELETE')):
                            connection.commit()
                        
                        return True, data, None
                    else:
                        # For non-SELECT queries, return rowcount
                        affected_rows = result.rowcount
                        
                        # Commit for write operations
                        if query.strip().upper().startswith(('INSERT', 'UPDATE', 'DELETE')):
                            connection.commit()
                        
                        return True, affected_rows, None
                
        except Exception as e:
            return False, None, str(e)

    def execute_query_with_transaction(self, queries: List[str]) -> Tuple[bool, List[Any], Optional[str]]:
        """
        Execute multiple queries in a single transaction with rollback support
        
        Args:
            queries: List of SQL queries to execute in transaction
            
        Returns:
            Tuple of (success, results_list, error_message)
        """
        results = []
        
        try:
            # Use connection pool if available, otherwise fall back to engine
            if self.connection_manager and self.workspace_id:
                with self.connection_manager.get_connection(self.workspace_id) as conn:
                    # Start transaction explicitly
                    conn.autocommit = False
                    cursor = conn.cursor()
                    
                    try:
                        for i, query in enumerate(queries):
                            cursor.execute(query)
                            
                            # Check if query returns rows
                            if cursor.description:
                                # Get column names
                                columns = [desc[0] for desc in cursor.description]
                                rows = cursor.fetchall()
                                
                                # Convert to list of dictionaries
                                data = []
                                for row in rows:
                                    row_dict = {}
                                    for idx, column in enumerate(columns):
                                        value = row[idx]
                                        # Convert non-serializable types to strings for JSON compatibility
                                        if isinstance(value, (pd.Timestamp, pd.Timedelta)):
                                            value = str(value)
                                        row_dict[column] = value
                                    data.append(row_dict)
                                
                                results.append({
                                    "query_number": i + 1,
                                    "sql": query,
                                    "success": True,
                                    "results": data,
                                    "affected_rows": len(data),
                                    "error": None
                                })
                            else:
                                # For non-SELECT queries, return rowcount
                                affected_rows = cursor.rowcount
                                results.append({
                                    "query_number": i + 1,
                                    "sql": query,
                                    "success": True,
                                    "results": [],
                                    "affected_rows": affected_rows,
                                    "error": None
                                })
                        
                        # If we get here, all queries succeeded - commit the transaction
                        conn.commit()
                        cursor.close()
                        
                        # Check if any schema-changing operations were performed
                        schema_changed = self._detect_schema_changes(queries)
                        if schema_changed:
                            self._update_schema_from_queries(queries)
                        
                        return True, results, None
                        
                    except Exception as e:
                        # Rollback the transaction on any error
                        conn.rollback()
                        cursor.close()
                        
                        # Add error info to the last result
                        results.append({
                            "query_number": len(results) + 1,
                            "sql": queries[len(results)] if len(results) < len(queries) else "Unknown",
                            "success": False,
                            "results": [],
                            "affected_rows": 0,
                            "error": str(e)
                        })
                        
                        return False, results, f"Transaction failed at query {len(results)}: {str(e)}"
                        
            else:
                # Fallback to SQLAlchemy engine with transaction
                with self.engine.begin() as transaction:
                    try:
                        for i, query in enumerate(queries):
                            result = transaction.execute(text(query))
                            
                            if result.returns_rows:
                                # Convert result to a list of dictionaries
                                columns = result.keys()
                                data = []
                                for row in result:
                                    row_dict = {}
                                    for idx, column in enumerate(columns):
                                        value = row[idx]
                                        # Convert non-serializable types to strings for JSON compatibility
                                        if isinstance(value, (pd.Timestamp, pd.Timedelta)):
                                            value = str(value)
                                        row_dict[column] = value
                                    data.append(row_dict)
                                
                                results.append({
                                    "query_number": i + 1,
                                    "sql": query,
                                    "success": True,
                                    "results": data,
                                    "affected_rows": len(data),
                                    "error": None
                                })
                            else:
                                # For non-SELECT queries, return rowcount
                                affected_rows = result.rowcount
                                results.append({
                                    "query_number": i + 1,
                                    "sql": query,
                                    "success": True,
                                    "results": [],
                                    "affected_rows": affected_rows,
                                    "error": None
                                })
                        
                        # Transaction commits automatically if we reach here
                        # Check if any schema-changing operations were performed
                        schema_changed = self._detect_schema_changes(queries)
                        if schema_changed:
                            self._update_schema_from_queries(queries)
                        
                        return True, results, None
                        
                    except Exception as e:
                        # Transaction rolls back automatically
                        results.append({
                            "query_number": len(results) + 1,
                            "sql": queries[len(results)] if len(results) < len(queries) else "Unknown",
                            "success": False,
                            "results": [],
                            "affected_rows": 0,
                            "error": str(e)
                        })
                        
                        return False, results, f"Transaction failed at query {len(results)}: {str(e)}"
                
        except Exception as e:
            return False, results, f"Transaction setup failed: {str(e)}"

    def _detect_schema_changes(self, queries: List[str]) -> bool:
        """
        Detect if any of the queries contain schema-changing operations
        
        Args:
            queries: List of SQL queries to analyze
            
        Returns:
            True if schema changes detected, False otherwise
        """
        schema_changing_keywords = [
            'CREATE TABLE', 'DROP TABLE', 'ALTER TABLE', 
            'CREATE INDEX', 'DROP INDEX', 'CREATE VIEW', 'DROP VIEW',
            'CREATE SCHEMA', 'DROP SCHEMA', 'RENAME TABLE',
            'ADD COLUMN', 'DROP COLUMN', 'RENAME COLUMN',
            'CREATE SEQUENCE', 'DROP SEQUENCE', 'TRUNCATE TABLE'
        ]
        
        for query in queries:
            query_upper = query.upper().strip()
            for keyword in schema_changing_keywords:
                if keyword in query_upper:
                    return True
        
        return False

    def _update_schema_from_queries(self, queries: List[str]) -> None:
        """
        Update schema information based on executed queries without full re-analysis
        
        Args:
            queries: List of executed SQL queries
        """
        if not self.schema_info:
            # If no schema info exists, do full analysis
            self.analyze_schema()
            return
        
        try:
            for query in queries:
                self._process_schema_change_query(query)
            
            # Regenerate schema summary with updated info
            self.schema_info["summary"] = self._generate_schema_summary(self.schema_info)
            
        except Exception as e:
            print(f"Error updating schema from queries: {e}")
            # Fallback to full re-analysis if incremental update fails
            print("Falling back to full schema re-analysis...")
            self.analyze_schema()

    def _process_schema_change_query(self, query: str) -> None:
        """
        Process a single schema-changing query and update schema info
        
        Args:
            query: SQL query that changes schema
        """
        query_upper = query.upper().strip()
        
        # Handle CREATE TABLE
        if 'CREATE TABLE' in query_upper:
            self._handle_create_table(query)
        
        # Handle DROP TABLE
        elif 'DROP TABLE' in query_upper:
            self._handle_drop_table(query)
        
        # Handle ALTER TABLE
        elif 'ALTER TABLE' in query_upper:
            self._handle_alter_table(query)
        
        # Handle CREATE INDEX
        elif 'CREATE INDEX' in query_upper:
            self._handle_create_index(query)
        
        # Handle DROP INDEX
        elif 'DROP INDEX' in query_upper:
            self._handle_drop_index(query)
        
        # Handle TRUNCATE TABLE
        elif 'TRUNCATE TABLE' in query_upper:
            self._handle_truncate_table(query)

    def _handle_create_table(self, query: str) -> None:
        """Handle CREATE TABLE query"""
        try:
            # Extract table name from query
            import re
            match = re.search(r'CREATE TABLE\s+(?:IF NOT EXISTS\s+)?(?:(\w+)\.)?(\w+)', query, re.IGNORECASE)
            if match:
                schema_name = match.group(1) or 'public'
                table_name = match.group(2)
                
                # Get fresh table info from database
                with self.engine.connect() as connection:
                    table_info = self._get_table_info(table_name, connection, schema_name)
                    
                    # Add to schema info
                    qualified_name = f"{schema_name}.{table_name}"
                    self.schema_info["tables"][qualified_name] = table_info
                    self.schema_info["tables"][table_name] = table_info
                    
                    print(f"Added new table to schema: {qualified_name}")
        except Exception as e:
            print(f"Error handling CREATE TABLE: {e}")

    def _handle_drop_table(self, query: str) -> None:
        """Handle DROP TABLE query"""
        try:
            import re
            match = re.search(r'DROP TABLE\s+(?:IF EXISTS\s+)?(?:(\w+)\.)?(\w+)', query, re.IGNORECASE)
            if match:
                schema_name = match.group(1) or 'public'
                table_name = match.group(2)
                
                # Remove from schema info
                qualified_name = f"{schema_name}.{table_name}"
                if qualified_name in self.schema_info["tables"]:
                    del self.schema_info["tables"][qualified_name]
                if table_name in self.schema_info["tables"]:
                    del self.schema_info["tables"][table_name]
                
                # Remove related foreign keys
                self._remove_table_relationships(table_name, schema_name)
                
                print(f"Removed table from schema: {qualified_name}")
        except Exception as e:
            print(f"Error handling DROP TABLE: {e}")

    def _handle_alter_table(self, query: str) -> None:
        """Handle ALTER TABLE query"""
        try:
            import re
            # Extract table name
            match = re.search(r'ALTER TABLE\s+(?:(\w+)\.)?(\w+)', query, re.IGNORECASE)
            if match:
                schema_name = match.group(1) or 'public'
                table_name = match.group(2)
                qualified_name = f"{schema_name}.{table_name}"
                
                # For ALTER TABLE, refresh the specific table info
                if qualified_name in self.schema_info["tables"] or table_name in self.schema_info["tables"]:
                    with self.engine.connect() as connection:
                        updated_table_info = self._get_table_info(table_name, connection, schema_name)
                        self.schema_info["tables"][qualified_name] = updated_table_info
                        self.schema_info["tables"][table_name] = updated_table_info
                        
                        print(f"Updated table schema: {qualified_name}")
                        
                        # If it's adding/dropping foreign keys, update relationships
                        if 'FOREIGN KEY' in query.upper() or 'DROP CONSTRAINT' in query.upper():
                            # Re-analyze relationships for this schema
                            self.schema_info["relationships"] = self._analyze_relationships([schema_name])
        except Exception as e:
            print(f"Error handling ALTER TABLE: {e}")

    def _handle_create_index(self, query: str) -> None:
        """Handle CREATE INDEX query"""
        try:
            import re
            # Extract table name from CREATE INDEX
            match = re.search(r'ON\s+(?:(\w+)\.)?(\w+)', query, re.IGNORECASE)
            if match:
                schema_name = match.group(1) or 'public'
                table_name = match.group(2)
                qualified_name = f"{schema_name}.{table_name}"
                
                # Refresh indexes for this table
                if qualified_name in self.schema_info["tables"] or table_name in self.schema_info["tables"]:
                    new_indexes = self.inspector.get_indexes(table_name, schema=schema_name)
                    self.schema_info["tables"][qualified_name]["indexes"] = new_indexes
                    self.schema_info["tables"][table_name]["indexes"] = new_indexes
                    
                    print(f"Updated indexes for table: {qualified_name}")
        except Exception as e:
            print(f"Error handling CREATE INDEX: {e}")

    def _handle_drop_index(self, query: str) -> None:
        """Handle DROP INDEX query"""
        # Similar to create index but we need to refresh the table's index list
        self._handle_create_index(query)  # Reuse the logic to refresh indexes

    def _handle_truncate_table(self, query: str) -> None:
        """Handle TRUNCATE TABLE query"""
        try:
            import re
            match = re.search(r'TRUNCATE TABLE\s+(?:(\w+)\.)?(\w+)', query, re.IGNORECASE)
            if match:
                schema_name = match.group(1) or 'public'
                table_name = match.group(2)
                qualified_name = f"{schema_name}.{table_name}"
                
                # Update row count to 0 and clear sample data
                if qualified_name in self.schema_info["tables"]:
                    self.schema_info["tables"][qualified_name]["row_count"] = 0
                    self.schema_info["tables"][qualified_name]["sample_data"] = None
                if table_name in self.schema_info["tables"]:
                    self.schema_info["tables"][table_name]["row_count"] = 0
                    self.schema_info["tables"][table_name]["sample_data"] = None
                
                print(f"Updated row count for truncated table: {qualified_name}")
        except Exception as e:
            print(f"Error handling TRUNCATE TABLE: {e}")

    def _remove_table_relationships(self, table_name: str, schema_name: str) -> None:
        """Remove relationships involving a dropped table"""
        if "relationships" not in self.schema_info:
            return
        
        # Filter out relationships involving the dropped table
        self.schema_info["relationships"] = [
            rel for rel in self.schema_info["relationships"]
            if not (
                (rel["source_table"] == table_name and rel["source_schema"] == schema_name) or
                (rel["target_table"] == table_name and rel["target_schema"] == schema_name)
            )
        ]

    def refresh_schema_for_table(self, table_name: str, schema_name: str = "public") -> bool:
        """
        Refresh schema information for a specific table
        
        Args:
            table_name: Name of the table to refresh
            schema_name: Schema name (defaults to 'public')
            
        Returns:
            True if successful, False otherwise
        """
        try:
            with self.engine.connect() as connection:
                table_info = self._get_table_info(table_name, connection, schema_name)
                
                qualified_name = f"{schema_name}.{table_name}"
                self.schema_info["tables"][qualified_name] = table_info
                self.schema_info["tables"][table_name] = table_info
                
                print(f"Refreshed schema for table: {qualified_name}")
                return True
        except Exception as e:
            print(f"Error refreshing schema for table {table_name}: {e}")
            return False


if __name__ == "__main__":
    # Example usage
    from dotenv import load_dotenv
    
    load_dotenv()
    db_name = os.getenv("DB_NAME", "postgres")
    username = os.getenv("DB_USERNAME", "postgres")
    password = os.getenv("DB_PASSWORD", "postgres")
    host = os.getenv("DB_HOST", "localhost")
    port = os.getenv("DB_PORT", "5432")
    
    analyzer = DatabaseAnalyzer(db_name, username, password, host, port)
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
                print(f"- {schema_name}.{row['table_name']}") 