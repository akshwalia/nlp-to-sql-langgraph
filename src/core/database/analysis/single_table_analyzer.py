from typing import Dict, List, Any, Optional
from sqlalchemy import create_engine, inspect, text
import pandas as pd
import logging
import os
from datetime import datetime
import json

logger = logging.getLogger(__name__)


class SingleTableAnalyzer:
    """
    Simplified analyzer focused on analyzing a single table comprehensively
    """
    
    def __init__(
        self,
        db_name: str,
        username: str,
        password: str,
        host: str = "localhost",
        port: str = "5432",
        table_name: str = "IT_Professional_Services",
        schema_name: str = "public",
        output_file: str = "single_table_analysis.txt",
        enum_threshold: int = 50
    ):
        """
        Initialize the single table analyzer
        
        Args:
            db_name: PostgreSQL database name
            username: PostgreSQL username
            password: PostgreSQL password
            host: PostgreSQL host
            port: PostgreSQL port
            table_name: Name of the table to analyze
            schema_name: Schema name (defaults to 'public')
            output_file: Path to the output text file for analysis results
            enum_threshold: Maximum number of unique values to treat as enum (default: 50)
        """
        self.db_name = db_name
        self.username = username
        self.password = password
        self.host = host
        self.port = port
        self.table_name = table_name
        self.schema_name = schema_name
        self.output_file = output_file
        self.enum_threshold = enum_threshold
        
        # Create database connection
        self.connection_string = f"postgresql://{username}:{password}@{host}:{port}/{db_name}"
        self.engine = create_engine(self.connection_string)
        self.inspector = inspect(self.engine)
        
        # Analysis results
        self.table_analysis = None
        self.llm_context = None
        
        logger.info(f"Initialized SingleTableAnalyzer for table: {schema_name}.{table_name}")
    
    def set_table_name(self, table_name: str, schema_name: str = "public"):
        """
        Set a new table name to analyze
        
        Args:
            table_name: Name of the table to analyze
            schema_name: Schema name (defaults to 'public')
        """
        self.table_name = table_name
        self.schema_name = schema_name
        logger.info(f"Table name updated to: {schema_name}.{table_name}")
    
    def analyze_table(self, save_to_file: bool = True) -> Dict[str, Any]:
        """
        Perform comprehensive analysis of the single table
        
        Args:
            save_to_file: Whether to save analysis to file
            
        Returns:
            Dictionary containing comprehensive table analysis
        """
        logger.info(f"Starting comprehensive analysis for table: {self.schema_name}.{self.table_name}")
        
        try:
            # Check if table exists
            if not self._table_exists():
                error_msg = f"Table {self.schema_name}.{self.table_name} does not exist"
                logger.error(error_msg)
                return {"error": error_msg, "success": False}
            
            with self.engine.connect() as connection:
                # Initialize analysis structure
                self.table_analysis = {
                    "table_name": self.table_name,
                    "schema_name": self.schema_name,
                    "analysis_timestamp": datetime.now().isoformat(),
                    "database_info": self._get_database_info(),
                    "table_structure": self._analyze_table_structure(connection),
                    "data_analysis": self._analyze_data_content(connection),
                    "column_statistics": self._analyze_column_statistics(connection),
                    "constraints_and_indexes": self._analyze_constraints_and_indexes(connection),
                    "relationships": self._analyze_relationships(connection),
                    "data_quality": self._analyze_data_quality(connection),
                    "sample_data": self._get_sample_data(connection),
                    "statistics": self._generate_statistics(connection),
                    "recommendations": self._generate_recommendations(),
                    "success": True
                }
                
                logger.info(f"Analysis completed successfully for table: {self.schema_name}.{self.table_name}")
                
                # Generate LLM context
                self.llm_context = self._generate_llm_context()
                
                # Save to file if requested
                if save_to_file:
                    self._save_analysis_to_file()
                
                return self.table_analysis
                
        except Exception as e:
            logger.error(f"Error during table analysis: {e}")
            return {"error": str(e), "success": False}
    
    def _table_exists(self) -> bool:
        """Check if the table exists"""
        try:
            table_names = self.inspector.get_table_names(schema=self.schema_name)
            exists = self.table_name in table_names
            logger.debug(f"Table {self.schema_name}.{self.table_name} exists: {exists}")
            return exists
        except Exception as e:
            logger.error(f"Error checking if table exists: {e}")
            return False
    
    def _get_database_info(self) -> Dict[str, Any]:
        """Get basic database information"""
        logger.debug("Getting database information")
        return {
            "database_name": self.db_name,
            "host": self.host,
            "port": self.port,
            "connection_string": self.connection_string.replace(f":{self.password}@", ":***@")
        }
    
    def _analyze_table_structure(self, connection) -> Dict[str, Any]:
        """Analyze table structure including columns, types, and constraints"""
        logger.info("Analyzing table structure")
        
        structure = {
            "columns": [],
            "column_count": 0,
            "data_types": {},
            "nullable_columns": [],
            "non_nullable_columns": []
        }
        
        try:
            # Get column information
            columns = self.inspector.get_columns(self.table_name, schema=self.schema_name)
            structure["column_count"] = len(columns)
            
            for column in columns:
                column_info = {
                    "name": column["name"],
                    "type": str(column["type"]),
                    "nullable": column.get("nullable", True),
                    "default": str(column.get("default", "")),
                    "autoincrement": column.get("autoincrement", False),
                    "comment": column.get("comment", "")
                }
                
                structure["columns"].append(column_info)
                
                # Track data types
                data_type = str(column["type"])
                structure["data_types"][data_type] = structure["data_types"].get(data_type, 0) + 1
                
                # Track nullable columns
                if column.get("nullable", True):
                    structure["nullable_columns"].append(column["name"])
                else:
                    structure["non_nullable_columns"].append(column["name"])
                
                logger.debug(f"Column: {column_info['name']} ({column_info['type']}) - Nullable: {column_info['nullable']}")
            
            logger.info(f"Table structure analysis completed: {len(columns)} columns")
            
        except Exception as e:
            logger.error(f"Error analyzing table structure: {e}")
            structure["error"] = str(e)
        
        return structure
    
    def _analyze_data_content(self, connection) -> Dict[str, Any]:
        """Analyze data content including row count, distribution, etc."""
        logger.info("Analyzing data content")
        
        data_analysis = {
            "row_count": 0,
            "table_size": "Unknown",
            "column_statistics": {}
        }
        
        try:
            # Get row count
            result = connection.execute(text(f'SELECT COUNT(*) FROM "{self.schema_name}"."{self.table_name}"'))
            data_analysis["row_count"] = result.scalar()
            logger.info(f"Table has {data_analysis['row_count']} rows")
            
            # Get table size
            try:
                result = connection.execute(text(f"""
                    SELECT pg_size_pretty(pg_total_relation_size('"{self.schema_name}"."{self.table_name}"'))
                """))
                data_analysis["table_size"] = result.scalar()
                logger.debug(f"Table size: {data_analysis['table_size']}")
            except Exception as e:
                logger.warning(f"Could not get table size: {e}")
            
            # Analyze column statistics if table has data
            if data_analysis["row_count"] > 0:
                data_analysis["column_statistics"] = self._analyze_column_statistics(connection)
            
        except Exception as e:
            logger.error(f"Error analyzing data content: {e}")
            data_analysis["error"] = str(e)
        
        return data_analysis
    
    def _analyze_column_statistics(self, connection) -> Dict[str, Any]:
        """Analyze statistics for each column"""
        logger.info("Analyzing column statistics")
        
        column_stats = {}
        columns = self.inspector.get_columns(self.table_name, schema=self.schema_name)
        
        for column in columns:
            col_name = column["name"]
            col_type = str(column["type"]).lower()
            
            logger.debug(f"Analyzing statistics for column: {col_name}")
            
            stats = {
                "null_count": 0,
                "null_percentage": 0.0,
                "distinct_count": 0,
                "most_common_values": [],
                "is_enum_like": False,
                "unique_values": []
            }
            
            try:
                # Get null count and percentage
                result = connection.execute(text(f"""
                    SELECT 
                        COUNT(*) - COUNT("{col_name}") as null_count,
                        ROUND(((COUNT(*) - COUNT("{col_name}")) * 100.0 / COUNT(*)), 2) as null_percentage
                    FROM "{self.schema_name}"."{self.table_name}"
                """))
                row = result.first()
                if row:
                    stats["null_count"] = row[0]
                    stats["null_percentage"] = row[1]
                
                # Get distinct count
                result = connection.execute(text(f"""
                    SELECT COUNT(DISTINCT "{col_name}") as distinct_count
                    FROM "{self.schema_name}"."{self.table_name}"
                """))
                stats["distinct_count"] = result.scalar()
                
                # Check if column should be treated as enum-like (low cardinality)
                if (stats["distinct_count"] > 0 and 
                    stats["distinct_count"] <= self.enum_threshold):
                    
                    stats["is_enum_like"] = True
                    logger.debug(f"Column {col_name} detected as enum-like with {stats['distinct_count']} unique values")
                    
                    # Get ALL unique values for enum-like columns
                    try:
                        result = connection.execute(text(f"""
                            SELECT DISTINCT "{col_name}"
                            FROM "{self.schema_name}"."{self.table_name}"
                            WHERE "{col_name}" IS NOT NULL
                            ORDER BY "{col_name}"
                        """))
                        stats["unique_values"] = [str(row[0]) for row in result]
                        logger.debug(f"Stored {len(stats['unique_values'])} unique values for {col_name}")
                    except Exception as e:
                        logger.warning(f"Could not get unique values for {col_name}: {e}")
                        stats["is_enum_like"] = False
                
                # For numeric columns, get additional statistics
                if any(t in col_type for t in ["int", "float", "numeric", "decimal", "double"]):
                    try:
                        result = connection.execute(text(f"""
                            SELECT 
                                MIN("{col_name}") as min_val,
                                MAX("{col_name}") as max_val,
                                AVG("{col_name}") as avg_val,
                                STDDEV("{col_name}") as stddev_val
                            FROM "{self.schema_name}"."{self.table_name}"
                            WHERE "{col_name}" IS NOT NULL
                        """))
                        row = result.first()
                        if row:
                            stats.update({
                                "min_value": row[0],
                                "max_value": row[1],
                                "average": row[2],
                                "standard_deviation": row[3]
                            })
                    except Exception as e:
                        logger.warning(f"Could not get numeric statistics for {col_name}: {e}")
                
                # Get most common values (limit to top 5) for non-enum columns
                if (not stats["is_enum_like"] and 
                    stats["distinct_count"] > 0 and 
                    stats["distinct_count"] < 1000):
                    try:
                        result = connection.execute(text(f"""
                            SELECT "{col_name}", COUNT(*) as frequency
                            FROM "{self.schema_name}"."{self.table_name}"
                            WHERE "{col_name}" IS NOT NULL
                            GROUP BY "{col_name}"
                            ORDER BY frequency DESC
                            LIMIT 5
                        """))
                        stats["most_common_values"] = [
                            {"value": str(row[0]), "frequency": row[1]}
                            for row in result
                        ]
                    except Exception as e:
                        logger.warning(f"Could not get most common values for {col_name}: {e}")
                
                column_stats[col_name] = stats
                logger.debug(f"Statistics completed for column: {col_name}")
                
            except Exception as e:
                logger.error(f"Error analyzing statistics for column {col_name}: {e}")
                column_stats[col_name] = {"error": str(e)}
        
        return column_stats
    
    def _analyze_constraints_and_indexes(self, connection) -> Dict[str, Any]:
        """Analyze constraints and indexes"""
        logger.info("Analyzing constraints and indexes")
        
        constraints_indexes = {
            "primary_key": [],
            "foreign_keys": [],
            "unique_constraints": [],
            "check_constraints": [],
            "indexes": []
        }
        
        try:
            # Primary key
            pk_constraint = self.inspector.get_pk_constraint(self.table_name, schema=self.schema_name)
            if pk_constraint and pk_constraint.get('constrained_columns'):
                constraints_indexes["primary_key"] = pk_constraint['constrained_columns']
                logger.debug(f"Primary key: {constraints_indexes['primary_key']}")
            
            # Foreign keys
            foreign_keys = self.inspector.get_foreign_keys(self.table_name, schema=self.schema_name)
            for fk in foreign_keys:
                fk_info = {
                    "constrained_columns": fk["constrained_columns"],
                    "referred_table": fk["referred_table"],
                    "referred_schema": fk.get("referred_schema", self.schema_name),
                    "referred_columns": fk["referred_columns"],
                    "name": fk.get("name", "")
                }
                constraints_indexes["foreign_keys"].append(fk_info)
                logger.debug(f"Foreign key: {fk_info}")
            
            # Unique constraints
            unique_constraints = self.inspector.get_unique_constraints(self.table_name, schema=self.schema_name)
            for uc in unique_constraints:
                constraints_indexes["unique_constraints"].append({
                    "name": uc.get("name", ""),
                    "columns": uc["column_names"]
                })
            
            # Check constraints
            check_constraints = self.inspector.get_check_constraints(self.table_name, schema=self.schema_name)
            for cc in check_constraints:
                constraints_indexes["check_constraints"].append({
                    "name": cc.get("name", ""),
                    "definition": cc.get("sqltext", "")
                })
            
            # Indexes
            indexes = self.inspector.get_indexes(self.table_name, schema=self.schema_name)
            for idx in indexes:
                constraints_indexes["indexes"].append({
                    "name": idx["name"],
                    "columns": idx["column_names"],
                    "unique": idx.get("unique", False)
                })
                logger.debug(f"Index: {idx['name']} on {idx['column_names']}")
            
        except Exception as e:
            logger.error(f"Error analyzing constraints and indexes: {e}")
            constraints_indexes["error"] = str(e)
        
        return constraints_indexes
    
    def _analyze_relationships(self, connection) -> Dict[str, Any]:
        """Analyze relationships with other tables"""
        logger.info("Analyzing table relationships")
        
        relationships = {
            "outgoing_references": [],  # Tables this table references
            "incoming_references": [],  # Tables that reference this table
            "related_tables": []
        }
        
        try:
            # Get outgoing references (foreign keys from this table)
            foreign_keys = self.inspector.get_foreign_keys(self.table_name, schema=self.schema_name)
            for fk in foreign_keys:
                relationships["outgoing_references"].append({
                    "to_table": f"{fk.get('referred_schema', self.schema_name)}.{fk['referred_table']}",
                    "from_columns": fk["constrained_columns"],
                    "to_columns": fk["referred_columns"]
                })
            
            # Get incoming references (other tables that reference this table)
            # This requires checking all tables in the schema
            all_tables = self.inspector.get_table_names(schema=self.schema_name)
            for table in all_tables:
                if table != self.table_name:
                    try:
                        table_fks = self.inspector.get_foreign_keys(table, schema=self.schema_name)
                        for fk in table_fks:
                            if fk["referred_table"] == self.table_name:
                                relationships["incoming_references"].append({
                                    "from_table": f"{self.schema_name}.{table}",
                                    "from_columns": fk["constrained_columns"],
                                    "to_columns": fk["referred_columns"]
                                })
                    except Exception as e:
                        logger.warning(f"Could not check foreign keys for table {table}: {e}")
            
            # Compile related tables
            related_tables = set()
            for ref in relationships["outgoing_references"]:
                related_tables.add(ref["to_table"])
            for ref in relationships["incoming_references"]:
                related_tables.add(ref["from_table"])
            
            relationships["related_tables"] = list(related_tables)
            logger.debug(f"Related tables: {relationships['related_tables']}")
            
        except Exception as e:
            logger.error(f"Error analyzing relationships: {e}")
            relationships["error"] = str(e)
        
        return relationships
    
    def _analyze_data_quality(self, connection) -> Dict[str, Any]:
        """Analyze data quality issues"""
        logger.info("Analyzing data quality")
        
        quality_analysis = {
            "completeness": {},
            "consistency": {},
            "potential_issues": []
        }
        
        try:
            columns = self.inspector.get_columns(self.table_name, schema=self.schema_name)
            
            # Check completeness (null values)
            for column in columns:
                col_name = column["name"]
                try:
                    result = connection.execute(text(f"""
                        SELECT 
                            COUNT(*) as total_count,
                            COUNT("{col_name}") as non_null_count,
                            ROUND(((COUNT("{col_name}") * 100.0) / COUNT(*)), 2) as completeness_percentage
                        FROM "{self.schema_name}"."{self.table_name}"
                    """))
                    row = result.first()
                    if row:
                        quality_analysis["completeness"][col_name] = {
                            "total_count": row[0],
                            "non_null_count": row[1],
                            "completeness_percentage": row[2]
                        }
                        
                        # Flag potential issues
                        if row[2] < 50:  # Less than 50% complete
                            quality_analysis["potential_issues"].append({
                                "type": "low_completeness",
                                "column": col_name,
                                "description": f"Column {col_name} is only {row[2]}% complete"
                            })
                except Exception as e:
                    logger.warning(f"Could not analyze completeness for {col_name}: {e}")
            
            # Check for potential duplicate rows
            try:
                # First get all column names for the table
                columns_result = connection.execute(text(f"""
                    SELECT column_name 
                    FROM information_schema.columns 
                    WHERE table_name = '{self.table_name}' 
                    AND table_schema = '{self.schema_name}'
                    ORDER BY ordinal_position
                """))
                columns = [row[0] for row in columns_result.fetchall()]
                
                if columns:
                    # Create a query to check for duplicates by comparing total rows vs distinct rows
                    # We'll use a hash of all columns to detect duplicates
                    columns_concat = ", ".join([f'COALESCE(CAST("{col}" AS TEXT), \'\')' for col in columns])
                    
                    result = connection.execute(text(f"""
                        SELECT 
                            COUNT(*) as total_rows,
                            COUNT(DISTINCT MD5(CONCAT({columns_concat}))) as distinct_rows
                        FROM "{self.schema_name}"."{self.table_name}"
                    """))
                    row = result.first()
                    if row and row[0] != row[1]:
                        quality_analysis["potential_issues"].append({
                            "type": "duplicate_rows",
                            "description": f"Table has {row[0]} total rows but only {row[1]} distinct rows"
                        })
                else:
                    logger.warning("No columns found for duplicate row check")
                    
            except Exception as e:
                logger.warning(f"Could not check for duplicate rows: {e}")
                # Fallback: simpler approach without MD5 hash
                try:
                    result = connection.execute(text(f"""
                        WITH duplicate_check AS (
                            SELECT *, ROW_NUMBER() OVER (PARTITION BY * ORDER BY (SELECT NULL)) as rn
                            FROM "{self.schema_name}"."{self.table_name}"
                        )
                        SELECT 
                            (SELECT COUNT(*) FROM "{self.schema_name}"."{self.table_name}") as total_rows,
                            COUNT(*) as distinct_rows
                        FROM duplicate_check
                        WHERE rn = 1
                    """))
                    row = result.first()
                    if row and row[0] != row[1]:
                        quality_analysis["potential_issues"].append({
                            "type": "duplicate_rows",
                            "description": f"Table has {row[0]} total rows but only {row[1]} distinct rows"
                        })
                except Exception as e2:
                    logger.warning(f"Fallback duplicate check also failed: {e2}")
            
        except Exception as e:
            logger.error(f"Error analyzing data quality: {e}")
            quality_analysis["error"] = str(e)
        
        return quality_analysis
    
    def _get_sample_data(self, connection, limit: int = 10) -> Dict[str, Any]:
        """Get sample data from the table"""
        logger.info(f"Getting sample data (limit: {limit})")
        
        sample_data = {
            "rows": [],
            "count": 0
        }
        
        try:
            result = connection.execute(text(f'SELECT * FROM "{self.schema_name}"."{self.table_name}" LIMIT {limit}'))
            
            # Convert rows to dictionaries
            for row in result:
                row_dict = {}
                for idx, column in enumerate(result.keys()):
                    value = row[idx]
                    # Convert non-serializable types to strings
                    if isinstance(value, (pd.Timestamp, pd.Timedelta)):
                        value = str(value)
                    row_dict[column] = value
                sample_data["rows"].append(row_dict)
            
            sample_data["count"] = len(sample_data["rows"])
            logger.debug(f"Retrieved {sample_data['count']} sample rows")
            
        except Exception as e:
            logger.error(f"Error getting sample data: {e}")
            sample_data["error"] = str(e)
        
        return sample_data
    
    def _generate_statistics(self, connection) -> Dict[str, Any]:
        """Generate overall table statistics"""
        logger.info("Generating table statistics")
        
        stats = {
            "generated_at": datetime.now().isoformat(),
            "summary": {}
        }
        
        try:
            if self.table_analysis:
                structure = self.table_analysis.get("table_structure", {})
                data_analysis = self.table_analysis.get("data_analysis", {})
                constraints = self.table_analysis.get("constraints_and_indexes", {})
                
                stats["summary"] = {
                    "total_columns": structure.get("column_count", 0),
                    "total_rows": data_analysis.get("row_count", 0),
                    "table_size": data_analysis.get("table_size", "Unknown"),
                    "nullable_columns": len(structure.get("nullable_columns", [])),
                    "non_nullable_columns": len(structure.get("non_nullable_columns", [])),
                    "primary_key_columns": len(constraints.get("primary_key", [])),
                    "foreign_key_count": len(constraints.get("foreign_keys", [])),
                    "index_count": len(constraints.get("indexes", [])),
                    "data_types_count": len(structure.get("data_types", {}))
                }
        
        except Exception as e:
            logger.error(f"Error generating statistics: {e}")
            stats["error"] = str(e)
        
        return stats
    
    def _generate_recommendations(self) -> List[Dict[str, str]]:
        """Generate recommendations based on analysis"""
        logger.info("Generating recommendations")
        
        recommendations = []
        
        try:
            if self.table_analysis:
                structure = self.table_analysis.get("table_structure", {})
                data_analysis = self.table_analysis.get("data_analysis", {})
                constraints = self.table_analysis.get("constraints_and_indexes", {})
                quality = self.table_analysis.get("data_quality", {})
                
                # Check for missing primary key
                if not constraints.get("primary_key"):
                    recommendations.append({
                        "type": "structure",
                        "priority": "high",
                        "recommendation": "Consider adding a primary key to improve data integrity and query performance"
                    })
                
                # Check for tables with no indexes
                if not constraints.get("indexes"):
                    recommendations.append({
                        "type": "performance",
                        "priority": "medium",
                        "recommendation": "Consider adding indexes on frequently queried columns to improve performance"
                    })
                
                # Check for data quality issues
                potential_issues = quality.get("potential_issues", [])
                for issue in potential_issues:
                    recommendations.append({
                        "type": "data_quality",
                        "priority": "medium",
                        "recommendation": f"Address {issue['type']}: {issue['description']}"
                    })
                
                # Check for large tables without partitioning
                row_count = data_analysis.get("row_count", 0)
                if row_count > 1000000:
                    recommendations.append({
                        "type": "scalability",
                        "priority": "medium",
                        "recommendation": f"Table has {row_count} rows. Consider partitioning for better performance"
                    })
        
        except Exception as e:
            logger.error(f"Error generating recommendations: {e}")
        
        return recommendations
    
    def _generate_llm_context(self) -> str:
        """Generate formatted context for LLM consumption"""
        logger.info("Generating LLM context")
        
        if not self.table_analysis:
            return "No table analysis available"
        
        context_parts = []
        
        # Header
        context_parts.append(f"DATABASE TABLE ANALYSIS: {self.schema_name}.{self.table_name}")
        context_parts.append("=" * 80)
        context_parts.append("")
        
        # Basic Information
        context_parts.append("BASIC INFORMATION:")
        context_parts.append(f"- Database: {self.db_name}")
        context_parts.append(f"- Table: {self.schema_name}.{self.table_name}")
        context_parts.append(f"- Analysis Date: {self.table_analysis['analysis_timestamp']}")
        context_parts.append("")
        
        # Table Structure
        structure = self.table_analysis.get("table_structure", {})
        context_parts.append(f"TABLE STRUCTURE:")
        context_parts.append(f"- Total Columns: {structure.get('column_count', 0)}")
        context_parts.append(f"- Data Types: {list(structure.get('data_types', {}).keys())}")
        context_parts.append("")
        
        context_parts.append("COLUMNS:")
        for column in structure.get("columns", []):
            context_parts.append(f"  - {column['name']}: {column['type']} (Nullable: {column['nullable']})")
        context_parts.append("")
        
        # Column Statistics with Enum Values
        column_stats = self.table_analysis.get("column_statistics", {})
        enum_columns = []  # Initialize here to avoid scope issues
        
        if column_stats:
            context_parts.append("COLUMN STATISTICS:")
            
            # Find enum-like columns
            for col_name, stats in column_stats.items():
                if isinstance(stats, dict) and stats.get("is_enum_like", False):
                    enum_columns.append((col_name, stats))
            
            # Display enum-like columns first
            if enum_columns:
                context_parts.append("\nENUM-LIKE COLUMNS (Low Cardinality):")
                for col_name, stats in enum_columns:
                    unique_values = stats.get("unique_values", [])
                    context_parts.append(f"  - {col_name}: {len(unique_values)} unique values")
                    if unique_values:
                        # Format values nicely
                        if len(unique_values) <= 10:
                            values_str = ", ".join([f"'{val}'" for val in unique_values])
                        else:
                            values_str = ", ".join([f"'{val}'" for val in unique_values[:8]]) + f", ... (+{len(unique_values) - 8} more)"
                        context_parts.append(f"    Values: {values_str}")
                context_parts.append("")
            
            # Display other column statistics
            context_parts.append("OTHER COLUMN STATISTICS:")
            for col_name, stats in column_stats.items():
                if isinstance(stats, dict) and not stats.get("is_enum_like", False):
                    distinct_count = stats.get("distinct_count", 0)
                    null_pct = stats.get("null_percentage", 0)
                    context_parts.append(f"  - {col_name}: {distinct_count} distinct values, {null_pct}% null")
                    
                    # Add numeric stats if available
                    if "min_value" in stats:
                        min_val = stats.get("min_value")
                        max_val = stats.get("max_value")
                        avg_val = stats.get("average")
                        context_parts.append(f"    Range: {min_val} to {max_val}, Average: {avg_val}")
            context_parts.append("")
        
        # Data Analysis
        data_analysis = self.table_analysis.get("data_analysis", {})
        context_parts.append("DATA ANALYSIS:")
        context_parts.append(f"- Total Rows: {data_analysis.get('row_count', 0)}")
        context_parts.append(f"- Table Size: {data_analysis.get('table_size', 'Unknown')}")
        context_parts.append("")
        
        # Constraints and Indexes
        constraints = self.table_analysis.get("constraints_and_indexes", {})
        context_parts.append("CONSTRAINTS AND INDEXES:")
        context_parts.append(f"- Primary Key: {constraints.get('primary_key', 'None')}")
        context_parts.append(f"- Foreign Keys: {len(constraints.get('foreign_keys', []))}")
        context_parts.append(f"- Indexes: {len(constraints.get('indexes', []))}")
        context_parts.append("")
        
        # Relationships
        relationships = self.table_analysis.get("relationships", {})
        context_parts.append("RELATIONSHIPS:")
        context_parts.append(f"- Related Tables: {relationships.get('related_tables', [])}")
        context_parts.append(f"- Outgoing References: {len(relationships.get('outgoing_references', []))}")
        context_parts.append(f"- Incoming References: {len(relationships.get('incoming_references', []))}")
        context_parts.append("")
        
        # Sample Data
        sample_data = self.table_analysis.get("sample_data", {})
        if sample_data.get("rows"):
            context_parts.append("SAMPLE DATA:")
            for i, row in enumerate(sample_data["rows"][:3], 1):  # Show first 3 rows
                context_parts.append(f"  Row {i}: {row}")
            context_parts.append("")
        
        # Query Guidance
        if enum_columns:
            context_parts.append("QUERY GUIDANCE:")
            context_parts.append("When filtering by enum-like columns, use the exact values listed above.")
            context_parts.append("These columns have limited possible values, so queries should use these specific values.")
            context_parts.append("")
        
        # Recommendations
        recommendations = self.table_analysis.get("recommendations", [])
        if recommendations:
            context_parts.append("RECOMMENDATIONS:")
            for rec in recommendations:
                context_parts.append(f"  - [{rec['priority'].upper()}] {rec['recommendation']}")
            context_parts.append("")
        
        return "\n".join(context_parts)
    
    def _save_analysis_to_file(self):
        """Save analysis to file"""
        logger.info(f"Saving analysis to file: {self.output_file}")
        
        try:
            # Create directory if it doesn't exist
            os.makedirs(os.path.dirname(self.output_file) if os.path.dirname(self.output_file) else '.', exist_ok=True)
            
            with open(self.output_file, 'w', encoding='utf-8') as f:
                # Write LLM context
                f.write("LLM CONTEXT:\n")
                f.write("=" * 80 + "\n")
                f.write(self.llm_context + "\n\n")
                
                # Write detailed analysis as JSON
                f.write("DETAILED ANALYSIS (JSON):\n")
                f.write("=" * 80 + "\n")
                f.write(json.dumps(self.table_analysis, indent=2, default=str))
            
            logger.info(f"Analysis saved to: {self.output_file}")
            
        except Exception as e:
            logger.error(f"Error saving analysis to file: {e}")
    
    def get_llm_context(self) -> str:
        """Get the formatted LLM context"""
        if not self.llm_context:
            if not self.table_analysis:
                self.analyze_table()
            self.llm_context = self._generate_llm_context()
        
        return self.llm_context
    
    def get_analysis_summary(self) -> Dict[str, Any]:
        """Get a summary of the analysis"""
        if not self.table_analysis:
            return {"error": "No analysis performed yet"}
        
        return {
            "table_name": f"{self.schema_name}.{self.table_name}",
            "success": self.table_analysis.get("success", False),
            "statistics": self.table_analysis.get("statistics", {}),
            "recommendations_count": len(self.table_analysis.get("recommendations", [])),
            "has_primary_key": bool(self.table_analysis.get("constraints_and_indexes", {}).get("primary_key")),
            "foreign_keys_count": len(self.table_analysis.get("constraints_and_indexes", {}).get("foreign_keys", [])),
            "related_tables_count": len(self.table_analysis.get("relationships", {}).get("related_tables", []))
        }


# Example usage
if __name__ == "__main__":
    from dotenv import load_dotenv
    import os
    
    load_dotenv()
    
    # Database connection parameters
    db_name = os.getenv("DB_NAME", "postgres")
    username = os.getenv("DB_USERNAME", "postgres")
    password = os.getenv("DB_PASSWORD", "postgres")
    host = os.getenv("DB_HOST", "localhost")
    port = os.getenv("DB_PORT", "5432")
    
    # Initialize analyzer
    analyzer = SingleTableAnalyzer(
        db_name=db_name,
        username=username,
        password=password,
        host=host,
        port=port,
        table_name="IT_Professional_Services",
        schema_name="public",
        output_file="single_table_analysis.txt"
    )
    
    # Perform analysis
    result = analyzer.analyze_table()
    
    if result.get("success"):
        print("Analysis completed successfully!")
        print(f"Analysis saved to: {analyzer.output_file}")
        print("\nSummary:")
        print(analyzer.get_analysis_summary())
        print("\nLLM Context:")
        print(analyzer.get_llm_context())
    else:
        print(f"Analysis failed: {result.get('error')}") 