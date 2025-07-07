from typing import Dict, List, Any
from sqlalchemy import text
import logging

logger = logging.getLogger(__name__)


class TableAnalyzer:
    """
    Handles table-level analysis including columns, constraints, and sample data
    """
    
    def __init__(self, engine, inspector):
        """
        Initialize the table analyzer
        
        Args:
            engine: SQLAlchemy engine instance
            inspector: SQLAlchemy inspector instance
        """
        self.engine = engine
        self.inspector = inspector
    
    def get_table_info(self, table_name: str, connection, schema_name: str = "public") -> Dict[str, Any]:
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
            logger.error(f"Error getting row count for {schema_name}.{table_name}: {e}")
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
            logger.error(f"Error getting sample data for {schema_name}.{table_name}: {e}")
        
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
                logger.error(f"Error analyzing statistics for {schema_name}.{table_name}.{col_name}: {e}")
    
    def refresh_table_info(self, table_name: str, schema_name: str = "public") -> Dict[str, Any]:
        """
        Refresh information for a specific table
        
        Args:
            table_name: Name of the table to refresh
            schema_name: Schema name (defaults to 'public')
            
        Returns:
            Updated table information dictionary
        """
        try:
            with self.engine.connect() as connection:
                table_info = self.get_table_info(table_name, connection, schema_name)
                logger.info(f"Refreshed table info for: {schema_name}.{table_name}")
                return table_info
        except Exception as e:
            logger.error(f"Error refreshing table info for {table_name}: {e}")
            return {}
    
    def get_table_sample_data(self, table_name: str, schema_name: str = "public", limit: int = 5) -> List[Dict[str, Any]]:
        """
        Get sample data from a table
        
        Args:
            table_name: Name of the table
            schema_name: Schema name (defaults to 'public')
            limit: Number of rows to return
            
        Returns:
            List of sample rows as dictionaries
        """
        try:
            with self.engine.connect() as connection:
                result = connection.execute(text(f"SELECT * FROM \"{schema_name}\".\"{table_name}\" LIMIT {limit}"))
                rows = []
                for row in result:
                    # Convert row to dictionary properly
                    row_dict = {}
                    for idx, column in enumerate(result.keys()):
                        row_dict[column] = row[idx]
                    rows.append(row_dict)
                return rows
        except Exception as e:
            logger.error(f"Error getting sample data for {schema_name}.{table_name}: {e}")
            return []
    
    def get_table_count(self, table_name: str, schema_name: str = "public") -> int:
        """
        Get the row count for a table
        
        Args:
            table_name: Name of the table
            schema_name: Schema name (defaults to 'public')
            
        Returns:
            Number of rows in the table
        """
        try:
            with self.engine.connect() as connection:
                result = connection.execute(text(f"SELECT COUNT(*) FROM \"{schema_name}\".\"{table_name}\""))
                return result.scalar()
        except Exception as e:
            logger.error(f"Error getting row count for {schema_name}.{table_name}: {e}")
            return 0 