from typing import Dict, List, Any
import logging

logger = logging.getLogger(__name__)


class RelationshipAnalyzer:
    """
    Handles analysis of relationships between database tables
    """
    
    def __init__(self, inspector):
        """
        Initialize the relationship analyzer
        
        Args:
            inspector: SQLAlchemy inspector instance
        """
        self.inspector = inspector
    
    def analyze_relationships(self, schema_names: List[str] = ["public"]) -> List[Dict[str, Any]]:
        """
        Analyze relationships between tables in the database
        
        Args:
            schema_names: List of schema names to analyze
            
        Returns:
            List of relationships
        """
        relationships = []
        
        try:
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
            
            logger.info(f"Found {len(relationships)} relationships")
            
        except Exception as e:
            logger.error(f"Error analyzing relationships: {e}")
        
        return relationships
    
    def get_table_relationships(self, table_name: str, schema_name: str = "public") -> Dict[str, List[Dict[str, Any]]]:
        """
        Get all relationships for a specific table
        
        Args:
            table_name: Name of the table
            schema_name: Schema name (defaults to 'public')
            
        Returns:
            Dictionary with 'outgoing' and 'incoming' relationships
        """
        relationships = {
            "outgoing": [],  # Foreign keys from this table to others
            "incoming": []   # Foreign keys from other tables to this table
        }
        
        try:
            # Get outgoing relationships (foreign keys from this table)
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
                relationships["outgoing"].append(relationship)
            
            # Get incoming relationships (foreign keys from other tables to this table)
            # We need to check all tables in the schema
            for other_table in self.inspector.get_table_names(schema=schema_name):
                if other_table != table_name:
                    for fk in self.inspector.get_foreign_keys(other_table, schema=schema_name):
                        if (fk["referred_table"] == table_name and 
                            fk.get("referred_schema", schema_name) == schema_name):
                            relationship = {
                                "source_schema": schema_name,
                                "source_table": other_table,
                                "source_columns": fk["constrained_columns"],
                                "target_schema": schema_name,
                                "target_table": table_name,
                                "target_columns": fk["referred_columns"],
                                "name": fk.get("name")
                            }
                            relationships["incoming"].append(relationship)
            
            logger.info(f"Found {len(relationships['outgoing'])} outgoing and {len(relationships['incoming'])} incoming relationships for {schema_name}.{table_name}")
            
        except Exception as e:
            logger.error(f"Error getting relationships for {schema_name}.{table_name}: {e}")
        
        return relationships
    
    def remove_table_relationships(self, table_name: str, schema_name: str, relationships: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Remove relationships involving a specific table
        
        Args:
            table_name: Name of the table
            schema_name: Schema name
            relationships: List of current relationships
            
        Returns:
            Updated list of relationships with the table's relationships removed
        """
        # Filter out relationships involving the specified table
        filtered_relationships = [
            rel for rel in relationships
            if not (
                (rel["source_table"] == table_name and rel["source_schema"] == schema_name) or
                (rel["target_table"] == table_name and rel["target_schema"] == schema_name)
            )
        ]
        
        removed_count = len(relationships) - len(filtered_relationships)
        logger.info(f"Removed {removed_count} relationships involving {schema_name}.{table_name}")
        
        return filtered_relationships
    
    def get_relationship_graph(self, schema_names: List[str] = ["public"]) -> Dict[str, Any]:
        """
        Get a graph representation of table relationships
        
        Args:
            schema_names: List of schema names to analyze
            
        Returns:
            Dictionary with nodes (tables) and edges (relationships)
        """
        relationships = self.analyze_relationships(schema_names)
        
        # Build nodes (tables)
        nodes = set()
        for rel in relationships:
            nodes.add(f"{rel['source_schema']}.{rel['source_table']}")
            nodes.add(f"{rel['target_schema']}.{rel['target_table']}")
        
        # Build edges (relationships)
        edges = []
        for rel in relationships:
            source = f"{rel['source_schema']}.{rel['source_table']}"
            target = f"{rel['target_schema']}.{rel['target_table']}"
            edge = {
                "source": source,
                "target": target,
                "source_columns": rel["source_columns"],
                "target_columns": rel["target_columns"],
                "name": rel.get("name")
            }
            edges.append(edge)
        
        return {
            "nodes": list(nodes),
            "edges": edges,
            "total_tables": len(nodes),
            "total_relationships": len(edges)
        }
    
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
        related_tables = set()
        current_tables = {f"{schema_name}.{table_name}"}
        
        for depth in range(max_depth):
            next_tables = set()
            
            for current_table in current_tables:
                schema, table = current_table.split(".", 1)
                table_rels = self.get_table_relationships(table, schema)
                
                # Add outgoing relationships
                for rel in table_rels["outgoing"]:
                    target_table = f"{rel['target_schema']}.{rel['target_table']}"
                    if target_table not in related_tables:
                        related_tables.add(target_table)
                        next_tables.add(target_table)
                
                # Add incoming relationships
                for rel in table_rels["incoming"]:
                    source_table = f"{rel['source_schema']}.{rel['source_table']}"
                    if source_table not in related_tables:
                        related_tables.add(source_table)
                        next_tables.add(source_table)
            
            current_tables = next_tables
            if not current_tables:
                break
        
        # Remove the original table from the results
        related_tables.discard(f"{schema_name}.{table_name}")
        
        return list(related_tables) 