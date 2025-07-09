import time
from typing import Dict, Any, List
from src.observability.langfuse_config import observe_function


class ExecutionManager:
    """Manages SQL execution for the SQL generator"""
    
    def __init__(self, db_analyzer, session_context_manager):
        self.db_analyzer = db_analyzer
        self.session_context_manager = session_context_manager
    
    @observe_function("sql_query_execution")
    async def execute_query(self, question: str, sql: str, auto_fix: bool = True, max_attempts: int = 2) -> Dict[str, Any]:
        """Execute SQL query with error handling and auto-fix"""
        start_time = time.time()
        
        try:
            # Execute the query
            result = self._execute_single_query(sql, start_time)
            
            if result["success"]:
                # Update session context with successful execution
                self.session_context_manager.update_session_context(
                    question, sql, result["results"]
                )
                
                return {
                    "success": True,
                    "sql": sql,
                    "results": result["results"],
                    "execution_time": result["execution_time"],
                    "question": question,
                    "row_count": len(result["results"]) if result["results"] else 0
                }
            else:
                # Query failed
                return {
                    "success": False,
                    "sql": sql,
                    "results": [],
                    "error": result["error"],
                    "execution_time": result["execution_time"],
                    "question": question,
                    "row_count": 0
                }
                
        except Exception as e:
            execution_time = time.time() - start_time
            return {
                "success": False,
                "sql": sql,
                "results": [],
                "error": f"Execution error: {str(e)}",
                "execution_time": execution_time,
                "question": question,
                "row_count": 0
            }
    
    def _execute_single_query(self, sql: str, start_time: float) -> Dict[str, Any]:
        """Execute a single SQL query"""
        try:
            # Execute query using the database analyzer
            # DatabaseAnalyzer.execute_query returns (success, results, error)
            success, results, error = self.db_analyzer.execute_query(sql)
            
            execution_time = time.time() - start_time
            
            if success:
                # Handle different result types
                if isinstance(results, list):
                    return {
                        "success": True,
                        "results": results,
                        "execution_time": execution_time,
                        "row_count": len(results)
                    }
                elif isinstance(results, dict):
                    # Handle single result
                    return {
                        "success": True,
                        "results": [results],
                        "execution_time": execution_time,
                        "row_count": 1
                    }
                else:
                    # Handle other result types
                    return {
                        "success": True,
                        "results": [{"result": results}] if results is not None else [],
                        "execution_time": execution_time,
                        "row_count": 1 if results is not None else 0
                    }
            else:
                # Query failed
                return {
                    "success": False,
                    "results": [],
                    "error": error or "Query execution failed",
                    "execution_time": execution_time,
                    "row_count": 0
                }
                
        except Exception as e:
            execution_time = time.time() - start_time
            return {
                "success": False,
                "results": [],
                "error": str(e),
                "execution_time": execution_time,
                "row_count": 0
            }
    
    def _execute_multiple_queries_with_transaction(self, sql_statements: List[str], start_time: float) -> Dict[str, Any]:
        """Execute multiple SQL statements in a transaction"""
        try:
            # This would be implemented with actual transaction handling
            # For now, execute each statement individually
            all_results = []
            
            for sql in sql_statements:
                result = self._execute_single_query(sql, start_time)
                if not result["success"]:
                    # If any query fails, return the error
                    return result
                all_results.extend(result["results"])
            
            execution_time = time.time() - start_time
            
            return {
                "success": True,
                "results": all_results,
                "execution_time": execution_time,
                "row_count": len(all_results)
            }
            
        except Exception as e:
            execution_time = time.time() - start_time
            return {
                "success": False,
                "results": [],
                "error": f"Transaction error: {str(e)}",
                "execution_time": execution_time,
                "row_count": 0
            }
    
    def execute_edit_query(self, sql: str) -> Dict[str, Any]:
        """Execute an edit query (INSERT, UPDATE, DELETE)"""
        start_time = time.time()
        
        try:
            # Check if this is a multi-statement query
            if "<----->" in sql:
                sql_statements = [stmt.strip() for stmt in sql.split("<----->") if stmt.strip()]
                return self._execute_multiple_queries_with_transaction(sql_statements, start_time)
            else:
                return self._execute_single_query(sql, start_time)
                
        except Exception as e:
            execution_time = time.time() - start_time
            return {
                "success": False,
                "results": [],
                "error": f"Edit query execution error: {str(e)}",
                "execution_time": execution_time,
                "row_count": 0
            }
    
    def execute_edit_query_with_schema_update(self, sql: str) -> Dict[str, Any]:
        """Execute edit query and update schema if needed"""
        try:
            # Execute the query
            result = self.execute_edit_query(sql)
            
            if result["success"]:
                # Check if schema needs to be updated
                if self._query_affects_schema(sql):
                    self.refresh_schema_context()
            
            return result
            
        except Exception as e:
            return {
                "success": False,
                "results": [],
                "error": f"Error executing edit query with schema update: {str(e)}",
                "execution_time": 0,
                "row_count": 0
            }
    
    def _query_affects_schema(self, sql: str) -> bool:
        """Check if query affects database schema"""
        sql_upper = sql.upper()
        schema_affecting_keywords = [
            'CREATE TABLE', 'DROP TABLE', 'ALTER TABLE',
            'CREATE INDEX', 'DROP INDEX',
            'CREATE VIEW', 'DROP VIEW',
            'CREATE SCHEMA', 'DROP SCHEMA'
        ]
        
        return any(keyword in sql_upper for keyword in schema_affecting_keywords)
    
    def refresh_schema_context(self) -> bool:
        """Refresh schema context after schema changes"""
        try:
            # This would trigger a schema refresh in the database analyzer
            self.db_analyzer.refresh_schema()
            return True
        except Exception as e:
            print(f"Error refreshing schema context: {e}")
            return False
    
    def check_and_refresh_schema_if_needed(self, executed_sql: str) -> bool:
        """Check if schema refresh is needed after query execution"""
        try:
            if self._query_affects_schema(executed_sql):
                return self.refresh_schema_context()
            return True
        except Exception as e:
            print(f"Error checking schema refresh: {e}")
            return False 