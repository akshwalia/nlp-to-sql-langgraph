from typing import Dict, Any, List


class MultiQueryManager:
    """Manages multi-query analysis for the SQL generator"""
    
    def __init__(self, query_analyzer, sql_generation_manager):
        self.query_analyzer = query_analyzer
        self.sql_generation_manager = sql_generation_manager
    
    async def generate_sql_for_subquery(self, question: str, query_info: Dict[str, Any]) -> Dict[str, Any]:
        """Generate SQL for a subquery in multi-query analysis"""
        try:
            # Use the existing SQL generation functionality
            result = await self.sql_generation_manager.generate_sql(question, query_info.get("db_analyzer"))
            
            # Add query info metadata
            result["query_id"] = query_info.get("id", "unknown")
            result["query_type"] = query_info.get("type", "main")
            result["dependencies"] = query_info.get("dependencies", [])
            
            return result
            
        except Exception as e:
            return {
                "success": False,
                "sql": "",
                "error": f"Error generating subquery SQL: {str(e)}",
                "question": question,
                "query_id": query_info.get("id", "unknown"),
                "query_type": query_info.get("type", "main"),
                "dependencies": query_info.get("dependencies", [])
            }
    
    async def execute_multi_query_analysis(self, question: str, query_plan: Dict[str, Any]) -> Dict[str, Any]:
        """Execute multi-query analysis based on query plan"""
        try:
            queries = query_plan.get("queries", [])
            if not queries:
                return {
                    "success": False,
                    "error": "No queries in plan",
                    "results": [],
                    "question": question
                }
            
            # Execute queries in dependency order
            executed_queries = {}
            all_results = []
            
            for query_info in queries:
                query_id = query_info.get("id", f"query_{len(executed_queries)}")
                dependencies = query_info.get("dependencies", [])
                
                # Check if all dependencies are satisfied
                if not all(dep in executed_queries for dep in dependencies):
                    return {
                        "success": False,
                        "error": f"Dependencies not satisfied for query {query_id}",
                        "results": all_results,
                        "question": question
                    }
                
                # Generate and execute the query
                sub_question = query_info.get("question", question)
                result = await self.generate_sql_for_subquery(sub_question, query_info)
                
                if result["success"]:
                    # Execute the SQL (this would need to be implemented with actual execution)
                    # For now, we'll just store the SQL
                    executed_queries[query_id] = result
                    all_results.append({
                        "query_id": query_id,
                        "question": sub_question,
                        "sql": result["sql"],
                        "type": query_info.get("type", "main"),
                        "results": []  # Would contain actual query results
                    })
                else:
                    return {
                        "success": False,
                        "error": f"Failed to generate SQL for query {query_id}: {result.get('error', 'Unknown error')}",
                        "results": all_results,
                        "question": question
                    }
            
            return {
                "success": True,
                "results": all_results,
                "question": question,
                "total_queries": len(queries),
                "executed_queries": len(executed_queries)
            }
            
        except Exception as e:
            return {
                "success": False,
                "error": f"Error executing multi-query analysis: {str(e)}",
                "results": [],
                "question": question
            }
    
    def plan_queries(self, question: str) -> Dict[str, Any]:
        """Plan multiple queries for complex analysis"""
        try:
            # Use the query analyzer to plan queries
            return self.query_analyzer.plan_queries(question)
            
        except Exception as e:
            return {
                "is_multi_query": False,
                "queries": [
                    {
                        "id": "main_query",
                        "question": question,
                        "type": "main",
                        "dependencies": []
                    }
                ],
                "error": f"Error planning queries: {str(e)}"
            } 