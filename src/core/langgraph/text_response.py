import json
import re
from typing import Dict, Any, List
from decimal import Decimal
from datetime import datetime, date
from src.observability.langfuse_config import observe_function


class TextResponseManager:
    """Manages text response generation for the SQL generator"""
    
    def __init__(self, prompts_manager, memory_manager, llm):
        self.prompts_manager = prompts_manager
        self.memory_manager = memory_manager
        self.llm = llm
    
    @observe_function("text_response_generation")
    def generate_text_response(self, question: str, sql: str = None, results: Any = None) -> Dict[str, Any]:
        """Generate natural language response from SQL results"""
        try:
            # Prepare the results for display
            formatted_results = self._format_results_for_display(results)
            
            # Get memory context with enhanced error handling
            memory_context = ""
            
            if self.memory_manager.use_memory:
                try:
                    memory_context = self.memory_manager.get_memory_context(question)
                    
                    # Handle potential tuple/list responses from memory
                    if isinstance(memory_context, (tuple, list)):
                        if len(memory_context) > 0:
                            memory_context = str(memory_context[0])
                        else:
                            memory_context = ""
                except Exception as memory_error:
                    memory_context = ""
            
            # Prepare prompt values
            prompt_values = {
                "question": question,
                "sql": sql or "No SQL provided",
                "results": formatted_results,
                "schema": "Database schema context"
            }
            
            if self.memory_manager.use_memory:
                prompt_values["memory"] = memory_context
            
            # Generate response with enhanced error handling
            try:
                formatted_messages = self.prompts_manager.text_response_prompt.format_messages(**prompt_values)
                response = self.llm.invoke(formatted_messages)
                
                text_response = self._extract_response_content(response)
                
                # Validate response
                if not text_response or not text_response.strip():
                    text_response = self._create_fallback_response(question, sql, results)
                
                # Store in memory
                if self.memory_manager.use_memory:
                    try:
                        self.memory_manager.store_text_in_memory(question, text_response, sql, results)
                    except Exception as store_error:
                        pass  # Continue even if memory storage fails
                
                final_result = {
                    "success": True,
                    "response": text_response,
                    "question": question,
                    "sql": sql,
                    "results_count": len(results) if isinstance(results, list) else 0
                }
                return final_result
                
            except Exception as llm_error:
                # Return a fallback response
                fallback_response = self._create_fallback_response(question, sql, results)
                return {
                    "success": True,
                    "response": fallback_response,
                    "question": question,
                    "sql": sql,
                    "results_count": len(results) if isinstance(results, list) else 0
                }
            
        except Exception as e:
            # Return a fallback response
            fallback_response = self._create_fallback_response(question, sql, results)
            return {
                "success": False,
                "response": fallback_response,
                "question": question,
                "sql": sql,
                "results_count": 0
            }
    
    def _format_results_for_display(self, results: Any) -> str:
        """Format results for display in text response"""
        try:
            if not results:
                return "No results found."
            
            if isinstance(results, list):
                if not results:
                    return "No results found."
                
                # Handle single result
                if len(results) == 1:
                    return self._format_single_result(results[0])
                
                # Handle multiple results
                return self._format_multiple_results(results)
            
            elif isinstance(results, dict):
                return self._format_single_result(results)
            
            else:
                return str(results)
                
        except Exception as e:
            print(f"Error formatting results: {e}")
            return "Error formatting results for display."
    
    def _format_single_result(self, result: Dict) -> str:
        """Format a single result for display"""
        try:
            if not result:
                return "Empty result."
            
            formatted_items = []
            for key, value in result.items():
                formatted_value = self._format_value(value)
                formatted_items.append(f"{key}: {formatted_value}")
            
            return "Result: " + ", ".join(formatted_items)
            
        except Exception as e:
            print(f"Error formatting single result: {e}")
            return "Error formatting result."
    
    def _format_multiple_results(self, results: List[Dict]) -> str:
        """Format multiple results for display"""
        try:
            if not results:
                return "No results found."
            
            total_count = len(results)
            
            # Show more results for better data coverage (increased from 5 to 15)
            display_limit = min(15, total_count)  # Show up to 15 results or all if fewer
            formatted_results = []
            
            for i, result in enumerate(results[:display_limit]):
                formatted_items = []
                for key, value in result.items():
                    formatted_value = self._format_value(value)
                    formatted_items.append(f"{key}: {formatted_value}")
                
                formatted_results.append(f"Result {i+1}: " + ", ".join(formatted_items))
            
            response = f"Found {total_count} results:\n" + "\n".join(formatted_results)
            
            if total_count > display_limit:
                response += f"\n... and {total_count - display_limit} more results"
            
            return response
            
        except Exception as e:
            print(f"Error formatting multiple results: {e}")
            return "Error formatting results."
    
    def _format_value(self, value: Any) -> str:
        """Format a single value for display"""
        try:
            if value is None:
                return "None"
            elif isinstance(value, bool):
                return "Yes" if value else "No"
            elif isinstance(value, (int, float)):
                # Format numbers with appropriate precision
                if isinstance(value, float):
                    return f"{value:.2f}"
                return str(value)
            elif isinstance(value, Decimal):
                return f"{value:.2f}"
            elif isinstance(value, (datetime, date)):
                return value.strftime("%Y-%m-%d")
            elif isinstance(value, str):
                return value
            else:
                return str(value)
                
        except Exception as e:
            print(f"Error formatting value: {e}")
            return str(value)
    
    def _format_results_manually(self, results: list) -> str:
        """Manually format results when automatic formatting fails"""
        try:
            if not results:
                return "No results found."
            
            formatted_lines = []
            
            # Add header
            formatted_lines.append(f"Found {len(results)} results:")
            
            # Format each result - increased limit from 10 to 15 for better data coverage
            display_limit = min(15, len(results))
            for i, result in enumerate(results[:display_limit]):
                if isinstance(result, dict):
                    items = []
                    for key, value in result.items():
                        safe_value = self._clean_data_for_template(value)
                        items.append(f"{key}: {safe_value}")
                    
                    formatted_lines.append(f"  {i+1}. {', '.join(items)}")
                else:
                    safe_result = self._clean_data_for_template(result)
                    formatted_lines.append(f"  {i+1}. {safe_result}")
            
            if len(results) > display_limit:
                formatted_lines.append(f"  ... and {len(results) - display_limit} more results")
            
            return "\n".join(formatted_lines)
            
        except Exception as e:
            print(f"Error in manual formatting: {e}")
            return f"Results found but formatting failed: {len(results) if results else 0} rows"
    
    def _clean_for_template(self, text: str) -> str:
        """Clean text for template safety"""
        if not isinstance(text, str):
            return str(text)
        
        # Remove or replace problematic characters
        text = text.replace('{', '{{').replace('}', '}}')
        text = text.replace('\n', ' ').replace('\r', ' ')
        text = re.sub(r'\s+', ' ', text)  # Replace multiple spaces with single space
        
        return text.strip()
    
    def _clean_data_for_template(self, data: Any) -> str:
        """Clean data for safe template insertion"""
        try:
            if data is None:
                return "None"
            elif isinstance(data, bool):
                return "Yes" if data else "No"
            elif isinstance(data, (int, float)):
                return str(data)
            elif isinstance(data, Decimal):
                return f"{data:.2f}"
            elif isinstance(data, (datetime, date)):
                return data.strftime("%Y-%m-%d")
            elif isinstance(data, str):
                # Clean string data
                cleaned = data.replace('{', '{{').replace('}', '}}')
                cleaned = cleaned.replace('\n', ' ').replace('\r', ' ')
                cleaned = re.sub(r'\s+', ' ', cleaned)  # Replace multiple spaces with single space
                return cleaned.strip()
            elif isinstance(data, (list, dict)):
                # Convert to string and clean
                str_data = str(data)
                return self._clean_for_template(str_data)
            else:
                return str(data)
                
        except Exception as e:
            print(f"Error cleaning data: {e}")
            return "Error"
    
    def _create_safe_results_summary(self, results) -> str:
        """Create a safe summary of results for template insertion"""
        try:
            if not results:
                return "No results found."
            
            if isinstance(results, list):
                count = len(results)
                if count == 0:
                    return "No results found."
                elif count == 1:
                    # Single result
                    first_result = results[0]
                    if isinstance(first_result, dict):
                        keys = list(first_result.keys())
                        return f"Found 1 result with fields: {', '.join(keys[:5])}"
                    else:
                        return "Found 1 result."
                else:
                    # Multiple results
                    first_result = results[0]
                    if isinstance(first_result, dict):
                        keys = list(first_result.keys())
                        return f"Found {count} results with fields: {', '.join(keys[:5])}"
                    else:
                        return f"Found {count} results."
            
            elif isinstance(results, dict):
                keys = list(results.keys())
                return f"Found result with fields: {', '.join(keys[:5])}"
            
            else:
                return f"Found result: {type(results).__name__}"
                
        except Exception as e:
            print(f"Error creating safe results summary: {e}")
            return "Results found but summary failed."
    
    def _extract_response_content(self, response) -> str:
        """Extract content from LLM response"""
        try:
            if hasattr(response, 'content'):
                return response.content.strip()
            elif hasattr(response, 'text'):
                return response.text.strip()
            elif isinstance(response, str):
                return response.strip()
            else:
                return str(response).strip()
        except Exception as e:
            print(f"Error extracting response content: {e}")
            return ""
    
    def _create_fallback_response(self, question: str, sql: str = None, results: Any = None) -> str:
        """Create a fallback response when LLM fails"""
        try:
            if not results:
                return "No results were found for your query."
            
            if isinstance(results, list):
                count = len(results)
                if count == 0:
                    return "No results were found for your query."
                elif count == 1:
                    return f"Found 1 result for your query."
                else:
                    return f"Found {count} results for your query."
            
            return "Query executed successfully."
            
        except Exception as e:
            print(f"Error creating fallback response: {e}")
            return "Query completed." 