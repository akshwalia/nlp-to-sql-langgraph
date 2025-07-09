from typing import Dict, Any, List
from src.observability.langfuse_config import observe_function


class ChartRecommendationsManager:
    """Manages chart recommendations for the SQL generator"""
    
    def __init__(self, prompts_manager, memory_manager, llm):
        self.prompts_manager = prompts_manager
        self.memory_manager = memory_manager
        self.llm = llm
    
    @observe_function("chart_recommendations")
    def generate_chart_recommendations(self, question: str, sql: str, results: List[Dict[str, Any]], database_type: str = None) -> Dict[str, Any]:
        """Generate chart recommendations based on query results"""
        try:
            # Analyze data characteristics
            data_characteristics = self._analyze_data_characteristics(results)
            
            # Check if data is visualizable
            is_visualizable = self._is_data_visualizable(data_characteristics)
            
            if not is_visualizable:
                return {
                    "is_visualizable": False,
                    "reason": "Data structure not suitable for visualization",
                    "recommended_charts": [],
                    "database_type": database_type or "general",
                    "data_characteristics": data_characteristics
                }
            
            # Create chart recommendation prompt if not already created
            if not self.prompts_manager.chart_recommendation_prompt:
                self.prompts_manager.create_chart_recommendation_prompt()
            
            # Prepare prompt values
            prompt_values = {
                "question": question,
                "sql": sql,
                "results": str(results[:3]),  # Show first 3 results
                "data_characteristics": str(data_characteristics),
                "schema": "Database schema context"
            }
            
            # Handle memory context with enhanced error checking
            if self.memory_manager.use_memory:
                try:
                    memory_context = self.memory_manager.get_memory_context(question)
                    
                    if isinstance(memory_context, (tuple, list)):
                        # Convert tuple/list to string safely
                        if len(memory_context) > 0:
                            memory_context = str(memory_context[0])
                        else:
                            memory_context = ""
                    prompt_values["memory"] = memory_context
                except Exception as memory_error:
                    prompt_values["memory"] = ""
            
            # Generate recommendations with better error handling
            try:
                # Invoke LLM with enhanced error handling
                try:
                    formatted_messages = self.prompts_manager.chart_recommendation_prompt.format_messages(**prompt_values)
                    response = self.llm.invoke(formatted_messages)
                    
                except Exception as llm_invoke_error:
                    return self._create_fallback_recommendations(data_characteristics, results)
                
                # Extract response content with enhanced error handling
                try:
                    response_text = self._extract_response_content(response)
                    
                    if not response_text.strip():
                        return self._create_fallback_recommendations(data_characteristics, results)
                        
                except Exception as extract_error:
                    return self._create_fallback_recommendations(data_characteristics, results)
                
                # Parse and format recommendations
                try:
                    import json
                    
                    # Try to parse the JSON response
                    if response_text.strip():
                        try:
                            recommendations_data = json.loads(response_text)
                            formatted_recommendations = self._format_chart_recommendations(recommendations_data, data_characteristics)
                            return formatted_recommendations
                        except json.JSONDecodeError as json_error:
                            return self._create_fallback_recommendations(data_characteristics, results)
                    else:
                        return self._create_fallback_recommendations(data_characteristics, results)
                        
                except (KeyError, TypeError) as e:
                    return self._create_fallback_recommendations(data_characteristics, results)
                except Exception as format_error:
                    return self._create_fallback_recommendations(data_characteristics, results)
                    
            except Exception as llm_error:
                return self._create_fallback_recommendations(data_characteristics, results)
                
        except Exception as e:
            return self._create_fallback_recommendations({}, results)
    
    def _analyze_data_characteristics(self, results: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Analyze characteristics of the data for visualization"""
        try:
            if not results:
                return {
                    "row_count": 0,
                    "column_count": 0,
                    "all_columns": [],
                    "numerical_columns": [],
                    "categorical_columns": [],
                    "date_columns": [],
                    "data_types": {}
                }
            
            first_row = results[0]
            all_columns = list(first_row.keys())
            
            # Analyze column types
            numerical_columns = []
            categorical_columns = []
            date_columns = []
            data_types = {}
            
            for column in all_columns:
                try:
                    # Get sample values with better bounds checking
                    sample_values = []
                    for row in results[:10]:  # Check first 10 rows
                        if isinstance(row, dict) and row.get(column) is not None:
                            sample_values.append(row.get(column))
                    
                    if not sample_values:
                        # Default to categorical if no sample values
                        categorical_columns.append(column)
                        data_types[column] = "categorical"
                        continue
                    
                    # Safely get the first sample value with bounds checking
                    sample_value = None
                    if len(sample_values) > 0:
                        sample_value = sample_values[0]
                    
                    if sample_value is None:
                        categorical_columns.append(column)
                        data_types[column] = "categorical"
                        continue
                    
                    # Determine column type
                    if isinstance(sample_value, (int, float)):
                        numerical_columns.append(column)
                        data_types[column] = "numerical"
                    elif isinstance(sample_value, str):
                        # Check if it's a date string with safe bounds checking
                        if self._is_date_column(column, sample_values):
                            date_columns.append(column)
                            data_types[column] = "date"
                        else:
                            categorical_columns.append(column)
                            data_types[column] = "categorical"
                    else:
                        categorical_columns.append(column)
                        data_types[column] = "categorical"
                except (IndexError, KeyError, TypeError, AttributeError) as e:
                    print(f"Error analyzing column {column}: {e}")
                    # Default to categorical if we can't determine type
                    categorical_columns.append(column)
                    data_types[column] = "categorical"
                except Exception as e:
                    print(f"Unexpected error analyzing column {column}: {e}")
                    # Default to categorical if we can't determine type
                    categorical_columns.append(column)
                    data_types[column] = "categorical"
            
            return {
                "row_count": len(results),
                "column_count": len(all_columns),
                "all_columns": all_columns,
                "numerical_columns": numerical_columns,
                "categorical_columns": categorical_columns,
                "date_columns": date_columns,
                "data_types": data_types
            }
            
        except Exception as e:
            print(f"Error analyzing data characteristics: {e}")
            return {
                "row_count": 0,
                "column_count": 0,
                "all_columns": [],
                "numerical_columns": [],
                "categorical_columns": [],
                "date_columns": [],
                "data_types": {}
            }
    
    def _is_date_column(self, column_name: str, sample_values: List) -> bool:
        """Check if a column contains date data"""
        try:
            # Check column name for date indicators
            date_indicators = ['date', 'time', 'created', 'updated', 'modified']
            if any(indicator in column_name.lower() for indicator in date_indicators):
                return True
            
            # Check sample values for date patterns with bounds checking
            if not sample_values or len(sample_values) == 0:
                return False
            
            import re
            date_patterns = [
                r'\d{4}-\d{2}-\d{2}',  # YYYY-MM-DD
                r'\d{2}/\d{2}/\d{4}',  # MM/DD/YYYY
                r'\d{4}/\d{2}/\d{2}',  # YYYY/MM/DD
            ]
            
            # Check first 5 values safely
            values_to_check = sample_values[:5] if len(sample_values) >= 5 else sample_values
            
            for value in values_to_check:
                if isinstance(value, str):
                    for pattern in date_patterns:
                        if re.search(pattern, value):
                            return True
            
            return False
        except Exception as e:
            print(f"Error checking date column {column_name}: {e}")
            return False
    
    def _is_data_visualizable(self, data_characteristics: Dict[str, Any]) -> bool:
        """Check if data can be visualized"""
        row_count = data_characteristics.get("row_count", 0)
        column_count = data_characteristics.get("column_count", 0)
        
        # Need at least some data to visualize
        if row_count == 0 or column_count == 0:
            return False
        
        # Need at least one numerical or categorical column
        numerical_columns = data_characteristics.get("numerical_columns", [])
        categorical_columns = data_characteristics.get("categorical_columns", [])
        date_columns = data_characteristics.get("date_columns", [])
        
        return len(numerical_columns) > 0 or len(categorical_columns) > 0 or len(date_columns) > 0
    
    def _create_fallback_recommendations(self, data_characteristics: Dict, results: List[Dict]) -> Dict[str, Any]:
        """Create fallback chart recommendations when LLM fails"""
        try:
            if not data_characteristics:
                data_characteristics = self._analyze_data_characteristics(results)
            
            if not self._is_data_visualizable(data_characteristics):
                return {
                    "is_visualizable": False,
                    "reason": "Data structure not suitable for visualization",
                    "recommended_charts": [],
                    "database_type": "general",
                    "data_characteristics": data_characteristics
                }
            
            numerical_cols = data_characteristics.get("numerical_columns", [])
            categorical_cols = data_characteristics.get("categorical_columns", [])
            date_cols = data_characteristics.get("date_columns", [])
            
            recommendations = []
            
            # Basic chart recommendations
            if len(numerical_cols) >= 1 and len(categorical_cols) >= 1:
                recommendations.append({
                    "chart_type": "bar",
                    "title": f"{numerical_cols[0]} by {categorical_cols[0]}",
                    "description": "Bar chart showing values across categories",
                    "x_axis": categorical_cols[0],
                    "y_axis": numerical_cols[0],
                    "secondary_y_axis": None,
                    "chart_config": {},
                    "confidence_score": 0.8
                })
            
            if len(date_cols) >= 1 and len(numerical_cols) >= 1:
                recommendations.append({
                    "chart_type": "line",
                    "title": f"{numerical_cols[0]} over time",
                    "description": "Line chart showing trends over time",
                    "x_axis": date_cols[0],
                    "y_axis": numerical_cols[0],
                    "secondary_y_axis": None,
                    "chart_config": {},
                    "confidence_score": 0.9
                })
            
            if len(numerical_cols) >= 2:
                recommendations.append({
                    "chart_type": "scatter",
                    "title": f"{numerical_cols[0]} vs {numerical_cols[1]}",
                    "description": "Scatter plot showing correlation",
                    "x_axis": numerical_cols[0],
                    "y_axis": numerical_cols[1],
                    "secondary_y_axis": None,
                    "chart_config": {},
                    "confidence_score": 0.7
                })
            
            return {
                "is_visualizable": len(recommendations) > 0,
                "reason": None if len(recommendations) > 0 else "No suitable chart types found",
                "recommended_charts": recommendations,
                "database_type": "general",
                "data_characteristics": data_characteristics
            }
            
        except Exception as e:
            print(f"Error creating fallback recommendations: {e}")
            return {
                "is_visualizable": False,
                "reason": "Error generating recommendations",
                "recommended_charts": [],
                "database_type": "general",
                "data_characteristics": data_characteristics
            }
    
    def _format_chart_recommendations(self, recommendations_data: Dict, data_characteristics: Dict) -> Dict[str, Any]:
        """Format and validate chart recommendations from LLM response"""
        try:
            formatted = {
                "is_visualizable": recommendations_data.get("is_visualizable", False),
                "reason": recommendations_data.get("reason"),
                "recommended_charts": [],
                "database_type": recommendations_data.get("database_type", "general"),
                "data_characteristics": recommendations_data.get("data_characteristics", data_characteristics)
            }
            
            # Validate and format charts with more flexible validation
            available_columns = data_characteristics.get("all_columns", [])
            
            for chart in recommendations_data.get("recommended_charts", []):
                
                if isinstance(chart, dict) and chart.get("chart_type") and chart.get("title"):
                    # More flexible validation - allow charts with reasonable axis names
                    x_axis = chart.get("x_axis")
                    y_axis = chart.get("y_axis")
                    
                    # For visualization purposes, we can be more lenient with axis names
                    # The frontend can handle mapping display names to actual columns
                    is_valid_chart = True
                    
                    # Basic validation - ensure we have required fields
                    if not chart.get("chart_type") or not chart.get("title"):
                        is_valid_chart = False
                    
                    # For certain chart types, we need both axes
                    chart_type = chart.get("chart_type", "").lower()
                    if chart_type in ["bar", "column", "line", "scatter"] and (not x_axis or not y_axis):
                        is_valid_chart = False
                    
                    if is_valid_chart:
                        formatted_chart = {
                            "chart_type": str(chart["chart_type"]),
                            "title": str(chart["title"]),
                            "description": str(chart.get("description", "")),
                            "x_axis": x_axis,
                            "y_axis": y_axis,
                            "secondary_y_axis": chart.get("secondary_y_axis"),
                            "chart_config": chart.get("chart_config", {}),
                            "confidence_score": float(chart.get("confidence_score", 0.5))
                        }
                        formatted["recommended_charts"].append(formatted_chart)
                    else:
                        print(f"Skipped invalid chart: {chart.get('title', 'Unknown')}")
            
            return formatted
            
        except Exception as e:
            print(f"Error formatting chart recommendations: {e}")
            import traceback
            traceback.print_exc()
            return self._create_fallback_recommendations(data_characteristics, [])
    
    def _extract_response_content(self, response) -> str:
        """Extract content from LLM response"""
        try:
            # Handle different types of LLM responses
            if hasattr(response, 'content'):
                content = response.content
                if isinstance(content, str):
                    return content.strip()
                elif isinstance(content, (list, tuple)):
                    # Handle content as list/tuple - check bounds first
                    if len(content) > 0:
                        return str(content[0]).strip()
                    else:
                        print("Warning: Empty content list/tuple received")
                        return ""
                else:
                    return str(content).strip()
            elif hasattr(response, 'text'):
                return response.text.strip()
            elif isinstance(response, str):
                return response.strip()
            elif isinstance(response, (list, tuple)):
                # Handle response as list/tuple - check bounds first
                if len(response) > 0:
                    return str(response[0]).strip()
                else:
                    print("Warning: Empty response list/tuple received")
                    return ""
            else:
                return str(response).strip()
        except (IndexError, AttributeError, TypeError) as e:
            print(f"Error extracting response content: {e}")
            return ""
        except Exception as e:
            print(f"Unexpected error extracting response content: {e}")
            return "" 