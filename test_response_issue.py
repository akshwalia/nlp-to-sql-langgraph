#!/usr/bin/env python3
"""
Test script to debug text response and chart recommendations issues.
"""

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from core.langgraph.sql_generator import SmartSQLGenerator
from core.database import DatabaseAnalyzer


def test_response_generation():
    """Test text response and chart recommendations generation"""
    print("Testing response generation...")
    
    # Mock test data similar to the user's example
    test_question = "Hello, give me top 10 employees by the number of orders they have handled."
    test_sql = """SELECT e.businessentityid, p.firstname, p.lastname, COUNT(so.salesorderid) AS number_of_orders
FROM humanresources.employee e
JOIN person.person p ON e.businessentityid = p.businessentityid
LEFT JOIN sales.salesorderheader so ON e.businessentityid = so.salespersonid
GROUP BY e.businessentityid, p.firstname, p.lastname
ORDER BY number_of_orders DESC
LIMIT 10;"""
    
    test_results = [
        {"businessentityid": 277, "firstname": "Jillian", "lastname": "Carson", "number_of_orders": 473},
        {"businessentityid": 275, "firstname": "Michael", "lastname": "Blythe", "number_of_orders": 450},
        {"businessentityid": 279, "firstname": "Tsvi", "lastname": "Reiter", "number_of_orders": 429}
    ]
    
    try:
        # Create a mock database analyzer
        db_analyzer = None  # We'll mock this to avoid actual DB connection
        
        # Create SQL generator
        sql_generator = SmartSQLGenerator(
            db_analyzer=db_analyzer,
            use_memory=True,
            use_cache=False
        )
        
        # Test text response generation
        print("\n=== Testing Text Response Generation ===")
        text_result = sql_generator.generate_text_response(
            question=test_question,
            sql=test_sql,
            results=test_results
        )
        
        print(f"Text response success: {text_result['success']}")
        print(f"Text response: {text_result.get('response', 'NO RESPONSE')}")
        
        # Test chart recommendations generation
        print("\n=== Testing Chart Recommendations Generation ===")
        chart_result = sql_generator.generate_chart_recommendations(
            question=test_question,
            sql=test_sql,
            results=test_results
        )
        
        print(f"Chart recommendations success: {chart_result is not None}")
        print(f"Chart recommendations: {chart_result}")
        
        # Test the full unified query processing format
        print("\n=== Testing Response Structure ===")
        response = {
            "success": True,
            "question": test_question,
            "sql": test_sql,
            "results": test_results,
            "text": text_result.get("response", ""),
            "visualization_recommendations": chart_result,
            "execution_time": 0.058,
            "row_count": len(test_results),
            "query_type": "retrieve"
        }
        
        print(f"Final response text: '{response['text']}'")
        print(f"Final response visualization_recommendations: {response['visualization_recommendations']}")
        
        # Check for issues
        if not response["text"]:
            print("❌ ISSUE: Text response is empty!")
        else:
            print("✅ Text response generated successfully")
            
        if response["visualization_recommendations"] is None:
            print("❌ ISSUE: Visualization recommendations are null!")
        else:
            print("✅ Visualization recommendations generated successfully")
        
    except Exception as e:
        print(f"❌ Error during testing: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    test_response_generation() 