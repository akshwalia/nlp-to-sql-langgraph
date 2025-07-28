# Single Table Analyzer

A simplified, focused database analyzer designed to comprehensively analyze a single table and provide detailed insights for LLM consumption.

## Overview

The `SingleTableAnalyzer` is a streamlined version of the original `DatabaseAnalyzer` that focuses on analyzing just one table at a time using SQLite databases. It provides comprehensive analysis including structure, data quality, relationships, and generates formatted output suitable for LLM processing.

## Features

### Comprehensive Analysis
- **Table Structure**: Columns, data types, nullable fields, constraints
- **Data Content**: Row count, table size, column statistics
- **Constraints & Indexes**: Primary keys, foreign keys, unique constraints, indexes
- **Relationships**: References to/from other tables
- **Data Quality**: Completeness analysis, potential issues detection
- **Sample Data**: Representative data samples
- **Statistics**: Comprehensive table and column statistics
- **Recommendations**: Actionable insights based on analysis

### LLM-Ready Output
- Formatted text output optimized for LLM consumption
- Structured JSON data for programmatic access
- Detailed logging for debugging and monitoring
- File output for persistence and sharing

## Installation

The `SingleTableAnalyzer` is part of the database analysis package:

```python
from src.core.database.analysis import SingleTableAnalyzer
```

## Usage

### Basic Usage

```python
from src.core.database.analysis import SingleTableAnalyzer

# Initialize analyzer (defaults to "IT_Professional_Services")
analyzer = SingleTableAnalyzer(
    db_path="./data/PBTest.db",
    table_name="IT_Professional_Services",  # Hardcoded for now, adjustable
    schema_name=None,  # SQLite doesn't use schemas
    output_file="analysis_output.txt"
)

# Perform comprehensive analysis
result = analyzer.analyze_table(save_to_file=True)

if result["success"]:
    print("Analysis completed successfully!")
    print(f"Results saved to: {analyzer.output_file}")
else:
    print(f"Analysis failed: {result['error']}")
```

### Changing Table Name

```python
# Change the table to analyze
analyzer.set_table_name("users", None)  # SQLite doesn't use schemas

# Re-run analysis
result = analyzer.analyze_table()
```

### Getting LLM Context

```python
# Get formatted context for LLM consumption
llm_context = analyzer.get_llm_context()
print(llm_context)
```

### Getting Analysis Summary

```python
# Get a summary of the analysis
summary = analyzer.get_analysis_summary()
print(summary)
```

## Configuration

### Constructor Parameters

- `db_path`: Path to SQLite database file
- `table_name`: Table to analyze (default: "IT_Professional_Services")
- `schema_name`: Schema name (default: None for SQLite)
- `output_file`: Output file path (default: "single_table_analysis.txt")

### Legacy Parameters (for backward compatibility)
- `db_name`: Database name (ignored for SQLite)
- `username`: Database username (ignored for SQLite)
- `password`: Database password (ignored for SQLite)
- `host`: Database host (ignored for SQLite)
- `port`: Database port (ignored for SQLite)

### Environment Variables

Create a `.env` file with your database connection details:

```env
DB_PATH=./data/PBTest.db
DB_NAME=PBTest
# Legacy PostgreSQL variables (ignored for SQLite)
DB_USERNAME=
DB_PASSWORD=
DB_HOST=
DB_PORT=
```

## Output Format

### File Output

The analyzer generates two types of output in the file:

1. **LLM Context**: Human-readable formatted text optimized for LLM consumption
2. **Detailed Analysis**: Complete JSON structure with all analysis data

### LLM Context Format

```
DATABASE TABLE ANALYSIS: IT_Professional_Services
================================================================================

BASIC INFORMATION:
- Database Path: ./data/PBTest.db
- Table: IT_Professional_Services
- Analysis Date: 2024-01-XX...

TABLE STRUCTURE:
- Total Columns: 26
- Data Types: ['TEXT', 'REAL', 'INTEGER']

COLUMNS:
  - supplier: TEXT (Nullable: True)
  - job_title: TEXT (Nullable: True)
  - daily_rate_calc: REAL (Nullable: True)
  - year: INTEGER (Nullable: True)

DATA ANALYSIS:
- Total Rows: 73168
- Table Size: 15 MB

CONSTRAINTS AND INDEXES:
- Primary Key: None
- Foreign Keys: 0
- Indexes: 0

RELATIONSHIPS:
- Related Tables: ['IT_Professional_Services_description']
- Outgoing References: 0
- Incoming References: 0

SAMPLE DATA:
  Row 1: {'supplier': 'TCS', 'job_title': 'Software Engineer', 'daily_rate_calc': 500.0}
  Row 2: {'supplier': 'Accenture', 'job_title': 'Data Analyst', 'daily_rate_calc': 450.0}

RECOMMENDATIONS:
  - [HIGH] Consider adding a primary key to improve data integrity
  - [MEDIUM] Consider adding indexes on frequently queried columns
```

## Analysis Components

### 1. Table Structure Analysis
- Column names, types, and properties
- Nullable vs non-nullable fields
- Data type distribution
- Default values and auto-increment fields

### 2. Data Content Analysis
- Row count and table size
- Column-level statistics (min, max, avg, stddev for numeric)
- Null percentage and distinct value counts
- Most common values for categorical columns

### 3. Constraints and Indexes
- Primary key identification
- Foreign key relationships
- Unique constraints
- Check constraints
- Index analysis

### 4. Relationship Analysis
- Tables that this table references (outgoing)
- Tables that reference this table (incoming)
- Complete list of related tables

### 5. Data Quality Analysis
- Completeness assessment
- Potential data quality issues
- Duplicate row detection
- Consistency checks

### 6. Recommendations
- Structure improvements
- Performance optimizations
- Data quality enhancements
- Scalability considerations

## Testing

Run the test script to verify functionality:

```bash
python test_single_table_analyzer.py
```

The test script will:
1. Connect to your database
2. Analyze the "IT_Professional_Services" table
3. Generate output file
4. Display summary and LLM context
5. Show how to change table names

## Logging

The analyzer provides comprehensive logging at different levels:

- **INFO**: Major operations and progress
- **DEBUG**: Detailed step-by-step analysis
- **WARNING**: Non-critical issues
- **ERROR**: Critical failures

Configure logging in your application:

```python
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
```

## Error Handling

Common issues and solutions:

### Table Not Found
- Verify table name and schema
- Check database connection
- Ensure table exists in specified schema

### Connection Issues
- Verify SQLite database file exists at specified path
- Check file permissions for SQLite database
- Ensure directory structure exists

### Permission Issues
- Verify file system read permissions for SQLite database
- Check write permissions if analysis needs to create output files
- Ensure proper file path access

## Integration with LLM

The analyzer is designed to work seamlessly with LLM systems:

```python
# Get analysis for LLM
analyzer = SingleTableAnalyzer(
    db_path="./data/PBTest.db",
    table_name="IT_Professional_Services"
)
result = analyzer.analyze_table()

if result["success"]:
    # Get formatted context for LLM
    llm_context = analyzer.get_llm_context()
    
    # Send to LLM
    llm_response = your_llm_client.generate_response(
        prompt=f"Analyze this SQLite database table:\n\n{llm_context}"
    )
    
    print(llm_response)
```

## API Reference

### Main Methods

#### `analyze_table(save_to_file: bool = True) -> Dict[str, Any]`
Performs comprehensive table analysis.

#### `set_table_name(table_name: str, schema_name: str = "public")`
Changes the target table for analysis.

#### `get_llm_context() -> str`
Returns formatted text suitable for LLM consumption.

#### `get_analysis_summary() -> Dict[str, Any]`
Returns a concise summary of the analysis results.

### Properties

- `table_name`: Current table name
- `schema_name`: Current schema name
- `output_file`: Path to output file
- `table_analysis`: Complete analysis results
- `llm_context`: Formatted LLM context

## Future Enhancements

- Support for additional database types (MySQL, MariaDB, etc.)
- Advanced data profiling and statistics
- Performance metrics and optimization suggestions
- Integration with data quality frameworks
- Export to different formats (CSV, Excel, etc.)
- Enhanced SQLite-specific optimizations

## Contributing

When contributing to the Single Table Analyzer:

1. Maintain comprehensive logging
2. Follow the existing code structure
3. Add appropriate error handling
4. Update documentation and examples
5. Test with various table types and sizes

## License

This component is part of the NLP_TO_SQL project and follows the same licensing terms. 