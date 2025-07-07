# Transaction Support and Smart Schema Updates

This document describes the enhanced transaction support and smart schema update features implemented in the NLP to SQL system.

## Overview

The system now supports:

1. **Transaction-based Multi-Query Execution** - Multiple SQL queries are executed in a single transaction with automatic rollback on failure
2. **Smart Schema Updates** - The system automatically detects and handles schema changes without requiring full database re-analysis
3. **Enhanced Error Handling** - Comprehensive error reporting with rollback information

## Features

### 1. Transaction Support

#### Multi-Query Separator
- Use `<----->` to separate multiple SQL queries
- All queries in a multi-query request are executed in a single transaction
- If any query fails, all previous queries in the transaction are automatically rolled back

#### Example Usage
```sql
CREATE TABLE users (id SERIAL PRIMARY KEY, name VARCHAR(100))
<----->
INSERT INTO users (name) VALUES ('John Doe')
<----->
INSERT INTO users (name) VALUES ('Jane Smith')
```

#### Transaction Behavior
- **Success**: All queries execute successfully and are committed
- **Failure**: If any query fails, the entire transaction is rolled back
- **Atomicity**: Either all queries succeed, or none of them do

### 2. Smart Schema Updates

#### Automatic Detection
The system automatically detects schema-changing operations:
- `CREATE TABLE`, `DROP TABLE`, `ALTER TABLE`
- `CREATE INDEX`, `DROP INDEX`
- `CREATE VIEW`, `DROP VIEW`
- `CREATE SCHEMA`, `DROP SCHEMA`
- `RENAME TABLE`, `ADD COLUMN`, `DROP COLUMN`, `RENAME COLUMN`
- `CREATE SEQUENCE`, `DROP SEQUENCE`
- `TRUNCATE TABLE`

#### Incremental Updates
Instead of re-analyzing the entire database, the system:
- Detects specific changes from executed SQL
- Updates only the affected parts of the schema context
- Maintains relationships and constraints information
- Refreshes sample data when needed

#### Fallback Mechanism
If incremental updates fail, the system automatically falls back to full schema re-analysis.

## API Enhancements

### Enhanced Response Format

#### Transaction Information
```json
{
  "success": true,
  "transaction_mode": true,
  "query_count": 3,
  "affected_rows": 15,
  "query_results": [
    {
      "query_number": 1,
      "sql": "CREATE TABLE...",
      "success": true,
      "affected_rows": 0,
      "error": null
    }
  ]
}
```

#### Schema Update Information
```json
{
  "success": true,
  "schema_refreshed": true,
  "warning": null
}
```

#### Rollback Information (on failure)
```json
{
  "success": false,
  "transaction_mode": true,
  "rollback_performed": true,
  "failed_at_query": 2,
  "error": "Transaction failed at query 2: table 'xyz' does not exist (Transaction rolled back)"
}
```

### New Endpoints

#### Manual Schema Refresh
```http
POST /workspaces/{workspace_id}/refresh-schema
```
- Manually refresh schema analysis for a workspace
- Updates all active SQL generators for the workspace
- Returns list of refreshed sessions

## Implementation Details

### Database Analyzer Enhancements

#### New Methods
- `execute_query_with_transaction(queries: List[str])` - Execute multiple queries in transaction
- `_detect_schema_changes(queries: List[str])` - Detect schema-changing operations
- `_update_schema_from_queries(queries: List[str])` - Incrementally update schema
- `refresh_schema_for_table(table_name: str)` - Refresh specific table info

#### Transaction Handling
- Uses PostgreSQL's native transaction support
- Explicit transaction control with `BEGIN`, `COMMIT`, `ROLLBACK`
- Connection pooling support for better performance

### Smart SQL Generator Enhancements

#### New Methods
- `execute_edit_query_with_schema_update(sql: str)` - Execute with automatic schema updates
- `refresh_schema_context()` - Refresh the AI's schema understanding
- `check_and_refresh_schema_if_needed(sql: str)` - Conditional schema refresh

#### Enhanced Execution Flow
1. Parse and clean SQL statements
2. Determine if single or multi-query execution
3. For multi-query: Use transaction-based execution
4. Detect schema changes in executed SQL
5. Automatically refresh schema context if needed
6. Update AI's understanding of database structure

## Usage Examples

### Creating Tables with Data
```sql
CREATE TABLE products (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    price DECIMAL(10,2),
    category_id INTEGER
)
<----->
CREATE TABLE categories (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL
)
<----->
INSERT INTO categories (name) VALUES ('Electronics'), ('Books'), ('Clothing')
<----->
INSERT INTO products (name, price, category_id) VALUES 
    ('Laptop', 999.99, 1),
    ('Book', 19.99, 2),
    ('T-Shirt', 29.99, 3)
```

### Schema Modifications
```sql
ALTER TABLE products ADD COLUMN description TEXT
<----->
ALTER TABLE products ADD COLUMN created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
<----->
CREATE INDEX idx_products_category ON products(category_id)
<----->
UPDATE products SET description = 'High-quality laptop' WHERE name = 'Laptop'
```

### Handling Failures
If any query in a multi-query transaction fails:
- All previous queries are rolled back
- Database remains in original state
- Detailed error information is provided
- No partial changes are committed

## Benefits

### Data Consistency
- **Atomicity**: All-or-nothing execution prevents partial updates
- **Consistency**: Database constraints are maintained across multi-query operations
- **Isolation**: Transactions prevent interference from concurrent operations

### Performance
- **Reduced Re-analysis**: Incremental schema updates are much faster than full re-analysis
- **Connection Pooling**: Efficient database connection management
- **Batch Operations**: Multiple queries execute in single transaction

### User Experience
- **Automatic Updates**: Schema changes are detected and handled automatically
- **Clear Feedback**: Detailed information about transaction status and rollbacks
- **Graceful Degradation**: Fallback mechanisms ensure system reliability

## Error Handling

### Transaction Failures
- Automatic rollback on any query failure
- Detailed error reporting with query number and specific error message
- Transaction status clearly indicated in response

### Schema Update Failures
- Graceful fallback to full schema re-analysis
- Warning messages when incremental updates fail
- System continues to function even if schema refresh fails

### Connection Issues
- Connection pooling with retry logic
- Graceful handling of database connectivity problems
- Clear error messages for debugging

## Configuration

### Environment Variables
No additional environment variables are required. The system uses existing database connection parameters.

### Database Requirements
- PostgreSQL database with transaction support
- Appropriate user permissions for schema operations
- Connection pooling support (optional but recommended)

## Monitoring and Debugging

### Logging
The system provides detailed logging for:
- Transaction execution status
- Schema change detection
- Incremental update operations
- Fallback to full re-analysis

### Response Metadata
API responses include comprehensive metadata:
- Transaction mode indicators
- Query execution details
- Schema refresh status
- Error locations and rollback information

## Best Practices

### Multi-Query Design
1. **Order Matters**: Place schema changes before data operations
2. **Dependencies**: Ensure proper order for foreign key relationships
3. **Size Limits**: Keep transactions reasonably sized for performance
4. **Error Handling**: Design queries to minimize failure points

### Schema Changes
1. **Test First**: Test schema changes in development environment
2. **Backup**: Ensure database backups before major schema changes
3. **Incremental**: Make schema changes incrementally when possible
4. **Documentation**: Document schema changes for team awareness

### Performance Optimization
1. **Batch Operations**: Use multi-query transactions for related operations
2. **Index Creation**: Create indexes after bulk data insertions
3. **Connection Pooling**: Use connection pooling for better performance
4. **Monitor Resources**: Monitor database resources during large operations 