import pandas as pd
import psycopg2
from sqlalchemy import create_engine, text
import re
import os

# Database connection parameters
DB_CONFIG = {
    'host': 'localhost',
    'port': '5432',
    'database': 'PBTest',
    'user': 'postgres',
    'password': 'Arjit#195'
}

FILE_CONFIG = {
    'data_file_path': 'GR_IT_Professional_Services_till_V219.xlsx',
    'data_sheet_name': 'GR_IT_Professional_Services_til',
    'description_file_path': 'ImportantColumns_PriceBenchmarks.xlsx',  # New field for description file
    'description_sheet_name': 'IT|Professional Services',  # Updated to correct sheet name
    'table_name': 'IT_Professional_Services',
    'chunk_size': 100,  # Reduced from 1000 to 100 rows per chunk
    'timeout': 60,  # Increased timeout to 60 seconds
    # Default values for empty cells based on data type
    'default_values': {
        'integer': None,  # NULL in database
        'decimal': None,  # NULL in database
        'string': '',
        'boolean': False,
        'date': None  # NULL in database
    }
}

# Create connection string
connection_string = f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"

def clean_column_name(col_name):
    """Clean column names by removing data type annotations and special characters"""
    # Remove everything after | (including |)
    cleaned = col_name.split('|')[0]
    # Remove special characters and replace with underscores
    cleaned = re.sub(r'[^a-zA-Z0-9_]', '_', cleaned)
    # Remove multiple underscores
    cleaned = re.sub(r'_+', '_', cleaned)
    # Remove leading/trailing underscores
    cleaned = cleaned.strip('_')
    return cleaned.lower()

def clean_description_text(description_text):
    """Clean description text by removing numbering and extracting the actual description"""
    if pd.isna(description_text) or description_text == '':
        return ''
    
    # Convert to string in case it's not
    desc_str = str(description_text)
    
    # Pattern to match: "1. Unique_ID: A unique identifier to track records."
    # We want to extract everything after the first colon
    if ':' in desc_str:
        # Split on first colon and take everything after it
        parts = desc_str.split(':', 1)
        if len(parts) > 1:
            cleaned_desc = parts[1].strip()
            return cleaned_desc
    
    # If no colon found, return the original text
    return desc_str.strip()

def convert_boolean_field(value):
    """Convert string values to boolean based on "1" = True, "0" = False"""
    # Handle pandas NaN values
    if pd.isna(value):
        return False
    
    # Convert to string and check
    str_value = str(value).strip()
    
    if str_value == '1' or str_value == '1.0':
        return True
    elif str_value == '0' or str_value == '0.0':
        return False
    else:
        # Default to False for any other value
        return False

def get_postgres_datatype(col_name, sample_data):
    """Determine appropriate PostgreSQL data type based on column name and sample data"""
    col_lower = col_name.lower()
    
    # Check if it's clearly a string type from the original header
    if '|string' in col_name.lower():
        return 'VARCHAR(255)'
    elif '|integer' in col_name.lower() or '|int' in col_name.lower():
        return 'INTEGER'
    elif '|decimal' in col_name.lower() or '|float' in col_name.lower():
        return 'DECIMAL(12,5)'
    elif '|date' in col_name.lower():
        return 'DATE'
    elif '|boolean' in col_name.lower() or '|bool' in col_name.lower():
        return 'BOOLEAN'
    
    # If no type annotation, infer from sample data
    # Remove NaN values to get a better sample for type detection
    non_null_sample = sample_data.dropna()
    
    if len(non_null_sample) == 0:
        # If all values are NaN, default to VARCHAR
        return 'VARCHAR(255)'
    
    if pd.api.types.is_numeric_dtype(non_null_sample):
        if pd.api.types.is_integer_dtype(non_null_sample):
            return 'INTEGER'
        else:
            return 'DECIMAL(10,2)'
    elif pd.api.types.is_datetime64_any_dtype(non_null_sample):
        return 'TIMESTAMP'
    else:
        return 'VARCHAR(255)'

def import_description_table(description_file_path, description_table_name, sheet_name=0):
    """Import the description/metadata table"""
    print(f"\nImporting description table from: {description_file_path}")
    
    # Read the description Excel file
    desc_df = pd.read_excel(description_file_path, sheet_name=sheet_name)
    
    # Expected columns: ColName, Common Name, Description, isImportant, MustHave, Mandatory Entity
    print(f"Description file columns: {list(desc_df.columns)}")
    print(f"Description file shape: {desc_df.shape}")
    print(f"First few rows preview:")
    print(desc_df.head())
    
    # Clean and process the data
    processed_rows = []
    skipped_rows = 0
    
    for row_num, (index, row) in enumerate(desc_df.iterrows(), 1):
        # Get and clean the column name (remove |Type annotation)
        col_name = str(row['ColName']) if 'ColName' in row else ''
        
        # Skip rows with empty or invalid column names
        if not col_name or col_name.strip() == '' or col_name == 'nan':
            skipped_rows += 1
            print(f"Skipping row {row_num}: empty or invalid column name")
            continue
            
        cleaned_col_name = clean_column_name(col_name)
        
        # Skip if cleaned column name is empty
        if not cleaned_col_name or cleaned_col_name.strip() == '':
            skipped_rows += 1
            print(f"Skipping row {row_num}: cleaned column name is empty (original: '{col_name}')")
            continue
        
        # Get common name
        common_name = str(row['Common Name']) if 'Common Name' in row else ''
        
        # Clean the description text
        description = clean_description_text(row['Description']) if 'Description' in row else ''
        
        # Convert boolean fields
        is_important = convert_boolean_field(row['isImportant']) if 'isImportant' in row else False
        must_have = convert_boolean_field(row['MustHave']) if 'MustHave' in row else False
        mandatory_entity = convert_boolean_field(row['Mandatory Entity']) if 'Mandatory Entity' in row else False
        
        processed_rows.append({
            'column_name': cleaned_col_name,
            'common_name': common_name,
            'description': description,
            'is_important': is_important,
            'must_have': must_have,
            'mandatory_entity': mandatory_entity
        })
    
    # Create DataFrame from processed rows
    processed_df = pd.DataFrame(processed_rows)
    
    print(f"Processed {len(processed_df)} description records")
    print(f"Skipped {skipped_rows} rows due to invalid/empty column names")
    
    # Check for duplicate column names
    duplicates = processed_df['column_name'].duplicated()
    if duplicates.any():
        duplicate_names = processed_df[duplicates]['column_name'].tolist()
        print(f"Warning: Found duplicate column names: {duplicate_names}")
        # Remove duplicates, keeping the first occurrence
        processed_df = processed_df.drop_duplicates(subset=['column_name'], keep='first')
        print(f"After removing duplicates: {len(processed_df)} records")
    
    if processed_df.empty:
        print("Error: No valid records found in description file!")
        return False
    
    # Show sample of processed data
    print(f"\nSample processed data:")
    print(processed_df.head())
    
    # Create SQLAlchemy engine
    engine = create_engine(
        connection_string,
        pool_size=10,
        max_overflow=20,
        pool_pre_ping=True,
        pool_recycle=3600,
        connect_args={
            "connect_timeout": FILE_CONFIG['timeout'],
            "options": f"-c statement_timeout={FILE_CONFIG['timeout'] * 1000}ms"
        }
    )
    
    # Drop table if exists
    with engine.connect() as conn:
        conn.execute(text(f'DROP TABLE IF EXISTS "{description_table_name}"'))
        conn.commit()
    
    # Create description table
    create_table_sql = f'''
    CREATE TABLE "{description_table_name}" (
        "column_name" VARCHAR(255) PRIMARY KEY,
        "common_name" VARCHAR(255),
        "description" TEXT,
        "is_important" BOOLEAN,
        "must_have" BOOLEAN,
        "mandatory_entity" BOOLEAN
    );
    '''
    
    print("Creating description table with SQL:")
    print(create_table_sql)
    
    # Execute table creation
    with engine.connect() as conn:
        conn.execute(text(create_table_sql))
        conn.commit()
    
    # Import the data
    print(f"Importing {len(processed_df)} rows to description table {description_table_name}")
    
    try:
        processed_df.to_sql(description_table_name, engine, if_exists='append', index=False, method='multi')
        print("Description table import successful!")
        
        # Show sample data
        with engine.connect() as conn:
            result = conn.execute(text(f'SELECT * FROM "{description_table_name}" LIMIT 5'))
            sample_data = result.fetchall()
            print(f"\nSample data from {description_table_name}:")
            for row in sample_data:
                print(row)
                
    except Exception as e:
        print(f"Error importing description table: {e}")
        return False
    
    return True

def import_excel_to_postgres(excel_file_path, table_name, sheet_name=0):
    """Import Excel file to PostgreSQL with proper column names and data types"""
    
    # Read Excel file
    print(f"Reading Excel file: {excel_file_path}")
    df = pd.read_excel(excel_file_path, sheet_name=sheet_name)
    
    # Store original column names for reference
    original_columns = df.columns.tolist()
    print(f"Original columns: {original_columns}")
    
    # Clean column names
    column_mapping = {}
    for col in df.columns:
        clean_name = clean_column_name(col)
        column_mapping[col] = clean_name
    
    # Rename columns
    df.rename(columns=column_mapping, inplace=True)
    
    print(f"Cleaned columns: {list(df.columns)}")
    
    # Create SQLAlchemy engine with better connection handling
    engine = create_engine(
        connection_string,
        pool_size=10,
        max_overflow=20,
        pool_pre_ping=True,
        pool_recycle=3600,
        connect_args={
            "connect_timeout": FILE_CONFIG['timeout'],
            "options": f"-c statement_timeout={FILE_CONFIG['timeout'] * 1000}ms"  # Convert to milliseconds
        }
    )
    
    # Drop table if exists
    with engine.connect() as conn:
        conn.execute(text(f'DROP TABLE IF EXISTS "{table_name}"'))
        conn.commit()
    
    # Create table with proper data types
    create_table_sql = f'CREATE TABLE "{table_name}" (\n'
    column_definitions = []
    
    for original_col, clean_col in column_mapping.items():
        data_type = get_postgres_datatype(original_col, df[clean_col])
        column_definitions.append(f'    "{clean_col}" {data_type}')
    
    create_table_sql += ',\n'.join(column_definitions)
    create_table_sql += '\n);'
    
    print("Creating table with SQL:")
    print(create_table_sql)
    
    # Execute table creation
    with engine.connect() as conn:
        conn.execute(text(create_table_sql))
        conn.commit()
    
    # Clean data before import
    # Handle NaN values based on data type
    print("Cleaning data and handling empty cells...")
    
    for original_col, clean_col in column_mapping.items():
        data_type = get_postgres_datatype(original_col, df[clean_col])
        
        if 'INTEGER' in data_type:
            # For integer columns, replace NaN with configured default
            default_val = FILE_CONFIG['default_values']['integer']
            if default_val is not None:
                df[clean_col] = df[clean_col].fillna(default_val)
                print(f"Column '{clean_col}' ({data_type}): replaced empty cells with {default_val}")
            else:
                print(f"Column '{clean_col}' ({data_type}): keeping empty cells as NULL")
        elif 'DECIMAL' in data_type:
            # For decimal columns, replace NaN with configured default
            default_val = FILE_CONFIG['default_values']['decimal']
            if default_val is not None:
                df[clean_col] = df[clean_col].fillna(default_val)
                print(f"Column '{clean_col}' ({data_type}): replaced empty cells with {default_val}")
            else:
                print(f"Column '{clean_col}' ({data_type}): keeping empty cells as NULL")
        elif 'DATE' in data_type or 'TIMESTAMP' in data_type:
            # For date columns, replace NaN with configured default (None = NULL)
            default_val = FILE_CONFIG['default_values']['date']
            if default_val is not None:
                df[clean_col] = df[clean_col].fillna(default_val)
                print(f"Column '{clean_col}' ({data_type}): replaced empty cells with {default_val}")
            else:
                print(f"Column '{clean_col}' ({data_type}): keeping empty cells as NULL")
        elif 'BOOLEAN' in data_type:
            # For boolean columns, replace NaN with configured default
            default_val = FILE_CONFIG['default_values']['boolean']
            if default_val is not None:
                df[clean_col] = df[clean_col].fillna(default_val)
                print(f"Column '{clean_col}' ({data_type}): replaced empty cells with {default_val}")
            else:
                print(f"Column '{clean_col}' ({data_type}): keeping empty cells as NULL")
        else:
            # For string/varchar columns, replace NaN with configured default
            default_val = FILE_CONFIG['default_values']['string']
            if default_val is not None:
                df[clean_col] = df[clean_col].fillna(default_val)
                print(f"Column '{clean_col}' ({data_type}): replaced empty cells with '{default_val}'")
            else:
                print(f"Column '{clean_col}' ({data_type}): keeping empty cells as NULL")
    
    print(f"Data cleaning completed. DataFrame shape: {df.shape}")
    
    # Validate and convert data types after cleaning
    print("Validating and converting data types...")
    for original_col, clean_col in column_mapping.items():
        data_type = get_postgres_datatype(original_col, df[clean_col])
        
        try:
            if 'INTEGER' in data_type:
                # Ensure the column can be converted to integers
                df[clean_col] = df[clean_col].astype('Int64')
            elif 'DECIMAL' in data_type:
                # Ensure the column can be converted to floats
                df[clean_col] = df[clean_col].astype('float64')
            elif 'BOOLEAN' in data_type:
                # Convert to boolean
                df[clean_col] = df[clean_col].astype('bool')
                
        except Exception as e:
            print(f"Warning: Could not convert column '{clean_col}' to {data_type}: {e}")
            print(f"Keeping original data type for column '{clean_col}'")
    
    print("Data type validation completed.")
    
    # Import data in chunks to avoid parameter limit error
    print(f"Importing {len(df)} rows to table {table_name}")
    chunk_size = FILE_CONFIG['chunk_size']
    
    total_chunks = (len(df) - 1) // chunk_size + 1
    successful_chunks = 0
    failed_rows = []
    
    for i in range(0, len(df), chunk_size):
        chunk = df.iloc[i:i+chunk_size]
        chunk_num = i // chunk_size + 1
        
        print(f"Processing chunk {chunk_num}/{total_chunks} ({len(chunk)} rows)")
        
        # Try multiple strategies for this chunk
        if import_chunk_with_fallback(chunk, table_name, engine, chunk_num):
            successful_chunks += 1
        else:
            print(f"Chunk {chunk_num} completely failed - adding rows to failed list")
            failed_rows.extend(chunk.index.tolist())
    
    print(f"\nImport completed! Successfully processed {successful_chunks}/{total_chunks} chunks.")
    if failed_rows:
        print(f"Failed to import {len(failed_rows)} rows. Row indices: {failed_rows[:10]}{'...' if len(failed_rows) > 10 else ''}")
    
    # Show sample data
    with engine.connect() as conn:
        result = conn.execute(text(f'SELECT * FROM "{table_name}" LIMIT 5'))
        sample_data = result.fetchall()
        print(f"\nSample data from {table_name}:")
        for row in sample_data:
            print(row)

    return column_mapping


def import_chunk_with_fallback(chunk, table_name, engine, chunk_num):
    """Try multiple strategies to import a chunk of data"""
    
    # Strategy 1: Try multi-insert method (fastest)
    try:
        chunk.to_sql(table_name, engine, if_exists='append', index=False, method='multi')
        return True
    except Exception as e:
        print(f"Multi-insert failed for chunk {chunk_num}: {str(e)[:100]}...")
    
    # Strategy 2: Try single insert method
    try:
        chunk.to_sql(table_name, engine, if_exists='append', index=False, method=None)
        return True
    except Exception as e:
        print(f"Single insert failed for chunk {chunk_num}: {str(e)[:100]}...")
    
    # Strategy 3: Try smaller sub-chunks
    sub_chunk_size = max(1, len(chunk) // 4)  # Quarter of original chunk size
    print(f"Trying smaller sub-chunks of size {sub_chunk_size}")
    
    successful_sub_chunks = 0
    for j in range(0, len(chunk), sub_chunk_size):
        sub_chunk = chunk.iloc[j:j+sub_chunk_size]
        try:
            sub_chunk.to_sql(table_name, engine, if_exists='append', index=False, method='multi')
            successful_sub_chunks += 1
        except Exception as e:
            print(f"Sub-chunk {j//sub_chunk_size + 1} failed: {str(e)[:50]}...")
            
            # Strategy 4: Try row-by-row as last resort
            print("Trying row-by-row insertion...")
            for idx, row in sub_chunk.iterrows():
                try:
                    row_df = pd.DataFrame([row])
                    row_df.to_sql(table_name, engine, if_exists='append', index=False, method=None)
                except Exception as row_e:
                    print(f"Row {idx} failed: {str(row_e)[:50]}...")
    
    return successful_sub_chunks > 0


def import_both_tables():
    """Import both the main data table and the description table"""
    print("=== IMPORTING BOTH DATA AND DESCRIPTION TABLES ===")
    
    # File paths
    data_file_path = FILE_CONFIG['data_file_path']
    description_file_path = FILE_CONFIG['description_file_path']
    
    # Table names with consistent naming convention
    data_table_name = FILE_CONFIG['table_name']
    description_table_name = f"{FILE_CONFIG['table_name']}_description"
    
    # Check if both files exist
    if not os.path.exists(data_file_path):
        print(f"Error: Data file not found: {data_file_path}")
        return False
    
    if not os.path.exists(description_file_path):
        print(f"Error: Description file not found: {description_file_path}")
        return False
    
    print(f"Data file: {data_file_path}")
    print(f"Description file: {description_file_path}")
    print(f"Data table: {data_table_name}")
    print(f"Description table: {description_table_name}")
    
    # Import description table first
    print("\n" + "="*60)
    print("STEP 1: IMPORTING DESCRIPTION TABLE")
    print("="*60)
    
    description_success = import_description_table(
        description_file_path, 
        description_table_name, 
        FILE_CONFIG['description_sheet_name']
    )
    
    if not description_success:
        print("Description table import failed!")
        return False
    
    # Import main data table
    print("\n" + "="*60)
    print("STEP 2: IMPORTING MAIN DATA TABLE")
    print("="*60)
    
    column_mapping = import_excel_to_postgres(
        data_file_path, 
        data_table_name, 
        FILE_CONFIG['data_sheet_name']
    )
    
    if not column_mapping:
        print("Main data table import failed!")
        return False
    
    print("\n" + "="*60)
    print("IMPORT SUMMARY")
    print("="*60)
    print(f"✓ Description table: {description_table_name}")
    print(f"✓ Main data table: {data_table_name}")
    print(f"✓ Total tables created: 2")
        
    print("\nColumn mapping (Original -> Cleaned):")
    for original, cleaned in column_mapping.items():
        print(f"'{original}' -> '{cleaned}'")
            
    print(f"\nYou can now query using:")
    print(f"-- Main data table")
    print(f"SELECT * FROM \"{data_table_name}\" LIMIT 10;")
    print(f"-- Description table")
    print(f"SELECT * FROM \"{description_table_name}\";")
    print(f"-- Check important/mandatory columns")
    print(f"SELECT column_name, common_name, is_important, must_have, mandatory_entity")
    print(f"FROM \"{description_table_name}\"")
    print(f"WHERE is_important = true OR must_have = true OR mandatory_entity = true;")
    
    return True


# Usage example
if __name__ == "__main__":
    try:
        success = import_both_tables()
        if success:
            print("\n🎉 All tables imported successfully!")
        else:
            print("\n❌ Import failed!")
        
    except Exception as e:
        print(f"Error: {e}")
        print("Consider:")
        print("1. Checking if both Excel files exist")
        print("2. Verifying database connection and permissions")
        print("3. Checking Excel file format and content")
        print("4. Reducing chunk_size in FILE_CONFIG if needed")