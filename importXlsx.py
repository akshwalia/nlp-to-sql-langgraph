import pandas as pd
import sqlite3
from sqlalchemy import create_engine, text
import re
import os

# Database connection parameters - Updated for SQLite
DB_CONFIG = {
    'db_path': './data/PBTest.db',  # SQLite database file path
    # Legacy PostgreSQL parameters (not used)
    'host': 'localhost',
    'port': '5432',
    'database': 'PBTest',
    'user': 'admin123',
    'password': 'arjit'
}

FILE_CONFIG = {
    'data_file_path': 'GR_IT_Professional_Services_Spend2025.xlsx',
    'data_sheet_name': 'GR_IT_Professional_Services_Spe',
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

# Ensure database directory exists
os.makedirs(os.path.dirname(DB_CONFIG['db_path']), exist_ok=True)

# Create SQLite connection string
connection_string = f"sqlite:///{DB_CONFIG['db_path']}"

def parse_spend_range(spend_str):
    """
    Parse spend range strings and return lower and upper limits in millions.
    
    Examples:
    - '$20M - $30M' -> (20.0, 30.0)
    - '$2B - $3B' -> (2000.0, 3000.0)
    - '$1B+' -> (1000.0, None)
    - '<$1M' -> (0.0, 1.0)
    - '$500M - $1B' -> (500.0, 1000.0)
    
    Returns:
        tuple: (lower_limit_millions, upper_limit_millions) where None means unbounded
    """
    if pd.isna(spend_str) or spend_str == '' or str(spend_str).strip() == '':
        return (None, None)
    
    # Clean the string
    spend_str = str(spend_str).strip()
    
    # Helper function to convert value to millions
    def convert_to_millions(value_str):
        # Remove $ and any whitespace
        clean_val = re.sub(r'[\$\s]', '', value_str)
        
        if clean_val.endswith('B'):
            # Billions to millions
            num = float(clean_val[:-1])
            return num * 1000.0
        elif clean_val.endswith('M'):
            # Already in millions
            num = float(clean_val[:-1])
            return num
        elif clean_val.endswith('K'):
            # Thousands to millions
            num = float(clean_val[:-1])
            return num / 1000.0
        else:
            # Assume it's in raw dollars, convert to millions
            try:
                num = float(clean_val)
                return num / 1000000.0
            except:
                return None
    
    # Pattern 1: Range like "$20M - $30M" or "$2B - $3B"
    range_pattern = r'\$?([0-9.]+[MBK]?)\s*-\s*\$?([0-9.]+[MBK]?)'
    range_match = re.search(range_pattern, spend_str, re.IGNORECASE)
    
    if range_match:
        lower_str = range_match.group(1)
        upper_str = range_match.group(2)
        lower = convert_to_millions(lower_str)
        upper = convert_to_millions(upper_str)
        return (lower, upper)
    
    # Pattern 2: Greater than like "$1B+" 
    greater_pattern = r'\$?([0-9.]+[MBK]?)\+'
    greater_match = re.search(greater_pattern, spend_str, re.IGNORECASE)
    
    if greater_match:
        lower_str = greater_match.group(1)
        lower = convert_to_millions(lower_str)
        return (lower, None)  # No upper bound
    
    # Pattern 3: Less than like "<$1M"
    less_pattern = r'<\s*\$?([0-9.]+[MBK]?)'
    less_match = re.search(less_pattern, spend_str, re.IGNORECASE)
    
    if less_match:
        upper_str = less_match.group(1)
        upper = convert_to_millions(upper_str)
        return (0.0, upper)  # Lower bound is 0
    
    # Pattern 4: Single value like "$50M"
    single_pattern = r'\$?([0-9.]+[MBK]?)'
    single_match = re.search(single_pattern, spend_str, re.IGNORECASE)
    
    if single_match:
        value_str = single_match.group(1)
        value = convert_to_millions(value_str)
        return (value, value)  # Same lower and upper bound
    
    # If no pattern matches, return None
    return (None, None)

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

def get_sqlite_datatype(col_name, sample_data):
    """Determine appropriate SQLite data type based on column name and sample data"""
    col_lower = col_name.lower()
    
    # Check if it's clearly a string type from the original header
    if '|string' in col_name.lower():
        return 'TEXT'
    elif '|integer' in col_name.lower() or '|int' in col_name.lower():
        return 'INTEGER'
    elif '|decimal' in col_name.lower() or '|float' in col_name.lower():
        return 'REAL'
    elif '|date' in col_name.lower():
        return 'TEXT'  # SQLite stores dates as TEXT
    elif '|boolean' in col_name.lower() or '|bool' in col_name.lower():
        return 'INTEGER'  # SQLite uses INTEGER for booleans (0/1)
    elif '|spend' in col_name.lower():
        return 'SPEND'  # Special marker for spend columns
    
    # If no type annotation, infer from sample data
    # Remove NaN values to get a better sample for type detection
    non_null_sample = sample_data.dropna()
    
    if len(non_null_sample) == 0:
        # If all values are NaN, default to TEXT
        return 'TEXT'
    
    if pd.api.types.is_numeric_dtype(non_null_sample):
        if pd.api.types.is_integer_dtype(non_null_sample):
            return 'INTEGER'
        else:
            return 'REAL'
    elif pd.api.types.is_datetime64_any_dtype(non_null_sample):
        return 'TEXT'  # SQLite stores timestamps as TEXT
    else:
        return 'TEXT'

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
    
    # Create SQLAlchemy engine for SQLite
    engine = create_engine(
        connection_string,
        pool_pre_ping=True,
        connect_args={
            "timeout": FILE_CONFIG['timeout'],
            "check_same_thread": False  # Allow SQLite to be used across threads
        }
    )
    
    # Drop table if exists
    with engine.connect() as conn:
        conn.execute(text(f'DROP TABLE IF EXISTS "{description_table_name}"'))
        conn.commit()
    
    # Create description table
    create_table_sql = f'''
    CREATE TABLE "{description_table_name}" (
        "column_name" TEXT PRIMARY KEY,
        "common_name" TEXT,
        "description" TEXT,
        "is_important" INTEGER,
        "must_have" INTEGER,
        "mandatory_entity" INTEGER
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

def import_excel_to_sqlite(excel_file_path, table_name, sheet_name=0):
    """Import Excel file to SQLite with proper column names and data types"""
    
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
    
    # Create SQLAlchemy engine for SQLite
    engine = create_engine(
        connection_string,
        pool_pre_ping=True,
        connect_args={
            "timeout": FILE_CONFIG['timeout'],
            "check_same_thread": False  # Allow SQLite to be used across threads
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
        data_type = get_sqlite_datatype(original_col, df[clean_col])
        
        if data_type == 'SPEND':
            # For spend columns, create 3 columns: original, lower_limit, upper_limit
            column_definitions.append(f'    "{clean_col}" TEXT')  # Original string value
            column_definitions.append(f'    "{clean_col}_lower_limit" REAL')  # Lower limit in millions
            column_definitions.append(f'    "{clean_col}_upper_limit" REAL')  # Upper limit in millions
        else:
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
        data_type = get_sqlite_datatype(original_col, df[clean_col])
        
        if data_type == 'SPEND':
            # For spend columns, parse the values and create additional columns
            print(f"Processing spend column '{clean_col}'...")
            
            # Create new columns for lower and upper limits
            lower_col = f"{clean_col}_lower_limit"
            upper_col = f"{clean_col}_upper_limit"
            
            # Initialize the new columns
            df[lower_col] = None
            df[upper_col] = None
            
            # Process each value in the spend column
            for idx, spend_value in df[clean_col].items():
                lower, upper = parse_spend_range(spend_value)
                df.at[idx, lower_col] = lower
                df.at[idx, upper_col] = upper
            
            # Fill NaN values in original spend column with empty string
            df[clean_col] = df[clean_col].fillna('')
            
            print(f"Spend column '{clean_col}' processed:")
            print(f"  - Original values kept in '{clean_col}'")
            print(f"  - Lower limits stored in '{lower_col}' (millions)")
            print(f"  - Upper limits stored in '{upper_col}' (millions)")
            
            # Show some sample conversions
            sample_data = df[[clean_col, lower_col, upper_col]].dropna(subset=[clean_col]).head(3)
            if not sample_data.empty:
                print(f"  Sample conversions:")
                for _, row in sample_data.iterrows():
                    print(f"    '{row[clean_col]}' -> Lower: {row[lower_col]}, Upper: {row[upper_col]}")
        
        elif 'INTEGER' in data_type:
            # For integer columns, replace NaN with configured default
            default_val = FILE_CONFIG['default_values']['integer']
            if default_val is not None:
                df[clean_col] = df[clean_col].fillna(default_val)
                print(f"Column '{clean_col}' ({data_type}): replaced empty cells with {default_val}")
            else:
                print(f"Column '{clean_col}' ({data_type}): keeping empty cells as NULL")
        elif 'REAL' in data_type:
            # For real/decimal columns, replace NaN with configured default
            default_val = FILE_CONFIG['default_values']['decimal']
            if default_val is not None:
                df[clean_col] = df[clean_col].fillna(default_val)
                print(f"Column '{clean_col}' ({data_type}): replaced empty cells with {default_val}")
            else:
                print(f"Column '{clean_col}' ({data_type}): keeping empty cells as NULL")
        elif 'TEXT' in data_type and ('date' in original_col.lower() or 'timestamp' in original_col.lower()):
            # For date columns, replace NaN with configured default (None = NULL)
            default_val = FILE_CONFIG['default_values']['date']
            if default_val is not None:
                df[clean_col] = df[clean_col].fillna(default_val)
                print(f"Column '{clean_col}' ({data_type}): replaced empty cells with {default_val}")
            else:
                print(f"Column '{clean_col}' ({data_type}): keeping empty cells as NULL")
        elif 'INTEGER' in data_type and ('boolean' in original_col.lower() or 'bool' in original_col.lower()):
            # For boolean columns (stored as INTEGER in SQLite), replace NaN with configured default
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
        data_type = get_sqlite_datatype(original_col, df[clean_col])
        
        try:
            if data_type == 'SPEND':
                # For spend columns, validate the additional columns we created
                lower_col = f"{clean_col}_lower_limit"
                upper_col = f"{clean_col}_upper_limit"
                
                # Ensure spend limit columns are numeric
                if lower_col in df.columns:
                    df[lower_col] = df[lower_col].astype('float64')
                if upper_col in df.columns:
                    df[upper_col] = df[upper_col].astype('float64')
                
                # Ensure original spend column is string
                df[clean_col] = df[clean_col].astype('str')
                
            elif 'INTEGER' in data_type:
                # Ensure the column can be converted to integers
                df[clean_col] = df[clean_col].astype('Int64')
            elif 'REAL' in data_type:
                # Ensure the column can be converted to floats
                df[clean_col] = df[clean_col].astype('float64')
            elif 'INTEGER' in data_type and ('boolean' in original_col.lower() or 'bool' in original_col.lower()):
                # Convert to boolean using integers (SQLite uses 0/1 for booleans)
                df[clean_col] = df[clean_col].astype('bool').astype('int')
                
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
    
    column_mapping = import_excel_to_sqlite(
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