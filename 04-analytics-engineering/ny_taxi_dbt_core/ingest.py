import psycopg2
from psycopg2 import sql
import argparse
from pathlib import Path
from dotenv import load_dotenv
import os
import pandas as pd
import requests
import gzip
import io
from typing import List
from tqdm import tqdm

def setup_postgres_connection(db_config):
    """Create connection to PostgreSQL"""
    try:
        conn = psycopg2.connect(
            host=db_config['host'],
            port=db_config['port'],
            database=db_config['database'],
            user=db_config['user'],
            password=db_config['password']
        )
        conn.autocommit = False
        print("✓ Connected to PostgreSQL\n")
        return conn
    except Exception as e:
        raise Exception(f"Failed to connect to PostgreSQL: {e}")

def create_schema(conn):
    """Create raw, dev, and prod schemas if they don't exist"""
    try:
        with conn.cursor() as cur:
            cur.execute("CREATE SCHEMA IF NOT EXISTS raw")
            cur.execute("CREATE SCHEMA IF NOT EXISTS dev")
            cur.execute("CREATE SCHEMA IF NOT EXISTS prod")
            conn.commit()
        print("✓ Created schemas: raw, dev, prod\n")
    except Exception as e:
        conn.rollback()
        print(f"⚠ Warning creating schemas: {e}\n")

def generate_urls(taxi_type: str, years: List[int], months: List[int] = None) -> List[dict]:
    """Generate GitHub release URLs for given taxi type, years, and months"""
    base_url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download"
    urls = []
    
    # Default to all months if not specified
    if months is None:
        months = list(range(1, 13))
    
    for year in years:
        for month in months:
            url = f"{base_url}/{taxi_type}/{taxi_type}_tripdata_{year}-{month:02d}.csv.gz"
            urls.append({
                'url': url,
                'year': year,
                'month': month,
                'filename': f"{taxi_type}_tripdata_{year}-{month:02d}.csv.gz"
            })
    
    return urls

def check_url_exists(url: str) -> bool:
    """Check if URL exists without downloading"""
    try:
        response = requests.head(url, allow_redirects=True, timeout=30)
        return response.status_code == 200
    except:
        return False

def read_csv_from_url(url: str, filename: str, chunksize: int = 100000, max_retries: int = 3):
    """Download CSV.GZ file from URL and return as pandas DataFrame chunks"""
    
    for attempt in range(max_retries):
        try:
            # Get file size first
            response = requests.head(url, allow_redirects=True, timeout=30)
            file_size = int(response.headers.get('content-length', 0))
            
            if attempt > 0:
                print(f"  Retry {attempt}/{max_retries - 1} - Downloading {filename} ({file_size / 1024 / 1024:.1f} MB)...")
            else:
                print(f"  Downloading {filename} ({file_size / 1024 / 1024:.1f} MB)...")
            
            # Download with progress bar - increased timeout for large files
            # Use a longer timeout and connection timeout
            response = requests.get(
                url, 
                stream=True, 
                timeout=(30, 600)  # (connection timeout, read timeout) = 30s to connect, 10min to read
            )
            response.raise_for_status()
            
            content = io.BytesIO()
            
            with tqdm(total=file_size, unit='B', unit_scale=True, unit_divisor=1024, 
                      desc="  Download", leave=False) as pbar:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        content.write(chunk)
                        pbar.update(len(chunk))
            
            content.seek(0)
            
            # Decompress and read CSV
            print(f"  Decompressing and reading CSV...")
            with gzip.open(content, 'rt') as f:
                # Read in chunks to handle large files
                for chunk in pd.read_csv(f, chunksize=chunksize):
                    yield chunk
            
            # If we got here, download was successful
            return
                    
        except requests.exceptions.Timeout as e:
            if attempt < max_retries - 1:
                print(f"  ⚠ Timeout downloading {filename}, retrying...")
                continue
            else:
                raise Exception(f"Failed to download {url} after {max_retries} attempts: {e}")
        
        except requests.exceptions.RequestException as e:
            if attempt < max_retries - 1:
                print(f"  ⚠ Error downloading {filename}, retrying...")
                continue
            else:
                raise Exception(f"Failed to download {url} after {max_retries} attempts: {e}")

def create_table_from_first_chunk(conn, table_name: str, df: pd.DataFrame, drop_first: bool = False):
    """Create table based on first chunk's schema
    
    Args:
        conn: PostgreSQL connection
        table_name: Full table name (schema.table)
        df: DataFrame with schema to use
        drop_first: If True, drop existing table before creating (default: False)
    """
    
    # Map pandas dtypes to PostgreSQL types
    dtype_mapping = {
        'int64': 'BIGINT',
        'float64': 'DOUBLE PRECISION',
        'object': 'TEXT',
        'bool': 'BOOLEAN',
        'datetime64[ns]': 'TIMESTAMP',
    }
    
    columns = []
    for col, dtype in df.dtypes.items():
        pg_type = dtype_mapping.get(str(dtype), 'TEXT')
        # Clean column names (remove spaces, special chars)
        clean_col = col.replace(' ', '_').replace('-', '_').lower()
        columns.append(f'"{clean_col}" {pg_type}')
    
    # Split schema and table name
    schema, table = table_name.split('.')
    
    with conn.cursor() as cur:
        # Only drop if explicitly requested
        if drop_first:
            drop_sql = f"DROP TABLE IF EXISTS {schema}.{table}"
            cur.execute(drop_sql)
            print(f"  ✓ Dropped existing table: {table_name}")
        
        # Create table only if it doesn't exist
        create_sql = f"""
            CREATE TABLE IF NOT EXISTS {schema}.{table} (
                {', '.join(columns)}
            )
        """
        cur.execute(create_sql)
        conn.commit()
    
    print(f"  ✓ Table ready: {table_name}")
    print(f"    Columns: {len(columns)}")

def insert_dataframe_to_postgres(conn, table_name: str, df: pd.DataFrame) -> int:
    """Insert DataFrame into PostgreSQL table using COPY"""
    
    # Clean column names
    df.columns = [col.replace(' ', '_').replace('-', '_').lower() for col in df.columns]
    
    # Fix float columns that represent integers (e.g., 1.0 -> 1)
    # This prevents "invalid input syntax for type bigint" errors
    for col in df.select_dtypes(include=['float64']).columns:
        # Check if all non-null values are whole numbers
        if df[col].notna().any():
            non_null = df[col].dropna()
            if (non_null % 1 == 0).all():
                # Convert to Int64 (nullable integer type)
                df[col] = df[col].astype('Int64')
    
    # Create a CSV buffer with proper NULL handling
    buffer = io.StringIO()
    df.to_csv(buffer, index=False, header=False, na_rep='\\N')
    buffer.seek(0)
    
    # Split schema and table name for COPY command
    schema, table = table_name.split('.')
    
    with conn.cursor() as cur:
        # Build the COPY query with NULL handling
        columns_str = ', '.join([f'"{col}"' for col in df.columns])
        copy_query = f"COPY {schema}.{table} ({columns_str}) FROM STDIN WITH (FORMAT CSV, NULL '\\N')"
        
        cur.copy_expert(copy_query, buffer)
    
    return len(df)

def load_taxi_data_from_github(conn, taxi_type: str, years: List[int], months: List[int] = None, replace: bool = False):
    """Load taxi data from GitHub releases into PostgreSQL"""
    
    table_name = f"raw.{taxi_type}_tripdata"  # Changed from prod to raw
    
    print(f"Loading {taxi_type} taxi data from GitHub:")
    print(f"  Years: {years}")
    if months:
        print(f"  Months: {months}")
    else:
        print(f"  Months: All (1-12)")
    print(f"  Mode: {'REPLACE (delete existing data)' if replace else 'APPEND (keep existing data)'}")
    print(f"  Target: {table_name}\n")
    
    try:
        # Generate URLs
        urls = generate_urls(taxi_type, years, months)
        print(f"Checking {len(urls)} potential files...\n")
        
        # Filter to only existing URLs
        existing_urls = []
        for url_info in tqdm(urls, desc="Checking URLs", unit="url"):
            if check_url_exists(url_info['url']):
                existing_urls.append(url_info)
        
        if not existing_urls:
            print(f"⚠ No files found for {taxi_type} taxi in years {years}\n")
            return False
        
        print(f"\nFound {len(existing_urls)} files to download\n")
        
        total_rows = 0
        table_created = False
        
        # Progress bar for files
        with tqdm(total=len(existing_urls), desc=f"Processing {taxi_type} files", unit="file") as file_pbar:
            for i, url_info in enumerate(existing_urls, 1):
                url = url_info['url']
                filename = url_info['filename']
                
                file_pbar.set_description(f"Processing {taxi_type} files [{i}/{len(existing_urls)}]")
                
                try:
                    chunk_count = 0
                    file_rows = 0
                    
                    for chunk in read_csv_from_url(url, filename, chunksize=100000):
                        chunk_count += 1
                        
                        # Create table from first chunk of first file
                        if not table_created:
                            create_table_from_first_chunk(conn, table_name, chunk, drop_first=replace)
                            table_created = True
                        
                        # Insert chunk
                        rows_inserted = insert_dataframe_to_postgres(conn, table_name, chunk)
                        file_rows += rows_inserted
                        total_rows += rows_inserted
                    
                    conn.commit()
                    file_pbar.set_postfix({
                        "total_rows": f"{total_rows:,}", 
                        "file_rows": f"{file_rows:,}"
                    })
                    file_pbar.update(1)
                    
                except Exception as e:
                    conn.rollback()
                    tqdm.write(f"  ✗ Error processing {filename}: {e}")
                    file_pbar.update(1)
                    continue
        
        print(f"\n✓ Successfully loaded {total_rows:,} rows into {table_name}")
        return True
        
    except Exception as e:
        conn.rollback()
        print(f"✗ Error loading {taxi_type} data: {e}\n")
        return False

def verify_data(conn, taxi_type: str):
    """Verify loaded data"""
    table_name = f"raw.{taxi_type}_tripdata"  # Changed from prod to raw
    schema, table = table_name.split('.')
    
    try:
        with conn.cursor() as cur:
            # Count rows
            cur.execute(f"SELECT COUNT(*) FROM {schema}.{table}")
            count = cur.fetchone()[0]
            
            # Get sample
            cur.execute(f"SELECT * FROM {schema}.{table} LIMIT 5")
            sample = cur.fetchall()
            
            print(f"\n{'='*60}")
            print(f"Table: {table_name}")
            print(f"  Total rows: {count:,}")
            print(f"  Sample rows fetched: {len(sample)}")
            print(f"{'='*60}\n")
            
    except Exception as e:
        print(f"⚠ Could not verify data: {e}\n")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Load CSV.GZ files from GitHub releases into PostgreSQL',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Data Source:
  GitHub: https://github.com/DataTalksClub/nyc-tlc-data/releases

Setup:
  1. Start PostgreSQL with docker-compose:
     docker-compose up -d
  
  2. Add to setup/.env file (optional for PostgreSQL config):
     POSTGRES_HOST=localhost
     POSTGRES_PORT=5432
     POSTGRES_DB=nyc_taxi
     POSTGRES_USER=root
     POSTGRES_PASSWORD=root

Examples:
  # Load all months for 2019 and 2020 (append mode - keeps existing data)
  python ingest.py --taxi-type green --year 2019 2020
  
  # Load only January and February (append to existing table)
  python ingest.py --taxi-type green --year 2019 --month 1 2
  
  # Load March and ADD to existing Jan-Feb data
  python ingest.py --taxi-type green --year 2019 --month 3
  
  # Load January through June (replace mode - deletes existing data first)
  python ingest.py --taxi-type green --year 2019 2020 --month-range 1 6 --replace

  # Load January through June for yellow (append mode)
  python ingest.py --taxi-type yellow --year 2019 --month-range 1 6
  
  # Load specific months and replace existing data
  python ingest.py --taxi-type yellow green --year 2019 2020 --month 1 6 12 --replace
  
  # Default: both taxi types, years 2019-2020, all months, append mode
  python ingest.py
        """
    )
    
    parser.add_argument(
        '--taxi-type',
        nargs='+',
        choices=['yellow', 'green'],
        default=['yellow', 'green'],
        help='Taxi type(s) to process (default: yellow green)'
    )
    
    parser.add_argument(
        '--year',
        nargs='+',
        type=int,
        default=[2019, 2020],
        help='Year(s) to process (default: 2019 2020)'
    )
    
    parser.add_argument(
        '--month',
        nargs='+',
        type=int,
        choices=range(1, 13),
        metavar='MONTH',
        help='Specific month(s) to process (1-12). Example: --month 1 2 3'
    )
    
    parser.add_argument(
        '--month-range',
        nargs=2,
        type=int,
        metavar=('START', 'END'),
        help='Month range to process (inclusive). Example: --month-range 1 6 for Jan-Jun'
    )
    
    parser.add_argument(
        '--replace',
        action='store_true',
        help='Replace existing table data (default: append to existing table)'
    )
    
    args = parser.parse_args()
    
    # Validate month-range if provided
    if args.month_range:
        if not (1 <= args.month_range[0] <= 12 and 1 <= args.month_range[1] <= 12):
            parser.error("Month range must be between 1 and 12")
        if args.month_range[0] > args.month_range[1]:
            parser.error("Start month must be less than or equal to end month")
    
    # Can't use both --month and --month-range
    if args.month and args.month_range:
        parser.error("Cannot use both --month and --month-range")
    
    # Determine which months to process
    months_to_process = None
    if args.month:
        months_to_process = args.month
    elif args.month_range:
        months_to_process = list(range(args.month_range[0], args.month_range[1] + 1))
    
    # Load environment variables (optional)
    env_path = Path(__file__).parent / 'setup' / '.env'
    if env_path.exists():
        load_dotenv(env_path)
    
    # PostgreSQL configuration
    db_config = {
        'host': os.getenv('POSTGRES_HOST', 'localhost'),
        'port': int(os.getenv('POSTGRES_PORT', 5432)),
        'database': os.getenv('POSTGRES_DB', 'nyc_taxi'),
        'user': os.getenv('POSTGRES_USER', 'root'),
        'password': os.getenv('POSTGRES_PASSWORD', 'root')
    }
    
    print(f"\n{'='*60}")
    print(f"GitHub to PostgreSQL Data Loader")
    print(f"{'='*60}")
    print(f"Configuration:")
    print(f"  Data Source: GitHub DataTalksClub/nyc-tlc-data")
    print(f"  PostgreSQL: {db_config['user']}@{db_config['host']}:{db_config['port']}/{db_config['database']}")
    print(f"  Taxi Types: {args.taxi_type}")
    print(f"  Years: {args.year}")
    if months_to_process:
        print(f"  Months: {months_to_process}")
    else:
        print(f"  Months: All (1-12)")
    print(f"{'='*60}\n")
    
    # Connect to PostgreSQL
    try:
        conn = setup_postgres_connection(db_config)
        create_schema(conn)
    except Exception as e:
        print(f"\n{'='*60}")
        print(f"❌ Could not connect to PostgreSQL")
        print(f"{'='*60}")
        print(f"Error: {e}\n")
        print("Make sure PostgreSQL is running:")
        print("  docker-compose up -d")
        exit(1)
    
    success_count = 0
    for taxi_type in args.taxi_type:
        print(f"\n{'='*60}")
        print(f"Processing {taxi_type} taxi data")
        print(f"{'='*60}\n")
        
        if load_taxi_data_from_github(conn, taxi_type, args.year, months_to_process, args.replace):
            success_count += 1
            verify_data(conn, taxi_type)
    
    conn.close()
    
    print(f"\n{'='*60}")
    print(f"Summary")
    print(f"{'='*60}")
    if success_count == len(args.taxi_type):
        print(f"✓ All {success_count} table(s) loaded successfully!")
    else:
        print(f"⚠ {success_count}/{len(args.taxi_type)} table(s) loaded")
    print(f"\nAccess your data:")
    print(f"  pgAdmin: http://localhost:5050")
    print(f"  psql: psql -h localhost -U {db_config['user']} -d {db_config['database']}")
    print(f"{'='*60}\n")