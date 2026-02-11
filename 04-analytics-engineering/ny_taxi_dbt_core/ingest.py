import duckdb
import argparse
from pathlib import Path
from dotenv import load_dotenv
import os

def setup_duckdb_with_gcs_hmac(con):
    """Configure DuckDB to read from GCS using HMAC keys"""
    
    # Load .env from setup directory
    env_path = Path(__file__).parent / 'setup' / '.env'
    load_dotenv(env_path)
    
    gcs_access_key = os.getenv('GCS_ACCESS_KEY_ID')
    gcs_secret_key = os.getenv('GCS_SECRET_ACCESS_KEY')
    
    if not gcs_access_key or not gcs_secret_key:
        raise ValueError(f"Missing GCS_ACCESS_KEY_ID or GCS_SECRET_ACCESS_KEY in {env_path}")
    
    print("Setting up DuckDB for GCS access with HMAC keys...")
    
    # Install and load httpfs extension
    con.execute("INSTALL httpfs")
    con.execute("LOAD httpfs")
    
    # Drop existing secret if it exists
    try:
        con.execute("DROP SECRET IF EXISTS gcs_secret")
    except:
        pass
    
    # Create PERSISTENT GCS secret (this will stay in the database!)
    con.execute(f"""
        CREATE PERSISTENT SECRET gcs_secret (
            TYPE GCS,
            KEY_ID '{gcs_access_key}',
            SECRET '{gcs_secret_key}'
        )
    """)
    
    print("✓ DuckDB configured for GCS access with PERSISTENT HMAC keys\n")

def create_table_from_gcs_csv(con, taxi_type, bucket_name, years):
    """Create a view that reads CSV.GZ files directly from GCS"""
    
    # Build GCS paths - use gs:// prefix (documentation shows this works)
    gcs_paths = []
    for year in years:
        pattern = f"gs://{bucket_name}/raw/{taxi_type}/{year}/*.csv.gz"
        gcs_paths.append(pattern)
    
    print(f"Creating table from GCS CSV.GZ files:")
    for path in gcs_paths:
        print(f"  {path}")
    
    # Create view that reads directly from GCS CSV.GZ files
    paths_str = "', '".join(gcs_paths)
    
    try:
        con.execute(f"""
            CREATE OR REPLACE VIEW prod.{taxi_type}_tripdata AS
            SELECT * FROM read_csv_auto(['{paths_str}'], union_by_name=true)
        """)
        
        print(f"✓ Created view: prod.{taxi_type}_tripdata\n")
        return True
    except Exception as e:
        print(f"✗ Error creating view for {taxi_type}: {e}\n")
        return False

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Create DuckDB tables that read CSV.GZ files directly from GCS (no local storage)',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Setup:
  1. Create HMAC keys:
     gcloud storage hmac create <your-database@your-gcp-project.iam.gserviceaccount.com --project=your-gcp-project>
  
  2. Add to setup/.env file:
     GCS_ACCESS_KEY_ID=GOOG1E...
     GCS_SECRET_ACCESS_KEY=wJalrXU...

Examples:
  python ingest.py
  python ingest.py --taxi-type green --year 2019
  python ingest.py --year 2019 2020
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
        '--db-file',
        type=str,
        default='taxi_rides_ny.duckdb',
        help='DuckDB database file path (default: taxi_rides_ny.duckdb)'
    )
    
    args = parser.parse_args()
    
    # Load environment variables from setup directory
    env_path = Path(__file__).parent / 'setup' / '.env'
    load_dotenv(env_path)
    bucket_name = os.getenv('GCP_BUCKET_NAME')
    
    if not bucket_name:
        raise ValueError(f"Missing GCP_BUCKET_NAME in {env_path}")
    
    print(f"\n{'='*60}")
    print(f"Configuration:")
    print(f"  Config location: {env_path}")
    print(f"  GCS Bucket: gs://{bucket_name}")
    print(f"  Taxi Types: {args.taxi_type}")
    print(f"  Years: {args.year}")
    print(f"  Database: {args.db_file}")
    print(f"  Local Storage: ~0 MB (data stays in GCS)")
    print(f"{'='*60}\n")
    
    # Connect to DuckDB (at project root)
    con = duckdb.connect(args.db_file)
    con.execute("CREATE SCHEMA IF NOT EXISTS prod")
    
    # Setup GCS access with HMAC keys
    try:
        setup_duckdb_with_gcs_hmac(con)
    except Exception as e:
        print(f"\n{'='*60}")
        print(f"❌ Could not set up GCS access")
        print(f"{'='*60}")
        print(f"Error: {e}\n")
        print("Please create HMAC keys:")
        print("\nThen add to setup/.env file:")
        print("  GCS_ACCESS_KEY_ID=your_access_key")
        print("  GCS_SECRET_ACCESS_KEY=your_secret_key")
        exit(1)
    
    success_count = 0
    for taxi_type in args.taxi_type:
        print(f"{'='*60}")
        print(f"Setting up {taxi_type} taxi data from GCS")
        print(f"{'='*60}")
        
        if create_table_from_gcs_csv(con, taxi_type, bucket_name, args.year):
            success_count += 1
            
            # Test query
            try:
                print(f"Testing query...")
                result = con.execute(f"SELECT COUNT(*) FROM prod.{taxi_type}_tripdata").fetchone()
                print(f"  ✓ Total rows in {taxi_type}_tripdata: {result[0]:,}\n")
                
            except Exception as e:
                print(f"  ⚠ Warning: Could not query data: {e}\n")
    
    con.close()
    
    print(f"{'='*60}")
    if success_count == len(args.taxi_type):
        print(f"✓ All {success_count} tables created successfully!")
    else:
        print(f"⚠ {success_count}/{len(args.taxi_type)} tables created")
    print(f"  Data location: gs://{bucket_name}/raw/")
    print(f"  DuckDB file: {args.db_file}")
    print(f"{'='*60}")