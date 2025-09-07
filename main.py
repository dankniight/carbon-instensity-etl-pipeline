import requests
import argparse
import json
import os
from datetime import datetime, timedelta
from supabase import create_client, Client
from dotenv import load_dotenv

load_dotenv()

class CarbonIntensityETL:
    def __init__(self, supabase_url: str, supabase_key: str):
        self.base_url = "https://api.carbonintensity.org.uk"
        self.session = requests.Session()

        # Initialize Supabase client
        self.supabase: Client = create_client(supabase_url, supabase_key)

    def extract_intensity_data(self):
        try:
            response = self.session.get(f"{self.base_url}/intensity")
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Error fetching intensity data: {e}")
            return None

    def extract_generation_data(self):
        try:
            response = self.session.get(f"{self.base_url}/generation")
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Error fetching generation data: {e}")
            return None

    def extract_regional_data(self):
        try:
            response = self.session.get(f"{self.base_url}/regional")
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Error fetching regional data: {e}")
            return None

    def transform_intensity_data(self, data):
        if not data:
            return None

        try:
            intensity_data = data.get('data', [{}])[0]
            return {
                'from_time': intensity_data.get('from', 'N/A'),
                'to_time': intensity_data.get('to', 'N/A'),
                'forecast': intensity_data.get('intensity', {}).get('forecast'),
                'actual': intensity_data.get('intensity', {}).get('actual'),
                'index': intensity_data.get('intensity', {}).get('index', 'N/A')
            }
        except Exception as e:
            print(f"Error transforming intensity data: {e}")
            return None

    def transform_generation_data(self, data):
        if not data:
            return None

        try:
            generation_data = data.get('data', {})
            mix = generation_data.get('generationmix', [])

            # Sort by percentage (descending)
            mix_sorted = sorted(mix, key=lambda x: x['perc'], reverse=True)

            return {
                'from_time': generation_data.get('from', 'N/A'),
                'to_time': generation_data.get('to', 'N/A'),
                'generation_mix': json.dumps(mix_sorted)  # Store as JSON string
            }
        except Exception as e:
            print(f"Error transforming generation data: {e}")
            return None

    def transform_regional_data(self, data):
        """Transform regional data for storage"""
        if not data:
            return None

        try:
            regions_data = data.get('data', [{}])[0].get('regions', [])

            # Sort by forecast intensity (descending)
            regions_sorted = sorted(regions_data, key=lambda x: x.get('intensity', {}).get('forecast', 0), reverse=True)

            return {
                'from_time': data.get('data', [{}])[0].get('from', 'N/A'),
                'to_time': data.get('data', [{}])[0].get('to', 'N/A'),
                'regions': regions_sorted[:10]  # Store top 10 regions as Python object
            }
        except Exception as e:
            print(f"Error transforming regional data: {e}")
            return None

    def load_intensity_data(self, data):
        """Save intensity data to Supabase"""
        if not data:
            return False

        try:
            result = self.supabase.table('carbon_intensity').insert(data).execute()
            print(f"✓ Intensity data saved to Supabase - Forecast: {data['forecast']} gCO2/kWh")
            return True
        except Exception as e:
            print(f"Error saving intensity data to Supabase: {e}")
            return False

    def load_generation_data(self, data):
        """Save generation mix data to Supabase"""
        if not data:
            return False

        try:
            result = self.supabase.table('generation_mix').insert(data).execute()
            print(f"✓ Generation mix data saved to Supabase")
            return True
        except Exception as e:
            print(f"Error saving generation data to Supabase: {e}")
            return False

    def load_regional_data(self, data):
        """Save regional data to Supabase"""
        if not data:
            return False

        try:
            result = self.supabase.table('regional_intensity').insert(data).execute()
            print(f"✓ Regional data saved to Supabase")
            return True
        except Exception as e:
            print(f"Error saving regional data to Supabase: {e}")
            return False

    def create_tables_if_not_exist(self):
        """Create tables in Supabase if they don't exist (requires SQL execution)"""
        print("Note: Ensure the following tables exist in your Supabase database:")
        print("""
        -- Carbon Intensity Table
        CREATE TABLE IF NOT EXISTS carbon_intensity (
            id SERIAL PRIMARY KEY,
            from_time TEXT,
            to_time TEXT,
            forecast INTEGER,
            actual INTEGER,
            index TEXT,
            created_at TIMESTAMPTZ DEFAULT NOW()
        );

        -- Generation Mix Table
        CREATE TABLE IF NOT EXISTS generation_mix (
            id SERIAL PRIMARY KEY,
            from_time TEXT,
            to_time TEXT,
            generation_mix JSONB,
            created_at TIMESTAMPTZ DEFAULT NOW()
        );

        -- Regional Intensity Table
        CREATE TABLE IF NOT EXISTS regional_intensity (
            id SERIAL PRIMARY KEY,
            from_time TEXT,
            to_time TEXT,
            regions JSONB,
            created_at TIMESTAMPTZ DEFAULT NOW()
        );
        """)

    def run_etl_pipeline(self):
        print("Starting Carbon Intensity ETL Pipeline with Supabase...")

        self.create_tables_if_not_exist()

        try:
            print(f"\n{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - Running ETL cycle...")

            # Extract
            intensity_raw = self.extract_intensity_data()
            generation_raw = self.extract_generation_data()
            regional_raw = self.extract_regional_data()

            # Transform
            intensity_transformed = self.transform_intensity_data(intensity_raw)
            generation_transformed = self.transform_generation_data(generation_raw)
            regional_transformed = self.transform_regional_data(regional_raw)

            # Load to Supabase
            intensity_saved = self.load_intensity_data(intensity_transformed)
            generation_saved = self.load_generation_data(generation_transformed)
            regional_saved = self.load_regional_data(regional_transformed)

        except Exception as e:
            print(f"\nUnexpected error in ETL pipeline: {e}")

    def run_cleanup_only(self):
        """Delete data older than 1 week from all tables"""
        
        # Calculate the cutoff date (1 week ago)
        cutoff_date = datetime.now() - timedelta(days=7)
        cutoff_iso = cutoff_date.isoformat()
        
        print(f"Cleaning up data older than {cutoff_date.strftime('%Y-%m-%d %H:%M:%S')}...")
        
        try:
            # Clean up carbon_intensity table
            result = self.supabase.table('carbon_intensity')\
                .delete()\
                .lt('created_at', cutoff_iso)\
                .execute()
            print(f"✓ Deleted {len(result.data) if result.data else 0} old records from carbon_intensity")
            
            # Clean up generation_mix table
            result = self.supabase.table('generation_mix')\
                .delete()\
                .lt('created_at', cutoff_iso)\
                .execute()
            print(f"✓ Deleted {len(result.data) if result.data else 0} old records from generation_mix")
            
            # Clean up regional_intensity table
            result = self.supabase.table('regional_intensity')\
                .delete()\
                .lt('created_at', cutoff_iso)\
                .execute()
            print(f"✓ Deleted {len(result.data) if result.data else 0} old records from regional_intensity")
            
            print("Cleanup process completed successfully.")
            
        except Exception as e:
            print(f"Error during cleanup process: {e}")


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='Carbon Intensity ETL Pipeline')
    parser.add_argument('--cleanup-only', action='store_true',
                        help='Run only the cleanup process (delete old data)')
    args = parser.parse_args()

    SUPABASE_URL = os.getenv('SUPABASE_URL')
    SUPABASE_KEY = os.getenv('SUPABASE_KEY')

    if not SUPABASE_URL or not SUPABASE_KEY:
        print("SUPABASE_URL and/or SUPABASE_SERVICE_KEY environment variables missing")
        exit(1)

    etl = CarbonIntensityETL(SUPABASE_URL, SUPABASE_KEY)

    if args.cleanup_only:
        etl.run_cleanup_only()
    else:
        etl.run_etl_pipeline()