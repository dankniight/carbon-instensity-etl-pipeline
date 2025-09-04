import requests
import time
import json
from datetime import datetime


class CarbonIntensityETL:
    def __init__(self):
        self.base_url = "https://api.carbonintensity.org.uk"
        self.session = requests.Session()

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
                'timestamp': datetime.now().isoformat(),
                'from': intensity_data.get('from', 'N/A'),
                'to': intensity_data.get('to', 'N/A'),
                'forecast': intensity_data.get('intensity', {}).get('forecast', 'N/A'),
                'actual': intensity_data.get('intensity', {}).get('actual', 'N/A'),
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
                'timestamp': datetime.now().isoformat(),
                'from': generation_data.get('from', 'N/A'),
                'to': generation_data.get('to', 'N/A'),
                'generation_mix': mix_sorted
            }
        except Exception as e:
            print(f"Error transforming generation data: {e}")
            return None

    def transform_regional_data(self, data):
        """Transform regional data for display"""
        if not data:
            return None

        try:
            regions_data = data.get('data', [{}])[0].get('regions', [])
            
            # Sort by forecast intensity (descending)
            regions_sorted = sorted(regions_data, key=lambda x: x.get('intensity', {}).get('forecast', 0), reverse=True)
            
            return {
                'timestamp': datetime.now().isoformat(),
                'from': data.get('data', [{}])[0].get('from', 'N/A'),
                'to': data.get('data', [{}])[0].get('to', 'N/A'),
                'regions': regions_sorted[:5]  # Top 5 regions by intensity
            }
        except Exception as e:
            print(f"Error transforming regional data: {e}")
            return None

    def load_and_print_intensity(self, data):
        if not data:
            return

        print("\n" + "="*60)
        print("CARBON INTENSITY DATA")
        print("="*60)
        print(f"Timestamp: {data['timestamp']}")
        print(f"Period: {data['from']} to {data['to']}")
        print(f"Forecast Intensity: {data['forecast']} gCO2/kWh")
        print(f"Actual Intensity: {data['actual']} gCO2/kWh")
        print(f"Index: {data['index'].upper()}")

    def load_and_print_generation(self, data):
        if not data:
            return

        print("\n" + "="*60)
        print("GENERATION MIX DATA")
        print("="*60)
        print(f"Timestamp: {data['timestamp']}")
        print(f"Period: {data['from']} to {data['to']}")
        print("\nFuel Mix (sorted by contribution %):")
        for fuel in data['generation_mix']:
            print(f"  {fuel['fuel'].capitalize()}: {fuel['perc']:.1f}%")

    def load_and_print_regional(self, data):
        if not data:
            return

        print("\n" + "="*60)
        print("REGIONAL CARBON INTENSITY DATA (Top 5 Regions)")
        print("="*60)
        print(f"Timestamp: {data['timestamp']}")
        print(f"Period: {data['from']} to {data['to']}")
        print("\nRegions (sorted by forecast intensity):")
        for region in data['regions']:
            print(f"  {region['shortname']}: {region['intensity']['forecast']} gCO2/kWh ({region['intensity']['index']})")

    def run_etl_pipeline(self):
        print("Starting Carbon Intensity ETL Pipeline...")
        print("Press Ctrl+C to stop")
        
        try:
            while True:
                # Extract
                intensity_raw = self.extract_intensity_data()
                generation_raw = self.extract_generation_data()
                regional_raw = self.extract_regional_data()
                
                # Transform
                intensity_transformed = self.transform_intensity_data(intensity_raw)
                generation_transformed = self.transform_generation_data(generation_raw)
                regional_transformed = self.transform_regional_data(regional_raw)
                
                # Load
                self.load_and_print_intensity(intensity_transformed)
                self.load_and_print_generation(generation_transformed)
                self.load_and_print_regional(regional_transformed)

                print("\n" + "-"*60)
                print("Next update in 30 seconds...")
                print("-"*60)
                
                # Wait before next update
                time.sleep(30)
                
        except KeyboardInterrupt:
            print("\n\nETL Pipeline stopped by user.")
        except Exception as e:
            print(f"\nUnexpected error in ETL pipeline: {e}")


if __name__ == "__main__":
    etl = CarbonIntensityETL()
    etl.run_etl_pipeline()