import pandas as pd
import requests

class DataExtractor:
    def __init__(self, sales_data_path, weather_api_key):
        self.sales_data_path = sales_data_path
        self.weather_api_key = weather_api_key
        self.sales_data = None
        self.users_data = None  # Dictionary to hold user data

    def read_sales_data(self):
        self.sales_data = pd.read_csv(self.sales_data_path)
        print("Sales data read successfully.")
    
    def fetch_all_users_data(self):
        response = requests.get('https://jsonplaceholder.typicode.com/users')
        if response.status_code == 200:
            print("Fetching")
            self.users_data = {user['id']: user for user in response.json()}
            print("All users data fetched successfully.")
        else:
            print("Failed to fetch users data.")

    def fetch_weather_data(self, geo):
        lat, lon = geo['lat'], geo['lng']
        print("in fetch_weather_data")
        response = requests.get(f'http://api.openweathermap.org/data/2.5/weather',
                                params={'lat': lat, 'lon': lon, 'appid': self.weather_api_key})
        print("Out fetch_weather_data")
        return response.json() if response.status_code == 200 else print("Not found W")

    def enrich_sales_data(self):
        if self.sales_data is not None and self.users_data is not None:
            # Enrich with user data
            self.sales_data['user_data'] = self.sales_data['customer_id'].apply(lambda id: self.users_data.get(id))
            print(self.sales_data['user_data'])
            # Check if 'user_data' has 'address', then extract 'geo'
            self.sales_data['geo'] = self.sales_data['user_data'].apply(lambda user: user['address']['geo'] if user and 'address' in user and 'geo' in user['address'] else None)
            print(self.sales_data['geo'])
            # Enrich with weather data only if 'geo' data is available
            self.sales_data['weather_data'] = self.sales_data['geo'].apply(self.fetch_weather_data) if 'geo' in self.sales_data else None
            print(self.sales_data['weather_data'])
            print("Sales data enriched with user and weather data.")
        else:
            raise ValueError("Sales data or users data is not loaded. Please load the data first.")


    def save_enriched_data_to_csv(self, output_path):
        if self.sales_data is not None:
            self.sales_data.to_csv(output_path, index=False)
            print(f"Enriched sales data saved to {output_path}.")
        else:
            raise ValueError("Sales data is not available to save.")

    # ... rest of your methods ...

# Usage:

# Initialize with the sales data path and weather API key
weather_api_key = '523148460fe04e90d8129489306261d1'
data_extractor = DataExtractor('sales_data.csv', weather_api_key)

# Read the sales data from a CSV file
data_extractor.read_sales_data()

# Fetch all user data at once
data_extractor.fetch_all_users_data()

# Enrich the sales data with user and weather data
data_extractor.enrich_sales_data()

data_extractor.save_enriched_data_to_csv('enriched_sales_data.csv')

