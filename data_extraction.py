import pandas as pd
import requests

class DataExtractor:
    """
    A class to extract and enrich sales data with user and weather information.
    """
    
    def __init__(self, sales_data_path, weather_api_key):
        """
        Initializes the DataExtractor with the path to sales data and a weather API key.
        
        :param sales_data_path: Path to the sales data CSV file.
        :param weather_api_key: API key for accessing weather data.
        """
        self.sales_data_path = sales_data_path
        self.weather_api_key = weather_api_key
        self.sales_data = None
        self.users_data = None  # Dictionary to hold user data from an API.

    def read_sales_data(self):
        """Reads sales data from a CSV file into a pandas DataFrame."""
        self.sales_data = pd.read_csv(self.sales_data_path)
        print("Sales data read successfully.")
    
    def fetch_all_users_data(self):
        """Fetches user data from a placeholder API and stores it in a dictionary."""
        response = requests.get('https://jsonplaceholder.typicode.com/users')
        if response.status_code == 200:
            self.users_data = {user['id']: user for user in response.json()}
            print("All users data fetched successfully.")
        else:
            print("Failed to fetch users data.")

    def fetch_weather_data(self, geo):
        """
        Fetches weather data for a given geographical location.
        
        :param geo: A dictionary containing the latitude and longitude.
        :return: Weather data as JSON or an error message if not found.
        """
        lat, lon = geo['lat'], geo['lng']
        response = requests.get(f'http://api.openweathermap.org/data/2.5/weather',
                                params={'lat': lat, 'lon': lon, 'appid': self.weather_api_key})
        print("W out")
        return response.json() if response.status_code == 200 else print("Weather data not found.")

    def enrich_sales_data(self):
        """Enriches sales data with user and weather information."""
        if self.sales_data is not None and self.users_data is not None:
            # Enrich with user data
            self.sales_data['user_data'] = self.sales_data['customer_id'].apply(lambda id: self.users_data.get(id))
            
            # Extract 'geo' data from user information
            self.sales_data['geo'] = self.sales_data['user_data'].apply(lambda user: user['address']['geo'] if user and 'address' in user and 'geo' in user['address'] else None)
            
            # Enrich with weather data if 'geo' data is available
            self.sales_data['weather_data'] = self.sales_data['geo'].apply(self.fetch_weather_data) if 'geo' in self.sales_data else None
            
            print("Sales data enriched with user and weather data.")
        else:
            raise ValueError("Sales data or users data is not loaded. Please load the data first.")

    def save_enriched_data_to_csv(self, output_path):
        """Saves the enriched sales data to a CSV file."""
        if self.sales_data is not None:
            self.sales_data.to_csv(output_path, index=False)
            print(f"Enriched sales data saved to {output_path}.")
        else:
            raise ValueError("Sales data is not available to save.")

# Example usage

# Initialize with the sales data path and weather API key
weather_api_key = ''
data_extractor = DataExtractor('sales_data.csv', weather_api_key)

# Sequentially execute the data extraction and enrichment processes
data_extractor.read_sales_data()
data_extractor.fetch_all_users_data()
data_extractor.enrich_sales_data()
data_extractor.save_enriched_data_to_csv('enriched_sales_data_2.csv')
