import pandas as pd
import requests

class DataExtractor:
    def __init__(self, sales_data_path, weather_api_key):
        self.sales_data_path = sales_data_path
        self.weather_api_key = weather_api_key
        self.sales_data = None

    def read_sales_data(self):
        self.sales_data = pd.read_csv(self.sales_data_path)
        print("Done reading CSV")
    
    def fetch_user_data(self, user_id):
        response = requests.get(f'https://jsonplaceholder.typicode.com/users/{user_id}')
        print(response.json())
        return response.json() if response.status_code == 200 else print("Not Found")

    def fetch_weather_data(self, location):
        print("Freezed here")
        response = requests.get(f'http://api.openweathermap.org/data/2.5/weather?q={location}&appid={self.weather_api_key}')
        print(response.json())
        print("HHHHHHH")
        return response.json() if response.status_code == 200 else print("Weather_data")

    def enrich_sales_data(self):
        print("***********************************************")
        if self.sales_data is not None:
            self.sales_data['user_data'] = self.sales_data['customer_id'].apply(self.fetch_user_data)
            self.sales_data['user_data']
            self.sales_data['weather_data'] = self.sales_data['location'].apply(self.fetch_weather_data)
            print(self.sales_data['weather_data'])
        else:
            raise ValueError("Sales data is not loaded. Please load the data first.")

    def aggregate_data(self):
        # Implement any aggregation logic here
        # For example, summing sales amount per customer:
        return self.sales_data.groupby('customer_id')['sale_amount'].sum()

    def save_to_database(self):
        # Implement the logic to save to a database
        pass



weather_api_key = '523148460fe04e90d8129489306261d1'
data_extractor = DataExtractor('sales_data.csv', weather_api_key)

# Read the sales data
data_extractor.read_sales_data()

# Enrich the sales data with user and weather data
data_extractor.enrich_sales_data()

# Perform data aggregation
aggregated_data = data_extractor.aggregate_data()
print(aggregated_data)
# Optionally save the enriched and aggregated data to a database
#data_extractor.save_to_database()


