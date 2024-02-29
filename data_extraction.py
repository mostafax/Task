import pandas as pd
import requests
import numpy as np
from pandas import json_normalize
import ast
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
            print("Weather data Collected.")

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

#import pandas as pd
from datetime import datetime

class Transformation:
    def __init__(self, sales_data):
        """
        Initializes the Transformation with the sales data DataFrame.
        
        :param sales_data: A pandas DataFrame containing the sales data.
        """
        self.sales_data = sales_data
    def prepare_customer_data(self):
        customer_info = pd.json_normalize(self.sales_data['user_data'])
        customer_info.rename(columns={'id': 'customer_id'}, inplace=True)
        # Including address information directly in the customer data
        customer_info = customer_info[['customer_id', 'name', 'username', 'email', 'phone', 'website', 
                                       'address.street', 'address.suite', 'address.city', 'address.zipcode',
                                       'address.geo.lat', 'address.geo.lng']].drop_duplicates()
        customer_info.rename(columns={
            'address.street': 'street',
            'address.suite': 'suite',
            'address.city': 'city',
            'address.zipcode': 'zipcode',
            'address.geo.lat': 'geo_lat',
            'address.geo.lng': 'geo_lng'
        }, inplace=True)
        
        return customer_info

    def prepare_product_data(self):
        """
        Prepares product data for the Products table.
        """
        # Adjust this method to fit the product information available in your data
        product_info = self.sales_data[['product_id', 'price']].drop_duplicates()
        return product_info

    def prepare_orders_data(self):
        # Since OrderDetails is removed, we include product_id and quantity in Orders
        orders_info = self.sales_data[['order_id', 'customer_id', 'order_date', 'product_id', 'quantity']].drop_duplicates()
        return orders_info
    

    def prepare_company_data(self):
        # Extract company information and normalize it
        company_info = self.sales_data['user_data'].apply(lambda x: x['company'] if isinstance(x, dict) and 'company' in x else {})
        company_info = pd.json_normalize(company_info)
        company_info = company_info.drop_duplicates().reset_index(drop=True)
        return company_info


    def prepare_weather_data(self):
        """
        Prepares weather data for the WeatherRecord table including customer ID,
        matching weather_date with order_date.
        """
        # Convert 'weather_data' from string to dictionary if it's not already
        self.sales_data['weather_data'] = self.sales_data['weather_data'].apply(
            lambda x: ast.literal_eval(x) if isinstance(x, str) else x
        )
        
        # Initialize a DataFrame to store the normalized weather data
        weather_records = []

        # Normalize and extract the necessary data from the weather_data column
        for index, row in self.sales_data.iterrows():
            weather_data = row['weather_data']
            if isinstance(weather_data, dict):
                main_weather = weather_data.get('main', {})
                weather_description = weather_data.get('weather', [{}])[0]  # get the first item in 'weather' list
                
                # Append the extracted data to the weather_records list
                weather_records.append({
                    'customer_id': row['customer_id'],
                    'latitude': weather_data.get('coord', {}).get('lat'),
                    'longitude': weather_data.get('coord', {}).get('lon'),
                    'main': weather_description.get('main'),
                    'description': weather_description.get('description'),
                    'temperature': main_weather.get('temp'),
                    'humidity': main_weather.get('humidity'),
                    'weather_date': row['order_date']  # match with order_date
                })

        # Create a DataFrame from the records
        weather_info = pd.DataFrame(weather_records)

        # Convert 'order_date' to datetime and use it as 'weather_date'
        weather_info['weather_date'] = pd.to_datetime(weather_info['weather_date'])

        # # Drop any duplicates based on customer_id and weather_date to avoid one-to-many relationships
        # weather_info.drop_duplicates(subset=['customer_id', 'weather_date'], inplace=True)

        # Reset the index of the DataFrame
        weather_info.reset_index(drop=True, inplace=True)

        return weather_info




    def calculate_total_sales_per_customer(self):
        """Calculates total sales amount per customer."""
        self.sales_data['sales_amount'] = self.sales_data['quantity'] * self.sales_data['price']
        return self.sales_data.groupby('customer_id')['sales_amount'].sum()

    def determine_average_order_quantity_per_product(self):
        """Determines the average order quantity per product."""
        return self.sales_data.groupby('product_id')['quantity'].mean()

    def identify_top_selling_products_or_customers(self):
        """Identifies the top-selling products or customers."""
        product_sales = self.sales_data.groupby('product_id')['quantity'].sum().sort_values(ascending=False)
        customer_sales = self.sales_data.groupby('customer_id')['sales_amount'].sum().sort_values(ascending=False)
        return product_sales.head(1), customer_sales.head(1)

    def analyze_sales_trends_over_time(self):
        """Analyzes sales trends over time (monthly)."""
        self.sales_data['order_date'] = pd.to_datetime(self.sales_data['order_date'])
        # Use 'MS' for month start frequency to avoid FutureWarning
        return self.sales_data.set_index('order_date').resample('MS')['sales_amount'].sum()
    def parse_weather_data(self, weather_data):
        """Parses the weather data from string to dictionary and extracts the main weather."""
        try:
            # Convert the string to a dictionary if it's a string
            weather_dict = eval(weather_data) if isinstance(weather_data, str) else weather_data
            # Extract the 'main' weather condition
            main_weather = weather_dict['weather'][0]['main']
            return main_weather
        except (SyntaxError, TypeError, KeyError):
            # Return a sensible default or np.nan if there's an error
            return np.nan

    def include_weather_data_in_analysis(self):
        """Includes weather data in the analysis (average sales amount per weather condition)."""
        # Apply the parse_weather_data function to the 'weather_data' column
        self.sales_data['main_weather'] = self.sales_data['weather_data'].apply(self.parse_weather_data)
        
        # Ensure the 'sales_amount' column is calculated
        self.sales_data['sales_amount'] = self.sales_data['quantity'] * self.sales_data['price']
        
        # Perform the groupby operation
        return self.sales_data.groupby('main_weather')['sales_amount'].mean()

    def perform_transformations(self):
        """Executes all transformation steps."""
        total_sales_by_customer = self.calculate_total_sales_per_customer()
        avg_order_quantity_by_product = self.determine_average_order_quantity_per_product()
        top_selling_products, top_customers = self.identify_top_selling_products_or_customers()
        sales_trends = self.analyze_sales_trends_over_time()
        weather_impact = self.include_weather_data_in_analysis()

        return {
            "Total Sales by Customer": total_sales_by_customer,
            "Average Order Quantity by Product": avg_order_quantity_by_product,
            "Top Selling Products": top_selling_products,
            "Top Customers": top_customers,
            "Sales Trends": sales_trends,
            "Weather Impact on Sales": weather_impact
        }


# Initialize with the sales data path and weather API key
weather_api_key = 'cbbda7a0b45fcdb72e944964c45c80c4'
data_extractor = DataExtractor('sales_data.csv', weather_api_key)

# Read the sales data
data_extractor.read_sales_data()

# Ensure that sales_data is not None
if data_extractor.sales_data is not None:
    # Fetch all user data at once
    data_extractor.fetch_all_users_data()

    # Enrich the sales data with user and weather data
    data_extractor.enrich_sales_data()
    data_extractor.save_enriched_data_to_csv("data.csv")
    # Instantiate the Transformation class with the enriched sales data
    transformation = Transformation(data_extractor.sales_data)
    prepare_customer_data_test = transformation.prepare_customer_data()
    print(prepare_customer_data_test)
    print("*******************************")
    prepare_product_data_test = transformation.prepare_product_data()
    print(prepare_product_data_test)
    print("*******************************")

    prepare_company_data_test = transformation.prepare_company_data()
    print(prepare_company_data_test)
    print("*******************************")

    
    prepare_orders_data_test = transformation.prepare_orders_data()
    print(prepare_orders_data_test)
    print("*******************************")

    prepare_weather_data_test = transformation.prepare_weather_data()
    print(prepare_weather_data_test)
    print("*******************************")
    exit()
    # Calculate total sales per customer and save to CSV
    total_sales_per_customer = transformation.calculate_total_sales_per_customer()
    total_sales_per_customer.to_csv('total_sales_per_customer.csv', index=True)

    # Determine the average order quantity per product and save to CSV
    average_order_quantity_per_product = transformation.determine_average_order_quantity_per_product()
    average_order_quantity_per_product.to_csv('average_order_quantity_per_product.csv', index=True)

    # Identify top-selling products and save to CSV
    top_selling_products = transformation.identify_top_selling_products_or_customers()[0] # Assuming this returns a tuple (products, customers)
    top_selling_products.to_csv('top_selling_products.csv', index=True)

    # Analyze sales trends over time and save to CSV
    sales_trends = transformation.analyze_sales_trends_over_time()
    sales_trends.to_csv('sales_trends.csv', index=True)

    # Include weather data in analysis and save to CSV
    weather_impact = transformation.include_weather_data_in_analysis()
    weather_impact.to_csv('weather_impact.csv', index=True)
    
    
else:
    print("Failed to load sales data.")
