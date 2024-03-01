
import pandas as pd
import ast

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
        # Including address and company information directly in the customer data
        customer_info = customer_info[['customer_id', 'name', 'username', 'email', 'phone', 'website', 
                                       'address.street', 'address.suite', 'address.city', 'address.zipcode',
                                       'address.geo.lat', 'address.geo.lng', 'company.name']].drop_duplicates()
        customer_info.rename(columns={
            'address.street': 'street',
            'address.suite': 'suite',
            'address.city': 'city',
            'address.zipcode': 'zipcode',
            'address.geo.lat': 'geo_lat',
            'address.geo.lng': 'geo_lng',
            'company.name': 'company_name'  # Rename company.name to company_name
        }, inplace=True)
        
        return customer_info

    def prepare_product_data(self):
        """
        Prepares product data for the Products table, ensuring no duplicate product_ids are included,
        even if they have different prices. Keeps the first occurrence of each product_id.
        """
        product_info = self.sales_data.drop_duplicates(subset=['product_id'], keep='first')
            
            # Select only the 'product_id' and 'price' columns
        product_info = product_info[['product_id', 'price']]
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

