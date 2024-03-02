
from extraction import DataExtractor
from transforamtion import Transformation
from database_manager import DatabaseManager

import json

# Define the path to your configuration file
config_file_path = 'path/to/config.json'


# Read the configuration file
with open(config_file_path, 'r') as config_file:
    config = json.load(config_file)

# Extract the configuration data
weather_api_key = config['weather_api_key']
sales_data_path = config['sales_data_path']

data_extractor = DataExtractor(sales_data_path, weather_api_key)

# Read the sales data
data_extractor.read_sales_data()

# Ensure that sales_data is not None
if data_extractor.sales_data is not None:
    # Fetch all user data at once
    data_extractor.fetch_all_users_data()

    # Enrich the sales data with user and weather data
    data_extractor.enrich_sales_data()
    data_extractor.save_enriched_data_to_csv("../outputs/data.csv")
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
    prepare_company_data_test.to_csv('../outputs/prepare_company_data_test.csv', index=False)
    
    prepare_orders_data_test = transformation.prepare_orders_data()
    print(prepare_orders_data_test)
    print("*******************************")

    prepare_weather_data_test = transformation.prepare_weather_data()
    print(prepare_weather_data_test)
    print("*******************************")
    
    # Calculate total sales per customer and save to CSV
    total_sales_per_customer = transformation.calculate_total_sales_per_customer()
    total_sales_per_customer.to_csv('../outputs/total_sales_per_customer.csv', index=True)

    # Determine the average order quantity per product and save to CSV
    average_order_quantity_per_product = transformation.determine_average_order_quantity_per_product()
    average_order_quantity_per_product.to_csv('../outputs/average_order_quantity_per_product.csv', index=True)

    # Identify top-selling products and save to CSV
    top_selling_products = transformation.identify_top_selling_products_or_customers()[0] 
    top_selling_products.to_csv('../outputs/top_selling_products.csv', index=True)

    # Analyze sales trends over time and save to CSV
    sales_trends = transformation.analyze_sales_trends_over_time()
    sales_trends.to_csv('../outputs/sales_trends.csv', index=True)

    # Include weather data in analysis and save to CSV
    weather_impact = transformation.include_weather_data_in_analysis()
    weather_impact.to_csv('../outputs/weather_impact.csv', index=True)
    
else:
    print("Failed to load sales data.")

db_manager = DatabaseManager('../outputs/database.db')

# Connect to the database
db_manager.connect()

# Create tables if they don't exist
db_manager.create_tables()



# Insert data (assuming you have dataframes for each table)
# Convert dataframes to tuples for insertion
customer_data = prepare_customer_data_test.to_records(index=False).tolist()
product_data = prepare_product_data_test.to_records(index=False).tolist()
orders_data = prepare_orders_data_test.to_records(index=False).tolist()
weather_data = prepare_weather_data_test.to_records(index=False).tolist()
company_data = prepare_company_data_test.to_records(index=False).tolist()

# Insert data into tables
# Downs are working 
db_manager.insert_weather_data(weather_data)
db_manager.insert_company_data(company_data)
db_manager.insert_customer_data(customer_data)

db_manager.insert_product_data(product_data)
db_manager.insert_orders_data(orders_data)


db_manager.show_company_table()
db_manager.show_orders_table()
db_manager.show_weather_record_table()
# The above code is attempting to print the row count of the "Orders" table using a function called
# `get_table_row_count` from a `db_manager` object.
print(db_manager.get_table_row_count("Orders"))
# Close the database connection


