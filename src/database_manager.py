
import sqlite3
import pandas as pd
from pandas import json_normalize
import ast

class DatabaseManager:
    def __init__(self, db_name):
        self.db_name = db_name
        self.conn = None
        self.cursor = None

    def connect(self):
        """
        Creates a database connection.
        """
        self.conn = sqlite3.connect(self.db_name)
        self.cursor = self.conn.cursor()

    def create_tables(self):
        """
        Creates tables in the database according to the specified schema.
        """
        commands = [
            '''CREATE TABLE IF NOT EXISTS Companies (
                company_id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                catch_phrase TEXT,
                bs TEXT
            )''',
            '''CREATE TABLE IF NOT EXISTS Customers (
                customer_id INTEGER PRIMARY KEY,
                company_id INTEGER,
                name TEXT NOT NULL,
                username TEXT,
                email TEXT,
                phone TEXT,
                website TEXT,
                street TEXT,
                suite TEXT,
                city TEXT,
                zipcode TEXT,
                geo_lat REAL,
                geo_lng REAL,
                FOREIGN KEY (company_id) REFERENCES Companies (company_id)
            )''',
            '''CREATE TABLE IF NOT EXISTS Products (
                product_id INTEGER PRIMARY KEY,
                price REAL
            )''',
            '''CREATE TABLE IF NOT EXISTS Orders (
                order_id INTEGER PRIMARY KEY,
                customer_id INTEGER,
                order_date DATE,
                product_id INTEGER,
                quantity INTEGER,
                FOREIGN KEY (customer_id) REFERENCES Customers (customer_id),
                FOREIGN KEY (product_id) REFERENCES Products (product_id)
            )''',
            '''CREATE TABLE IF NOT EXISTS WeatherRecord (
                weather_id INTEGER PRIMARY KEY AUTOINCREMENT,
                customer_id INTEGER,
                latitude REAL,
                longitude REAL,
                main_weather TEXT,
                description TEXT,
                temperature REAL,
                humidity INTEGER,
                weather_date DATE,
                FOREIGN KEY (customer_id) REFERENCES Customers (customer_id)
            )'''
        ]
        for command in commands:
            self.cursor.execute(command)
        self.conn.commit()

    def close(self):
        """
        Closes the database connection.
        """
        self.conn.close()


    def insert_company_data(self, data):
        """
        Inserts company data into the Companies table.
        
        :param data: A list of tuples containing company data to be inserted.
                     Each tuple should contain (name, catch_phrase, bs).
        """
        query = '''INSERT INTO Companies (name, catch_phrase, bs)
                   VALUES (?, ?, ?)'''
        self.cursor.executemany(query, data)
        self.conn.commit()

    def insert_customer_data(self, data):
        """
        Inserts customer data into the Customers table, fetching company_id based on company name.
        
        :param data: A list of tuples containing customer data to be inserted, where one of the fields is expected to be 'company_name'.
        """
        for customer in data:
            # Extract company_name from the customer tuple
            # Assume company_name is at a specific index, e.g., the second item in the tuple for this example
            company_name = customer[-1]
            # Fetch company_id based on company_name
            self.cursor.execute("SELECT company_id FROM Companies WHERE name = ?", (company_name,))
            company_row = self.cursor.fetchone()
            if company_row:
                company_id = company_row[0]
                # Replace company_name with company_id in the customer tuple
                # Assuming company_name is the second element, and customer_id is the first element in the tuple
                customer_with_id = (customer[0],) + (company_id,) + customer[1:-1]  # Exclude the last item (company_name)
                
                # Prepare the SQL query without company_name
                query = '''INSERT INTO Customers (customer_id, company_id, name, username, email, phone, website, street, suite, city, zipcode, geo_lat, geo_lng)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'''
                self.cursor.execute(query, customer_with_id)
            else:
                print(f"Company not found for name: {company_name}")
                continue  # Skip this customer or handle the case as needed
            
        self.conn.commit()


    def insert_product_data(self, data):
        """
        Inserts product data into the Products table.
        """
        query = '''INSERT INTO Products (product_id, price) VALUES (?, ?)'''
        self.cursor.executemany(query, data)
        self.conn.commit()

    def insert_orders_data(self, data):
        """
        Inserts orders data into the Orders table.
        """
        query = '''INSERT  OR IGNORE INTO Orders (order_id, customer_id, order_date, product_id, quantity) VALUES (?, ?, ?, ?, ?)'''
        self.cursor.executemany(query, data)
        self.conn.commit()

    def insert_weather_data(self, data):
        """
        Inserts weather data into the WeatherRecord table.
        """
        query = '''INSERT INTO WeatherRecord (customer_id, latitude, longitude, main_weather, description, temperature, humidity, weather_date)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)'''
        self.cursor.executemany(query, data)
        self.conn.commit()
    def show_company_table(self):
        """
        Retrieves and prints all rows from the Companies table.
        """
        query = "SELECT * FROM Companies"
        try:
            self.cursor.execute(query)
            companies = self.cursor.fetchall()
            if companies:
                print("Company Table Contents:")
                for company in companies:
                    print(company)  # Each 'company' is a tuple representing a row from the Companies table.
            else:
                print("The Companies table is empty.")
        except sqlite3.Error as e:
            print(f"Error fetching data from Companies table: {e}")

    def show_orders_table(self):
        """
        Retrieves and prints all rows from the Orders table.
        """
        query = "SELECT * FROM Orders"
        try:
            self.cursor.execute(query)
            orders = self.cursor.fetchall()
            if orders:
                print("Orders Table Contents:")
                for order in orders:
                    print(order)  # Each 'order' is a tuple representing a row from the Orders table.
            else:
                print("The Orders table is empty.")
        except sqlite3.Error as e:
            print(f"Error fetching data from Orders table: {e}")



    def get_table_row_count(self, table_name):
        """
        Returns the number of rows in the specified table.
        """
        query = f"SELECT COUNT(*) FROM {table_name}"
        try:
            self.cursor.execute(query)
            count = self.cursor.fetchone()[0]
            return count
        except sqlite3.Error as e:
            print(f"Error fetching row count from {table_name} table: {e}")
            return None

    def show_weather_record_table(self):
        """
        Retrieves and prints all rows from the WeatherRecord table.
        """
        query = "SELECT * FROM WeatherRecord"
        try:
            self.cursor.execute(query)
            weather_records = self.cursor.fetchall()
            if weather_records:
                print("WeatherRecord Table Contents:")
                for record in weather_records:
                    # Each 'record' is a tuple representing a row from the WeatherRecord table
                    print(record)
            else:
                print("The WeatherRecord table is empty.")
        except sqlite3.Error as e:
            print(f"Error fetching data from WeatherRecord table: {e}")

