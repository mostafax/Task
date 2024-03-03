import unittest
from unittest.mock import patch, mock_open
from pathlib import Path
import sys
import unittest
import sys
from pathlib import Path
import json
import pandas as pd
import ast  
parent_dir_src = Path(__file__).resolve().parent.parent / 'src'
sys.path.append(str(parent_dir_src))



import unittest
from unittest.mock import MagicMock
import pandas as pd
from transformation import Transformation  # Adjust this import based on your project structure

class TestTransformation(unittest.TestCase):

    def setUp(self):
        self.mock_sales_data = pd.DataFrame({
            'order_id': [2334, 6228, 7784, 6588],
            'customer_id': [5, 8, 9, 5],
            'product_id': [40, 13, 44, 26],
            'quantity': [3, 7, 4, 1],
            'price': [35.6, 36.52, 46.56, 15.87],
            'order_date': ['2022-06-21', '2023-03-08', '2023-04-22', '2022-10-23'],
            'user_data': [
                ast.literal_eval(
                    "{'id': 5, 'name': 'Chelsey Dietrich', 'username': 'Kamren', "
                    "'email': 'Lucio_Hettinger@annie.ca', 'address': {'street': 'Skiles Walks', 'suite': 'Suite 351', "
                    "'city': 'Roscoeview', 'zipcode': '33263', 'geo': {'lat': '-31.8129', 'lng': '62.5342'}}, "
                    "'phone': '(254)954-1289', 'website': 'demarco.info', 'company': {'name': 'Keebler LLC', "
                    "'catchPhrase': 'User-centric fault-tolerant solution', 'bs': 'revolutionize end-to-end systems'}}"),
                ast.literal_eval(
                    "{'id': 8, 'name': 'Nicholas Runolfsdottir V', 'username': 'Maxime_Nienow', "
                    "'email': 'Sherwood@rosamond.me', 'address': {'street': 'Ellsworth Summit', 'suite': 'Suite 729', "
                    "'city': 'Aliyaview', 'zipcode': '45169', 'geo': {'lat': '-14.3990', 'lng': '-120.7677'}}, "
                    "'phone': '586.493.6943 x140', 'website': 'jacynthe.com', 'company': {'name': 'Abernathy Group', "
                    "'catchPhrase': 'Implemented secondary concept', 'bs': 'e-enable extensible e-tailers'}}"),
                ast.literal_eval(
                    "{'id': 9, 'name': 'Glenna Reichert', 'username': 'Delphine', "
                    "'email': 'Chaim_McDermott@dana.io', 'address': {'street': 'Dayna Park', 'suite': 'Suite 449', "
                    "'city': 'Bartholomebury', 'zipcode': '76495-3109', 'geo': {'lat': '24.6463', 'lng': '-168.8889'}}, "
                    "'phone': '(775)976-6794 x41206', 'website': 'conrad.com', 'company': {'name': 'Yost and Sons', "
                    "'catchPhrase': 'Switchable contextually-based project', 'bs': 'aggregate real-time technologies'}}"),
                ast.literal_eval(
                    "{'id': 5, 'name': 'Chelsey Dietrich', 'username': 'Kamren', "
                    "'email': 'Lucio_Hettinger@annie.ca', 'address': {'street': 'Skiles Walks', 'suite': 'Suite 351', "
                    "'city': 'Roscoeview', 'zipcode': '33263', 'geo': {'lat': '-31.8129', 'lng': '62.5342'}}, "
                    "'phone': '(254)954-1289', 'website': 'demarco.info', 'company': {'name': 'Keebler LLC', "
                    "'catchPhrase': 'User-centric fault-tolerant solution', 'bs': 'revolutionize end-to-end systems'}}")
            ],
            'geo': [
                "{'lat': '-31.8129', 'lng': '62.5342'}",
                "{'lat': '-14.3990', 'lng': '-120.7677'}",
                "{'lat': '24.6463', 'lng': '-168.8889'}",
                "{'lat': '-31.8129', 'lng': '62.5342'}"
            ],
            'weather_data': [
                ast.literal_eval(
                    "{'coord': {'lon': 62.5342, 'lat': -31.8129}, 'weather': [{'id': 804, 'main': 'Clouds', "
                    "'description': 'overcast clouds', 'icon': '04d'}], 'main': {'temp': 293.22, 'feels_like': 292.9, "
                    "'temp_min': 293.22, 'temp_max': 293.22, 'pressure': 1025, 'humidity': 62}, 'visibility': 10000, "
                    "'wind': {'speed': 4.04, 'deg': 109}, 'clouds': {'all': 100}, 'dt': 1709456675}"),
                ast.literal_eval(
                    "{'coord': {'lon': -120.7677, 'lat': -14.399}, 'weather': [{'id': 801, 'main': 'Clouds', "
                    "'description': 'few clouds', 'icon': '02n'}], 'main': {'temp': 298.97, 'feels_like': 299.49, "
                    "'temp_min': 298.97, 'temp_max': 298.97, 'pressure': 1014, 'humidity': 72}, 'visibility': 10000, "
                    "'wind': {'speed': 5.68, 'deg': 93}, 'clouds': {'all': 12}, 'dt': 1709456676}"),
                ast.literal_eval(
                    "{'coord': {'lon': -168.8889, 'lat': 24.6463}, 'weather': [{'id': 804, 'main': 'Clouds', "
                    "'description': 'overcast clouds', 'icon': '04n'}], 'main': {'temp': 295.24, 'feels_like': 295.54, "
                    "'temp_min': 295.24, 'temp_max': 295.24, 'pressure': 1024, 'humidity': 78}, 'visibility': 10000, "
                    "'wind': {'speed': 5.81, 'deg': 56}, 'clouds': {'all': 86}, 'dt': 1709456676}"),
                ast.literal_eval(
                    "{'coord': {'lon': 62.5342, 'lat': -31.8129}, 'weather': [{'id': 804, 'main': 'Clouds', "
                    "'description': 'overcast clouds', 'icon': '04d'}], 'main': {'temp': 293.22, 'feels_like': 292.9, "
                    "'temp_min': 293.22, 'temp_max': 293.22, 'pressure': 1025, 'humidity': 62}, 'visibility': 10000, "
                    "'wind': {'speed': 4.04, 'deg': 109}, 'clouds': {'all': 100}, 'dt': 1709456676}")
            ]
        })
        self.mock_sales_data['sales_amount'] = self.mock_sales_data['quantity'] * self.mock_sales_data['price']

    def test_identify_top_selling_products_or_customers(self):
        """Test identifying top-selling products and customers."""
        transformation = Transformation(self.mock_sales_data)
        top_selling_products, top_customers = transformation.identify_top_selling_products_or_customers()
        
        # Assuming these return Series objects for simplicity
        self.assertIsInstance(top_selling_products, pd.Series)
        self.assertIsInstance(top_customers, pd.Series)
        self.assertNotEqual(len(top_selling_products), 0)
        self.assertNotEqual(len(top_customers), 0)
        # Check if the top-selling product/customer is correctly identified
        # This assumes product_id and customer_id with the highest sales/quantity
        expected_top_product_id = self.mock_sales_data.groupby('product_id')['quantity'].sum().idxmax()
        expected_top_customer_id = self.mock_sales_data.groupby('customer_id')['quantity'].sum().idxmax()
        self.assertEqual(top_selling_products.index[0], expected_top_product_id)
        self.assertEqual(top_customers.index[0], expected_top_customer_id)

    def test_analyze_sales_trends_over_time(self):
        """Test analyzing sales trends over time."""
        transformation = Transformation(self.mock_sales_data)
        sales_trends = transformation.analyze_sales_trends_over_time()

        self.assertIsInstance(sales_trends, pd.Series)
        self.assertNotEqual(len(sales_trends), 0)
        # For simplicity, let's just check if the sales trend data includes the correct dates
        expected_dates = pd.to_datetime(self.mock_sales_data['order_date']).dt.to_period('M').unique()
        trend_dates = sales_trends.index.to_period('M')
        self.assertTrue(all(date in trend_dates for date in expected_dates))

    def test_include_weather_data_in_analysis(self):
        """Test including weather data in the analysis."""
        transformation = Transformation(self.mock_sales_data)
        weather_impact_on_sales = transformation.include_weather_data_in_analysis()

        self.assertIsInstance(weather_impact_on_sales, pd.Series)
        self.assertNotEqual(len(weather_impact_on_sales), 0)
        # Check if weather conditions are included
        expected_weather_conditions = self.mock_sales_data['weather_data'].apply(lambda x: x['weather'][0]['main'] if 'weather' in x and len(x['weather']) > 0 else None).unique()
        weather_conditions_in_analysis = weather_impact_on_sales.index.unique()
        self.assertTrue(all(condition in weather_conditions_in_analysis for condition in expected_weather_conditions if condition is not None))
    
    def analyze_sales_trends_over_time(self):
        # Ensure 'sales_amount' is calculated if not already present
        if 'sales_amount' not in self.sales_data.columns:
            self.sales_data['sales_amount'] = self.sales_data['quantity'] * self.sales_data['price']
        
        # Proceed with the original logic
        return self.sales_data.set_index('order_date').resample('MS')['sales_amount'].sum()


if __name__ == '__main__':
    unittest.main()
