import unittest
import os
import sys
from pathlib import Path

parent_dir_src = Path(__file__).resolve().parent.parent / 'src'
sys.path.append(str(parent_dir_src))

from database_manager import DatabaseManager


class TestDatabaseManager(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        """Set up a temporary database for testing."""
        cls.test_db_name = '../outputs/database.db'
        cls.db_manager = DatabaseManager(cls.test_db_name)
        cls.db_manager.connect()
        cls.db_manager.create_tables()

    @classmethod
    def tearDownClass(cls):
        """Clean up: close the database connection and remove the test database file."""
        cls.db_manager.close()
        os.remove(cls.test_db_name)

    def test_connection(self):
        """Test database connection is established."""
        self.assertIsNotNone(self.db_manager.conn)
        self.assertIsNotNone(self.db_manager.cursor)

    def test_table_creation(self):
        """Verify that tables are created."""
        tables = ["Companies", "Customers", "Products", "Orders", "WeatherRecord", "TotalSalesPerCustomer", "AverageOrderQuantityPerProduct"]
        for table in tables:
            with self.db_manager.conn:
                cur = self.db_manager.conn.cursor()
                cur.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table}';")
                self.assertEqual(cur.fetchone()[0], table)

    def test_insert_and_fetch_company_data(self):
        """Test inserting and fetching company data."""
        test_data = [('Test Company', 'Innovate endlessly', 'Empower and advance')]
        self.db_manager.insert_company_data(test_data)
        self.db_manager.conn.commit()  # Ensure data is committed for the test

        # Validate insertion
        self.db_manager.cursor.execute("SELECT name, catch_phrase, bs FROM Companies WHERE name = ?", ('Test Company',))
        result = self.db_manager.cursor.fetchone()
        self.assertIsNotNone(result)
        self.assertEqual(result, test_data[0])


if __name__ == '__main__':
    unittest.main()
