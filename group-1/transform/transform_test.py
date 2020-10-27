import unittest
import boto3
from unittest.mock import Mock
from classes import Transaction, Basket


from transform import cleaned_transactions, cleaned_basket

class TestTransform(unittest.TestCase):
    def test_cleaned_transactions(self):
        test_object = Transaction('18181', '0700', 'birmingham', 'ali', '500')
        test_list = [test_object]
        expected_output = [{
            'calendar_day': '18181',
            'time_of_day': '0700',
            'location': 'birmingham',
            'customer_name': 'ali',
            'total': '500'}
        ]
        self.assertEqual(cleaned_transactions(test_list), expected_output)
    def test_cleaned_basket(self):
        test_basket = Basket('cappucino', '6.00')
        test_list = [test_basket]
        expected_output = [{
               'Basket_item': 'cappucino',
               'Price': '6.00'
          }]
        self.assertEqual(cleaned_basket(test_list), expected_output)






if __name__ == "__main__":
    unittest.main()