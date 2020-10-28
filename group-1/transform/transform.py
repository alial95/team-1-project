import boto3
import json
from classes import Transaction, Basket
from functions import send_message

# sqs = boto3.client('sqs')
          
def cleaned_transactions(transactions):
     
     clean_transactions = []
     
     for transaction in transactions:
          
          transaction_dict = {
               'calendar_day': transaction.date,
               'time_of_day': transaction.time,
               'location': transaction.location,
               'customer_name': transaction.customer,
               'total': transaction.total
          }
          
          clean_transactions.append(transaction_dict)
          
     return clean_transactions
     
def cleaned_basket(baskets):
     
     clean_basket = []
     
     for basket in baskets:
     
          basket_dict = {
               'Basket_item': basket.basket_item,
               'Price': basket.price
          }
     
          clean_basket.append(basket_dict)
     
     return clean_basket

     
def get_object_price_and_item(basket_items):
     price = basket_items[0][-5:]
     basket_item = basket_items[0].strip(f'{price}').strip(' -')
     basket = Basket(basket_item, price)
     return basket
     

def start(event, context):
     print('lambda running')
     
     transactions = []
     baskets = []
     queue_url = 'https://sqs.eu-west-1.amazonaws.com/579154747729/g1-transform-to-load'
     
     for record in event['Records']:
     
          json_data = json.loads(record['body'])
     
          for transaction in json_data:
     
               transaction_obj = Transaction(transaction['date'][:-5], transaction['date'][-5:], transaction['location'], transaction['customer_name'], transaction['total'])
               transactions.append(transaction_obj)
               
               basket_items = transaction['basket'].split(',')
               basket_object = get_object_price_and_item(basket_items)
               baskets.append(basket_object)
          
          transactions_json = json.dumps(cleaned_transactions(transactions))           
          basket_json = json.dumps(cleaned_basket(baskets))

          send_message(transactions_json, queue_url)
          send_message(basket_json, queue_url)
                    
     record_count = len(event['Records'])
     print(json.dumps({ 'recordCount': record_count }))
