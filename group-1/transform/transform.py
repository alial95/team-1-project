import boto3
import json
from classes import Transaction, Basket

sqs = boto3.client('sqs')

def start(event, context):
     print('lambda running')
     transactions = []
     baskets = []
     for record in event['Records']:
          json_data = json.loads(record['body'])
          for transaction in json_data:
               transaction_obj = Transaction(transaction['date'][:-5], transaction['date'][-5:], transaction['location'], transaction['customer_name'], transaction['total'])
               transactions.append(transaction_obj)
               basket_items = transaction['basket'].split(',')
               for item in basket_items:
                    price = item[0][-5:]
                    basket_item = item[0].strip(f'{price}').strip(' -')
                    basket = Basket(basket_item, price)
                    baskets.append(basket)
          cleaned_transactions = []
          for transaction in transactions:
             transaction_dict = {
                 'calendar_day': transaction.date,
                 'time_of_day': transaction.time,
                 'location': transaction.location,
                 'customer_name': transaction.customer,
                 'total': transaction.total
             }
             cleaned_transactions.append(transaction_dict)
          transactions_json = json.dumps(cleaned_transactions) 
          

          clean_basket = []
          for basket in baskets:
               basket_dict = {
                    'Basket_item': basket.basket_item,
                    'Price': basket.price
               }
               clean_basket.append(basket_dict)
          basket_json = json.dumps(clean_basket)

          queue_url = 'https://sqs.eu-west-1.amazonaws.com/579154747729/g1-transform-to-load'

          sqs.send_message(
             QueueUrl = queue_url,
             MessageBody = basket_json
          )

          sqs.send_message(
             QueueUrl = queue_url,
             MessageBody = transactions_json
          )
          
     record_count = len(event['Records'])
     print(json.dumps({ 'recordCount': record_count }))
