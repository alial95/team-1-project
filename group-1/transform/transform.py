import boto3
import json

sqs = boto3.client('sqs')

def start(event, context):
     print('lambda running')
    
     for record in event['Records']:
          raw_transactions_string = record['body']

          raw_transactions_string = event['Records'][0]['body']
          raw_transactions = json.loads(raw_transactions_string)
          baskets = []
          dates= []
          location = []
          names = []
          total = []
          for transaction in raw_transactions:
               baskets.append(transaction['basket'])
          for transaction in raw_transactions:
               dates.append(transaction['date'])
               location.append(transaction['location'])
               names.append(transaction['customer_name'])
               total.append(transaction['total'])

          calendar_day = []
          time_of_day = []
          for date in dates:
               times = date[-5:]
               calendar_date = date[:-5]
               calendar_day.append(calendar_date)
               time_of_day.append(times)     

          cleaned_transactions = []
          for i in range(1, len(dates)):
             transaction = {
                 'calendar_day': calendar_day[i],
                 'time_of_day': time_of_day[i],
                 'location': location[i],
                 'customer_name': names[i],
                 'total': total[i]
             }
             cleaned_transactions.append(transaction)
          transactions_json = json.dumps(cleaned_transactions) 
          
          baskets_1 = []
          for basket in baskets:
              test_basket = basket.split(',')
              baskets_1.append(test_basket)

          basket_items = []
          prices = []
          clean_basket_items = []
          for item in baskets_1:
              basket_test = item[0].split(',')
              basket_items.append(basket_test)

          for item in basket_items:
              price = item[0][-5:]
              basket_item = item[0].strip(f'{price}').strip(' -')
              clean_basket_items.append(basket_item)
              prices.append(price)

          clean_basket = []
          for i in range(1, len(prices)):
               basket = {
                    'Basket_item': clean_basket_items[i],
                    'Price': prices[i]
               }
               clean_basket.append(basket)
          basket_json = json.dumps(clean_basket)

          queue_url = 'https://sqs.eu-west-1.amazonaws.com/579154747729/g1-transform-to-load'

          response = sqs.send_message(
             QueueUrl = queue_url,
             MessageBody = basket_json
          )

          response = sqs.send_message(
             QueueUrl = queue_url,
             MessageBody = transactions_json
          )
          
     record_count = len(event['Records'])
     print(json.dumps({ 'recordCount': record_count }))

