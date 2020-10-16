import boto3
import json

sqs = boto3.client('sqs')

def start(event, context):
    
     # transform the data

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
     cleaned_transactions = []
     for i in range(1, len(dates)):
        transaction = {
            'date': dates[i],
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


     # queue_url = sqs.get_queue_url(
     #      QueueName='transform-to-load',
     #      )
     queue_url_1 = 'https://sqs.eu-west-1.amazonaws.com/579154747729/transform-to-load'


     response = sqs.send_message(
        QueueUrl = queue_url_1,
        MessageBody = (transactions_json,
        basket_json
        )
     )

    
     
    
    
    
    
    
    
    

    
    
