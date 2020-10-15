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




     basket_items = []
     for item in baskets:
          item.split(',')
          basket_items.append(item)
     prices = []
     clean_basket_items = []
     for item in basket_items:
          price = item[-5:]
          basket_item = item.strip(f'{price}').strip(' -')
          clean_basket_items.append(basket_item)
          prices.append(price)
     print(prices[0])
     print(clean_basket_items[0])
     clean_basket = []
     for i in range(1, len(prices)):
          basket = {
               'Basket_item': clean_basket_items[i],
               'Price': prices[i]
          }
          clean_basket.append(basket)
     basket_json = json.dumps(clean_basket)


     queue_url = sqs.get_queue_url(
          QueueName='transform-to-load',
          )



     response_transaction = sqs.send_message(
        QueueUrl = queue_url,
        MessageBody = transactions_json
     
     )

     send_basket = sqs.send_message(
          QueueUrl = queue_url,
          MessageBody = basket_json
     )
     # host = os.getenv("DB_HOST")
     # port = int(os.getenv("DB_PORT"))
     # user = os.getenv("DB_USER")
     # passwd = os.getenv("DB_PASS")
     # db = os.getenv("DB_NAME")
     # cluster = os.getenv("DB_CLUSTER")
     
     # print('Got credentials')

     # try:
     #      client = boto3.client('redshift')
     #      creds = client.get_cluster_credentials(  # Lambda needs these permissions as well DataAPI permissions
     #           DbUser=user,
     #           DbName=db,
     #           ClusterIdentifier=cluster,
     #           DurationSeconds=3600) # Length of time access is granted
     # except Exception as ERROR:
     #      return {
     #           'message': ("Credentials Issue: " + str(ERROR))
     #      }
          

     # try:
     #      conn = psycopg2.connect(
     #           dbname=db,
     #           user=creds["DbUser"],
     #           password=creds["DbPassword"],
     #           port=port,
     #           host=host)
     # except Exception as ERROR:
     #      return ("Connection Issue: " + str(ERROR))

     # try:
     #      with conn.cursor() as cursor:
     #           psycopg2.extras.execute_values(cursor, """
     #                INSERT INTO transactions_group1 (total, customer_name, date_time, location) VALUES %s;
     #           """, [(
     #                total[i],
     #                customer_name[i],
     #                date[i],
     #                location[i]      
     #           ) for i in range (1, len(date))])
     #           conn.commit()    

     # except Exception as ERROR:
     #      return ("Execution Issue: " + str(ERROR))
    
    
    
    
    
    
    
    

    
    
