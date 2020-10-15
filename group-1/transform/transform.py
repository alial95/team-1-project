import psycopg2
import psycopg2.extras
from dotenv import load_dotenv
import boto3
import os
load_dotenv()


def start(event, context):
    
     # transform the data
     date = event['Records'][0]['messageAttributes']['Dates_for_purchases']['stringValue']
     location = event['Records'][0]['messageAttributes']['Location']['stringValue']
     customer_name = event['Records'][0]['messageAttributes']['Customer_name']['stringValue']
     basket = event['Records'][0]['messageAttributes']['Basket_total']['stringValue']
     total = event['Records'][0]['messageAttributes']['Pay_amount']['stringValue']

     
     basket_items_1 = basket.split(',')
     basket_items = []
     prices = []
     for item in basket_items_1:
          price = item[-5:]
          basket_item = item.strip(f'{price}').strip(' -')
          basket_items.append(basket_item)
          prices.append(price)
     print(prices)
     print(basket_items)

     host = os.getenv("DB_HOST")
     port = int(os.getenv("DB_PORT"))
     user = os.getenv("DB_USER")
     passwd = os.getenv("DB_PASS")
     db = os.getenv("DB_NAME")
     cluster = os.getenv("DB_CLUSTER")
     
     print('Got credentials')

     try:
          client = boto3.client('redshift')
          creds = client.get_cluster_credentials(  # Lambda needs these permissions as well DataAPI permissions
               DbUser=user,
               DbName=db,
               ClusterIdentifier=cluster,
               DurationSeconds=3600) # Length of time access is granted
     except Exception as ERROR:
          return ("Credentials Issue: " + str(ERROR))
          
          

     try:
          conn = psycopg2.connect(
               dbname=db,
               user=creds["DbUser"],
               password=creds["DbPassword"],
               port=port,
               host=host)
     except Exception as ERROR:
          return ("Connection Issue: " + str(ERROR))

     try:
          with conn.cursor() as cursor:
               psycopg2.extras.execute_values(cursor, """
                    INSERT INTO transactions_group1 (total, customer_name, date_time, location) VALUES %s;
               """, [(
                    total,
                    customer_name,
                    date,
                    location      
               ) ])
               conn.commit()    

     except Exception as ERROR:
          return ("Execution Issue: " + str(ERROR))
    
    
    
    
    
    
    
    

    
    
