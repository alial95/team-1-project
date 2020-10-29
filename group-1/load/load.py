import os
import boto3
import json
import psycopg2 
import psycopg2.extras
from dotenv import load_dotenv
load_dotenv()

class redShift:
    
    def __init__(self):
        pass

    def get_cluster_cred(self):
        global conn
        host = os.getenv("DB_HOST")
        port = int(os.getenv("DB_PORT"))
        user = os.getenv("DB_USER")
        passwd = os.getenv("DB_PASS")
        db = os.getenv("DB_NAME")
        cluster = os.getenv("DB_CLUSTER")

        try:
    
            client = boto3.client('redshift')
            creds = client.get_cluster_credentials(  # Lambda needs these permissions as well DataAPI permissions                                          
                DbUser=user,
                DbName=db,
                ClusterIdentifier=cluster,
                DurationSeconds=3600)  # Length of time access is granted
            
        except Exception as ERROR: 

            return {
                'message': ("Credentials Issue: " + str(ERROR))
            }
        print('Got credentials')
        try:
        
            conn = psycopg2.connect(
                
                dbname=db,
                user=creds["DbUser"],
                password=creds["DbPassword"],
                port=port,
                host=host)
            
        except Exception as ERROR:
            
            return {
                'Error message': "Connection issue: " + str(ERROR)
            }
        print('Connected')

    def insert_into_basket(self,basket_list):
        try:
            with conn.cursor() as cursor:
                psycopg2.extras.execute_values(cursor, """
                    INSERT INTO basket_group1 (basket_item, cost) VALUES %s;
                """, [(
                    i['Basket_item'],
                    i['Price'],
                    
                ) for i in basket_list])
                conn.commit()
        
        except Exception as ERROR:
            return {
                'Error message': "Insert into basket issue: " + str(ERROR)
            }
        print('Inserted into basket')
    
    def insert_into_transaction(self,transaction_list):
        try:
            with conn.cursor() as cursor:
                psycopg2.extras.execute_values(cursor, """
                    INSERT INTO transactions_group1 (total, customer_name, location, calendar_day, transaction_time) VALUES %s;
                """, [(
                    i['total'],
                    i['customer_name'],
                    i['location'],
                    i['calendar_day'],
                    i['time_of_day'],
                ) for i in transaction_list])
                conn.commit()

        except Exception as ERROR:
            return {
                'Error message': "Insert into transaction issue: " + str(ERROR)
            }
        print('Inserted into transactions')

def start(event, context):
    baskets = []
    transactions = []
    
    redshift_call = redShift()
    redshift_call.get_cluster_cred()
    basket_count = 0
    transactions_count = 0
    for record in event['Records']:      
        json_string = json.loads(record['body'])
        for i in json_string:
            if 'calendar_day' in i:
                transactions.append(i)
                transactions_count += 1
            else:
                baskets.append(i)
                basket_count += 1

    redshift_call.insert_into_basket(baskets)
    redshift_call.insert_into_transaction(transactions)

    print(json.dumps({
        'baskets' : len(baskets), 
        'transactions':len(transactions)
    }))
    print(json.dumps({
        'baskets': baskets,
        'transactions': transactions
    }))
    
