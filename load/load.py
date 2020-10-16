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
    def truncate_basket(self):
        try:
            with conn.cursor() as cursor:
                cursor.execute("TRUNCATE TABLE basket_group1")
                cursor.commit()

        except Exception as ERROR:
            return {
                'Error message': "Truncate basket issue: " + str(ERROR)
            }
        print('Basket truncated')

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
    def truncate_transaction(self):
        try:
            with conn.cursor() as cursor:
                cursor.execute ("TRUNCATE TABLE transactions_group1")
                cursor.commit()

        except Exception as ERROR:
            return {
                'Error message': "Transaction truncate issue: " + str(ERROR)
            }
        print('Transactions table truncated')
    def insert_into_transaction(self,transaction_list):
        try:
            with conn.cursor() as cursor:
                psycopg2.extras.execute_values(cursor, """
                    INSERT INTO transactions_group1 (total, customer_name, date_time, location) VALUES %s;
                """, [(
                    i['total'],
                    i['customer'],
                    i['date'],
                    i['location']
                ) for i in transaction_list])
                conn.commit()

        except Exception as ERROR:
            return {
                'Error message': "Insert into transaction issue: " + str(ERROR)
            }
        print('Inserted into transactions')

def start(event, context):
    print('lambda is running')
    
    
    basket = []
    transactions = []
    print(event['Records'][0]['body'])
    record = event['Records'][0]['body']
    # for record in event['Records'][0]['body']:

    #     json_string = json.loads(record)
    #     Data.append(json_string)
    json_string = json.loads(record)
    for record in json_string:

        if 'date' in record:

            transactions.append(record)

        else:

            basket.append(record)

    redshift_call = redShift()
    redshift_call.get_cluster_cred()
    redshift_call.truncate_basket()
    redshift_call.truncate_transaction()
    redshift_call.insert_into_transaction(transactions)
    redshift_call.insert_into_basket(basket)
