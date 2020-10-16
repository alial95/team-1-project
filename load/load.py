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
        
        try:
        
            conn = psycopg2.connect(
                
                dbname=db,
                user=creds["DbUser"],
                password=creds["DbPassword"],
                port=port,
                host=host)
            
        except Exception as ERROR:
            
            return {
                ("Connection Issue: " + str(ERROR))
            }
    
    def truncate_basket(self):
        try:
            with conn.cursor() as cursor:
                cursor.execute("TRUNCATE TABLE basket_group1")
                cursor.commit()

        except Exception as ERROR:
            return {
                ("Basket truncate issue: " + str(ERROR))
            }
    

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
                ("Execution basket Issue: " + str(ERROR))
            }

    def truncate_transaction(self):
            try:
                with conn.cursor() as cursor:
                    cursor.execute ("TRUNCATE TABLE transactions_group1")
                    cursor.commit()

            except Exception as ERROR:
                return {
                    ("Transaction truncate issue: " + str(ERROR))
                }

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
                    ("Execution transaction Issue: " + str(ERROR))
                }

def start(event, context):

    Data = []
    basket = []
    transactions = []
    print(event['Records'][0]['body'])

    for record in event['Records'][0]['body']:

        json_string = json.load(record)
        Data.append(json_string)

    for record in Data:

        if record['date'] == True:

            transactions.append(record)

        else:

            basket.append(record)

    redshift_call = redShift()
    redshift_call.get_cluster_cred()
    redshift_call.truncate_basket()
    redshift_call.truncate_transaction()
    redshift_call.insert_into_transaction(transactions)
    redshift_call.insert_into_basket(basket)
