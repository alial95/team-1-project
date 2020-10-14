import psycopg2
import psycopg2.extras
import sys
import os
import pandas as pd
import boto3
import numpy as np
from dotenv import load_dotenv
from itertools import chain
from basket import Basket
from transaction import Transaction
from core.functions import fill_basket_list, fill_transaction_list
load_dotenv()

def start(event, context):

    basket_list = fill_basket_list()
    transactions_list = fill_transaction_list()

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
            DurationSeconds=3600) # Length of time access is granted
    except Exception as ERROR:
        print("Credentials Issue: " + str(ERROR))
        sys.exit(1)

    print('got credentials')

    try:
        conn = psycopg2.connect(
            dbname=db,
            user=creds["DbUser"],
            password=creds["DbPassword"],
            port=port,
            host=host)
    except Exception as ERROR:
        print("Connection Issue: " + str(ERROR))
        sys.exit(1)
    
    print('connected')
    print('going to transactions now')
    try:
        with conn.cursor() as cursor:
            psycopg2.extras.execute_values(cursor, """
                INSERT INTO transactions_group1 (total, customer_name, date_time, location) VALUES %s;
            """, [(
                transaction.total,
                transaction.customer_name,
                transaction.date,
                transaction.location
            ) for transaction in transactions_list])
            conn.commit()    
    
    except Exception as ERROR:
        print("Execution Issue: " + str(ERROR))
    
    print('going to basket now')

    try:
        with conn.cursor() as cursor:
            psycopg2.extras.execute_values(cursor, """
                INSERT INTO basket_group1 (basket_item, cost) VALUES %s;
            """, [(
                transaction.basket_item,
                transaction.cost,
            ) for transaction in basket_list])
            conn.commit()  
            conn.close() 

    except Exception as ERROR:
        print("Execution Issue: " + str(ERROR))
    
       


    print('executed statement')

   