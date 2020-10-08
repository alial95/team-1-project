import psycopg2
import psycopg2.extras
import sys
import os
import pandas as pd
import boto3
import numpy as np
from dotenv import load_dotenv
from itertools import chain
load_dotenv()

def start(event, context):

    # connect to bucket and get bucket key
    s3 = boto3.client('s3')
    response = s3.list_objects(Bucket='cafe-transactions')
    first_step = response['Contents'][-1]
    key = first_step['Key']

    bucket = 'cafe-transactions'
    path = f's3://{bucket}/{key}'
    # read bucket contents and sort them into variables
    df = pd.read_csv(path, names=['date', 'location', 'customer_name', 'basket', 'pay_amount', 'payment_method', 'ccn'])
    dates = df['date']
    basket = df['basket']
    location = df['location']
    total = df['pay_amount']
    customer = df['customer_name']
    customers = list(customer)
    
    class Transaction:
        def __init__(self, transaction_id, total, customer_name, date, location):
            self.transaction_id= transaction_id
            self.total = total
            self.customer_name = customer_name
            self.date = date
            self.location = location
        def __repr__(self):
            return f'Customer name is {self.customer_name}.'
    # fill bucket list
    transactions_today = []
    for i in range(1, len(df)):
        customer = Transaction(i-1, total[i], customers[i], dates[i], location[i])
        transactions_today.append(customer)
    
    transaction_ids = []
    for i in transactions_today:
        trans_id = i.transaction_id
        transaction_ids.append(trans_id)

    # create comma-separated strings
    def chainer(s):
        return list(chain.from_iterable(s.str.split(',')))
    # determine lengths of splits
    lens = df['basket'].str.split(',').map(len)
    # create new dataframe for chain orders
    new = pd.DataFrame({'customer_name': np.repeat(df['customer_name'], lens),
                        'date': np.repeat(df['date'], lens),
                        'basket': chainer(df['basket'])})

    basket_price = []
    basket_items = []
    # getting the price of the basket item and the basket item
    for i in new['basket']:
        price = float(i[-5:])
        basket_item = i[:-5]
        basket_item_test = basket_item.strip(' -')
        basket_price.append(price)
        basket_items.append(basket_item_test)
    

    class Basket:
        def __init__(self, basket_id, basket_item, cost):
            self.basket_id = basket_id
            self.basket_item = basket_item
            self.cost = cost
    basket_sep = []
    # fill our basket list with the basket objects
    for i in range(0, len(new['basket'])):
        purchase = Basket(i, basket_items[i], basket_price[i])
        basket_sep.append(purchase)
    
    
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
                INSERT INTO transactions_group1 VALUES %s;
            """, [(
                transaction.transaction_id,
                transaction.total,
                transaction.customer_name,
                transaction.date,
                transaction.location
            ) for transaction in transactions_today])
            conn.commit()    
    
    except Exception as ERROR:
        print("Execution Issue: " + str(ERROR))
    
    print('going to basket now')

    try:
        with conn.cursor() as cursor:
            psycopg2.extras.execute_values(cursor, """
                INSERT INTO basket_group1 VALUES %s;
            """, [(
                transaction.basket_id,
                transaction.basket_item,
                transaction.cost,
            ) for transaction in basket_sep])
            conn.commit()  
            conn.close() 

    except Exception as ERROR:
        print("Execution Issue: " + str(ERROR))
    
       


    print('executed statement')

   