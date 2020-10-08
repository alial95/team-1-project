import boto3
import pandas as pd
import numpy as np
from itertools import chain
from classes.transaction import Transaction
from classes.basket import Basket

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


# fill transaction list
def fill_transaction_list():
    transactions_today = []
    for i in range(1, len(df)):
        customer = Transaction(total[i], customers[i], dates[i], location[i])
        transactions_today.append(customer)
    return transactions_today



# create comma-separated strings
def chainer(s):
    return list(chain.from_iterable(s.str.split(',')))
# determine lengths of splits
lens = basket.str.split(',').map(len)
# create new dataframe for chain orders
new = pd.DataFrame({'customer_name': np.repeat(df['customer_name'], lens),
                    'date': np.repeat(df['date'], lens),
                    'basket': chainer(df['basket'])})


# getting the price of the basket item and the basket item
def get_basket_item():
    for i in new['basket']:
        basket_items = []
        basket_item = i[:-5]
        basket_item_test = basket_item.strip(' -')
        basket_items.append(basket_item_test)
    return basket_items
    
def get_basket_price():
    for i in new['basket']:
        basket_price = []
        price = float(i[-5:])
        basket_price.append(price)
    return basket_price
    
    

    
def fill_basket_list():
    basket_price = get_basket_price()
    basket_items = get_basket_item()
    basket_sep = []
    # fill our basket list with the basket objects
    for i in range(0, len(new['basket'])):
        purchase = Basket(basket_items[i], basket_price[i])
        basket_sep.append(purchase)
    return basket_sep