import boto3
import pandas as pd
import json

from core.functions import send_message

def start(event, context):
    # get the csv and bucket name for read_csv function
    key = event['Records'][0]['s3']['object']['key']
    bucket = event['Records'][0]['s3']['bucket']['name']

    path = f's3://{bucket}/{key}'
    read_dataframe = pd.read_csv(path, names=[
                          'date', 'location', 'customer_name', 'basket', 'pay_amount', 'payment_method', 'ccn'])

    raw_transactions = []
    for i in range(1, len(read_dataframe)):
        transaction = {
            'date': read_dataframe['date'][i],
            'location': read_dataframe['location'][i],
            'customer_name': read_dataframe['customer_name'][i],
            'basket': read_dataframe['basket'][i],
            'total': str(read_dataframe['pay_amount'][i])
        }
        raw_transactions.append(transaction)
    json_data = json.dumps(raw_transactions)

    queue_url_1 = 'https://sqs.eu-west-1.amazonaws.com/579154747729/g1-extract-to-transform'

    send_message(json_data, queue_url_1)

