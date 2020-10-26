import boto3
import pandas as pd
import json

queue_url = 'https://sqs.eu-west-1.amazonaws.com/579154747729/extract-to-load'


def start(event, context):
    # get the csv and bucket name for read_csv function
    key = event['Records'][0]['s3']['object']['key']
    bucket = event['Records'][0]['s3']['bucket']['name']

    path = f's3://{bucket}/{key}'
    test_df = pd.read_csv(path, names=[
                          'date', 'location', 'customer_name', 'basket', 'pay_amount', 'payment_method', 'ccn'])



    raw_transactions = []
    for i in range(1, len(test_df)):
        transaction = {
            'date': test_df['date'][i],
            'location': test_df['location'][i],
            'customer_name': test_df['customer_name'][i],
            'basket': test_df['basket'][i],
            'total': str(test_df['pay_amount'][i])
        }
        raw_transactions.append(transaction)
    json_data = json.dumps(raw_transactions)

    queue_url_1 = 'https://sqs.eu-west-1.amazonaws.com/579154747729/g1-extract-to-transform'

    # connect to sqs
    sqs = boto3.client('sqs')
    # send the message
    response = sqs.send_message(
        QueueUrl=queue_url_1,
        MessageBody=json_data
    )
