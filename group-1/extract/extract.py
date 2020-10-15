import boto3
import pandas as pd
import json

queue_url = 'https://sqs.eu-west-1.amazonaws.com/579154747729/extract-to-load'


def start(event, context):

    print('TEST STATEMENT')
    print('second test')

    sqs = boto3.client('sqs')
    s3 = boto3.client('s3')
    response = s3.list_objects(Bucket='cafe-transactions')
    first_step = response['Contents']
    keys = []
    for key in first_step:
        # print(key['Key'])
        keys.append(key['Key'])
    test_key = keys[0]
    bucket = 'cafe-transactions'
    path = f's3://{bucket}/{test_key}'
    test_df = pd.read_csv(path, names=['date', 'location', 'customer_name', 'basket', 'pay_amount', 'payment_method', 'ccn'])
    # dataframes = []
    # for csv in keys:
    #     path = f's3://{bucket}/{csv}'
    #     df = pd.read_csv(path, names=['date', 'location', 'customer_name', 'basket', 'pay_amount', 'payment_method', 'ccn'])
    #     dataframes.append(df)
    raw_transactions = []
    for i in range(1, len(test_df)):
        transaction = {
            'date': test_df['date'][i],
            'location': test_df['location'][i],
            'customer_name': test_df['customer_name'][i],
            'basket': test_df['basket'][i],
            'total': test_df['pay_amount'][i]
        }
        raw_transactions.append(transaction)
    json_data = json.dumps(raw_transactions) 
    
    queue_url = sqs.get_queue_url(
        QueueName='extract-to-load',

        )




    # for dataframe in dataframes:
    

    response = sqs.send_message(
        QueueUrl = queue_url,
        MessageBody = json_data
    )

    
