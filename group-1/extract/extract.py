import boto3
import pandas as pd

queue_url = 'https://sqs.eu-west-1.amazonaws.com/579154747729/Group-1-Extract-Q'



def start(event, context):

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

    # for dataframe in dataframes:
    for i in range(1, len(test_df)):

        response = sqs.send_message(
            QueueUrl = queue_url,
            MessageAttributes = {
                'Dates_for_purchases': {
                    'DataType': 'String',
                    'StringValue': test_df['date'][i]
                    }
                'Location': {
                    'DataType': 'String',
                    'StringValue': test_df['location'][i]
                },
                 'Customer_name': {
                    'DataType': 'String',
                    'StringValue': test_df['customer_name'][i]
                },
                 'Basket_total': {
                    'DataType': 'String',
                    'StringValue': test_df['basket'][i]
                },
                 'Pay_amount': {
                    'DataType': 'String',
                    'StringValue': str(test_df['pay_amount'][i])
                } 
                }, 

                MessageBody = 'Test_String'
        )

    
