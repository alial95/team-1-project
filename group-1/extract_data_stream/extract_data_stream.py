import base64
import json

# perhaps use regex expressions to identify patterns in the data stream
["26/10/2020 18:39", "Chichester", "Angela Labrecque", " Speciality Tea - Earl Grey - 1.30", "1.30", "CARD", "5468283889313827"]

def start(event, context):
    
    raw_transactions = []
    for record in event['Records']:
        print(record)
        try:
            decoded_data = base64.b64decode(record['kinesis']['data'])
        except Exception as error:
            print(json.dumps({'Error Message': error}))
        try:
            transaction = {
                'date': decoded_data[0],
                'location': decoded_data[1],
                'customer_name': decoded_data[2],
                'basket': decoded_data[3],
                'total': decoded_data[4]
            }   
            raw_transactions.append(transaction)
        except Exception as error:
            print(json.dumps({'Error Message': error}))
        json_data = json.dumps(raw_transactions)
        print(json_data)
        # print(json.dumps({'Record processed': }))    

        
    # connect to sqs
    # sqs = boto3.client('sqs')
    # # send the message
    
    # response = sqs.send_message(
    #     QueueUrl = queue_url_1,
    #     MessageBody = json_data
    # )
    
    # return {'records': output}