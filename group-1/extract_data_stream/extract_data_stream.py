import base64
import json

# perhaps use regex expressions to identify patterns in the data stream

def start(event, context):
    
    output = []

    for record in event['records'] :
        print(record['recordId'])
        decoded_data = base64.b64decode(record['data']).decode('utf-8')
        print(decoded_data)
        reading = json.loads(decoded_data)
        print(reading)

        output_record = {
            'recordId': record['recordId'],
            'result': 'Ok',
            'data': base64.b64encode(json.dumps(reading).encode('UTF-8'))
        }
        output.append(output_record)

    print('Processed {} records.'.format(len(event['records'])))

    # connect to sqs
    # sqs = boto3.client('sqs')
    # # send the message
    
    # response = sqs.send_message(
    #     QueueUrl = queue_url_1,
    #     MessageBody = json_data
    # )
    
    return {'records': output}