import boto3

def send_message(json_data, queue_url):  
     sqs = boto3.client('sqs')   
     sqs.send_message(
          QueueUrl = queue_url,
          MessageBody = json_data
     )