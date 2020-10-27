def send_message(json_data, queue_url):
     
     sqs.send_message(
          QueueUrl = queue_url,
          MessageBody = json_data
     )