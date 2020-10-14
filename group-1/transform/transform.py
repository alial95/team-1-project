

def lambda_handler(event, context):
     # transfrom the data
    record = event['Records'][0]['body']
    
    
