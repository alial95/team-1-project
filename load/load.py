import os
import boto3
import json
import psycopg2


class redShift:

    def __init__(self):
        pass


    def get_cluster_cred(self):

        host = os.getenv("DB_HOST")
        port = int(os.getenv("DB_PORT"))
        user = os.getenv("DB_USER")
        passwd = os.getenv("DB_PASS")
        db = os.getenv("DB_NAME")
        cluster = os.getenv("DB_CLUSTER")

        try:
    
            client = boto3.client('redshift')
            creds = client.get_cluster_credentials(  # Lambda needs these permissions as well DataAPI permissions                                          
                DbUser=user,
                DbName=db,
                ClusterIdentifier=cluster,
                DurationSeconds=3600)  # Length of time access is granted
            
        except Exception as ERROR: 

            return {
                'message': ("Credentials Issue: " + str(ERROR))
            }
        
        try:
        
            conn = psycopg2.connect(
                
                dbname=db,
                user=creds["DbUser"],
                password=creds["DbPassword"],
                port=port,
                host=host)
            
        except Exception as ERROR:
            
            return {
                ("Connection Issue: " + str(ERROR))
            }
                    
def start(event, context):

    Data = []
    basket = []
    transactions = []

    for record in event['Records'][0]['body']:

        json_string = json.load(record)
        Data.append(json_string)

    for record in Data:

        if record['date'] == True:

            transactions.append(record)

        else:

            basket.append(record)
