import boto3
import json
import os
import logging
from elasticsearch import Elasticsearch, RequestsHttpConnection
from aws_requests_auth.aws_auth import AWSRequestsAuth

logger = logging.getLogger()
es_host = 'vpc-photos-6zorlygrzniagf4nfzcegtqjka.us-east-1.es.amazonaws.com'
es_index = 'photos'
access_key = 'AKIAZVY435LIRPSPSKH2'
secret_access_key = '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'
region = 'us-east-1'

# Establish connection to ElasticSearch
auth = AWSRequestsAuth(aws_access_key=access_key,
                      aws_secret_access_key=secret_access_key,
                      #aws_token=session_token,
                      aws_host=es_host,
                      aws_region=region,
                      aws_service='es')

es = Elasticsearch(host=es_host,
                  port=443,
                  use_ssl=True,
                  connection_class=RequestsHttpConnection,
                  http_auth=auth)

print ("es.info:", es.info())

def lambda_handler(event, context):
    """Lambda Function entrypoint handler

    :event: S3 Put event
    :context: Lambda context
    :returns: Number of records processed

    """
    print(json.dumps(event, indent=4, sort_keys=True))
    #print ("I am triggering 2...")
    processed = 0
    rek_client = boto3.client('rekognition')
    
    for record in event['Records']:

        s3_record = record['s3']
        
        key = s3_record['object']['key']
        bucket = s3_record['bucket']['name']
        ttamp = record['eventTime']
     
        print ("I am triggering 3...")
        resp = rek_client.detect_labels(
            Image={'S3Object': {'Bucket': bucket, 'Name': key}},
            MaxLabels=3,
            MinConfidence=90)

        print ("I am triggering 4...")    
        labels = []
        for l in resp['Labels']:
            labels.append(l['Name'])
        
        print ("labels are: ", labels)
        print ("es is: ", es)
      
        message = {'objectKey': key,'bucket': bucket,'createdTimestamp': ttamp, 'labels': labels}
        print ("message is:", type(message), message)
        res = es.index(index=es_index, doc_type='event',
                      body = message,
                      id=key
                      )

        print ("res are: ", res)
        logger.debug(res)
        processed = processed + 1
        
    response2 = {
                    'statusCode': 200,
                    'headers': { 
                        'Content-Type': 'application/json',
                        'Access-Control-Allow-Origin': "*" ,
                        'Access-Control-Allow-Methods': '*',
                        "Access-Control-Allow-Credentials" : True
                    },
                    'body': json.dumps(processed),
                    'isBase64Encoded': False
                }
    logger.info('Successfully processed {} records'.format(processed))
    print ("I am triggered")
    return response2


