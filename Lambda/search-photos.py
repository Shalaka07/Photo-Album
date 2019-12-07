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

client_run = boto3.client('lex-runtime',region_name='us-east-1')
client_model = boto3.client('lex-models',region_name='us-east-1')

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
    
    print ("event is:", event)
    print ("context is:", context)
    logger.info("event is:", event)
    
    print("event['queryStringParameters']['q']:", event['queryStringParameters']['q'])
    msg = str(event['queryStringParameters']['q'])
    print ("msg is:", msg)
    logger.info(msg)
    response = client_run.post_text(
        botName='PhotoAlbumBot',
        botAlias='$LATEST',
        #userId='yy',
        inputText=msg
    )
    bot_details = client_model.get_bot(
        name='PhotoAlbumBot',
        versionOrAlias='$LATEST'
    )
    
    print ("response:", response)
    kw1 = response['slots']['keywordA']
    kw2 = response['slots']['keywordB']
    print("kw1kw2", kw1, kw2, type(kw1), type(kw2))
    
    if kw2 == None:
        print("Only search kw1:", kw1)
        res = es.search(index=es_index, body={"query": {"match": {"labels": kw1}}})
        print("kw2 is null")
    else:
        res = es.search(index=es_index, body={"query": {"match": {"labels": kw1}}})
        res2 = es.search(index=es_index, body={"query": {"match": {"labels": kw2}}})
        res['hits']['total'] = res['hits']['total'] + res2['hits']['total']
        res['hits']['hits'] = res['hits']['hits']+res2['hits']['hits']
        print (res)
        print (res2)
        print("kw2 isn't null")

    print("Got %d Hits:" % res['hits']['total'])
    print (res)
    
    
    response2 = {
                    'statusCode': 200,
                    'headers': { 
                        'Content-Type': 'application/json',
                        'Access-Control-Allow-Origin': "*" ,
                        'Access-Control-Allow-Methods': '*',
                        "Access-Control-Allow-Credentials" : True
                    },
                    'body': json.dumps(res),
                    'isBase64Encoded': False
                }
    
    print ("response:", response)
    print ("I am triggered")
    
    return response2
