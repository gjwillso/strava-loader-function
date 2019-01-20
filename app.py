import requests
import os
import json
import psycopg2
import boto3
import base64
from chalice import Chalice
from botocore.exceptions import ClientError

app = Chalice(app_name='strava-loader-function')
app.debug = True

def get_secret():
    secret_name = "strava-token"
    region_name = "eu-west-1"

    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )

    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceNotFoundException':
            raise e

    else:
        if 'SecretString' in get_secret_value_response:
            secret = get_secret_value_response['SecretString']
            print ("obtaining secret")
            return secret
        else:
            decoded_binary_secret = base64.b64decode(get_secret_value_response['SecretBinary'])


@app.route('/strava')
def index():
    tokendata = json.loads(get_secret())   
    token = (tokendata['strava-token'])
    headers = {'Authorization': "Bearer {0}".format(token)}

    # need to add extraction steps in here to pull out Activity ID from webhook message body. 

    #id = 1986142961 # test commute id
    id = 2092155436 # test run id
    r = requests.get("https://www.strava.com/api/v3/activities/{0}".format(id), headers = headers)
    response = r.json()

    if response["commute"] == True:
        s3 = boto3.resource('s3')
        s3.Object('strava-loader', "strava-stats.json").put(Body=json.dumps(response))
    else:
        app.log.debug("Not a Commute Activity - Dropping")
  
@app.on_s3_event(bucket='strava-loader',events=['s3:ObjectCreated:*'])
def handle_s3_event(event):
    app.log.debug("Received event for bucket: %s, key: %s", event.bucket, event.key)
  
    s3 = boto3.client('s3')
    
    r = s3.select_object_content(
        Bucket='strava-loader',
        Key='strava-stats.json',
        ExpressionType='SQL',
        Expression="SELECT s FROM s3object s WHERE s.commute = true", 
        InputSerialization={'JSON': {"Type": "Lines"}},
        OutputSerialization={'JSON': {}}
    )

    for event in r['Payload']:
        if 'Records' in event:
            records = event['Records']['Payload'].decode('utf-8')
            s3 = boto3.resource('s3')
            s3.Object('strava-commute', "strava-commute-stats.json").put(Body=records)