import requests
import os
import json
import psycopg2
import boto3
import base64 
from chalice import Chalice, Response, NotFoundError
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
            return secret
        else:
            decoded_binary_secret = base64.b64decode(get_secret_value_response['SecretBinary'])

@app.route('/challenge', methods=['GET'])
def verify():
    
    # Webhook Subscription Handler. 
    request = app.current_request

    if request.query_params.get('hub.mode') and request.query_params.get('hub.verify_token') == 'STRAVA':
            response = json.dumps({
               'hub.challenge': request.query_params.get('hub.challenge')})
            return Response(body=response, status_code=200)
    else:
        app.log.debug("Invalid Request")

@app.route('/strava', methods=['POST'])
def index():
    
        tokendata = json.loads(get_secret())   
        token = (tokendata['strava-token'])
        headers = {'Authorization': "Bearer {0}".format(token)}

        webhook_body = app.current_request.json_body

        if webhook_body['aspect_type'] == 'create':
            print (webhook_body)
            app.log.info("Processing New Strava Actvity...")
            id  = webhook_body['object_id']
            r = requests.get("https://www.strava.com/api/v3/activities/{0}".format(id), headers = headers)
            activity = r.json()
            print (activity)

            if activity["type"] == 'Run':
                app.log.info("Copying Running Activity to S3...")
                s3 = boto3.resource('s3')
                s3.Object('run-stats', "strava-run-stats.json").put(Body=json.dumps(activity))
       
            elif activity["type"] == 'Ride':
                app.log.info("Copying Cycling Activity to S3...")
                s3 = boto3.resource('s3')
                s3.Object('ride-stats', "strava-ride-stats.json").put(Body=json.dumps(activity))
            
            elif activity["type"] == 'Rowing':
                app.log.info("Copying Rowing Activity to S3...")
                s3 = boto3.resource('s3')
                s3.Object('row-stats', "strava-row-stats.json").put(Body=json.dumps(activity))
            
            else: 
                app.log.debug("Dropping Random Activity...")
                # need to handle other sports
        else:
            app.log.debug("Dropping Update / Delete Activity for now...")
            
#@app.on_s3_event(bucket='strava-loader',events=['s3:ObjectCreated:*'])
#def handle_s3_event(event):
#    app.log.debug("Received event for bucket: %s, key: %s", event.bucket, event.key)
#  
#    s3 = boto3.client('s3')
    
#    r = s3.select_object_content(
#        Bucket='strava-loader',
#        Key='strava-stats.json',
#        ExpressionType='SQL',
#        Expression="SELECT s FROM s3object s WHERE s.commute = true", 
#        InputSerialization={'JSON': {"Type": "Lines"}},
#        OutputSerialization={'JSON': {}}
#    )

#    for event in r['Payload']:
#        if 'Records' in event:
#            records = event['Records']['Payload'].decode('utf-8')
#            s3 = boto3.resource('s3')
#            s3.Object('strava-commute', "strava-commute-stats.json").put(Body=records)
