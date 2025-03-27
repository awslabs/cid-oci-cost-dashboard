import boto3
import os
import datetime
import json
import logging
import urllib3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def send_response(event, context, response_status, response_data):
    response_body = {
        'Status': response_status,
        'Reason': f'See CloudWatch Log Stream: {context.log_stream_name}',
        'PhysicalResourceId': context.log_stream_name,
        'StackId': event['StackId'],
        'RequestId': event['RequestId'],
        'LogicalResourceId': event['LogicalResourceId'],
        'Data': response_data
    }
    
    response_body = json.dumps(response_body)
    logger.info(f"Response body: {response_body}")
    
    http = urllib3.PoolManager()
    try:
        response = http.request(
            'PUT',
            event['ResponseURL'],
            headers={'Content-Type': ''},
            body=response_body
        )
        logger.info(f"Status code: {response.status}")
    except Exception as e:
        logger.info(f"Error sending response: {str(e)}")
        raise

def get_all_s3_objects(s3Client, bucket_name, prefix):
    s3_client = s3Client
    all_objects = []
    paginator = s3_client.get_paginator('list_objects_v2')
    
    for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
        if 'Contents' in page:
            all_objects.extend(page['Contents'])
            print(f"Number of Objects found in this page = {page['KeyCount']}")
            #print(f"Contents found in Bucket: {page['Contents']} ")
    return all_objects

def get_secrets(SecretId):
    secretresponses = {}
    client = boto3.client('secretsmanager')
    response = client.get_secret_value(SecretId=SecretId)
    secrets = response['SecretString']
    
    # Parse the JSON string from SecretString 
    secret_dict = json.loads(secrets)
    
    return secret_dict

def store_run_status(dynamodb_table_name, run_id, run_status):
        # Create a DynamoDB client
    dynamodb = boto3.client('dynamodb')
    
    # Create the DynamoDB table if it does not exist
    try:
        dynamodb.describe_table(TableName=dynamodb_table_name)
    except dynamodb.exceptions.ResourceNotFoundException:
        dynamodb.create_table(
            TableName=dynamodb_table_name,
            KeySchema=[
                {
                    'AttributeName': 'file_name',
                    'KeyType': 'HASH'
                }
            ],
            AttributeDefinitions=[
                {
                    'AttributeName': 'file_name',
                    'AttributeType': 'S'
                }
            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': 5,
                'WriteCapacityUnits': 5
            }
        )
    # Create a DynamoDB client
    dynamodb = boto3.client('dynamodb')

    # Get the current timestamp
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Prepare the item to be stored in DynamoDB
    item = {
        'run_id': {'S': run_id},
        'run_status': {'S': run_status},
        'timestamp': {'S': timestamp}
    }

    # Store the item in DynamoDB
    dynamodb.put_item(TableName=dynamodb_table_name, Item=item)

def print_files_s3bucket(s3_client, bucket_name, Prefix):
    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=Prefix)
    print("Files found in Bucket: "+bucket_name+" are: ")
    if 'Contents' in response:
        for obj in response['Contents']:
            #Print both the file name, path and the last modified date
            print(obj['Key'], obj['Size'], obj['LastModified'])
    else:
        print("No files found in the bucket "+bucket_name)    

def lambda_handler(event, context):
    
    #add a variable (boolean) for sending response back or not
    send_response_back = False
    if 'ResponseURL' in event:
        send_response_back = True

    logger.info(f"Received event: {json.dumps(event)}")

    try:
        if event['RequestType'] == 'Delete':
            # Handle stack deletion - cleanup if needed
            logger.info("Handling Delete request")
            send_response(event, context, 'SUCCESS', {'Message': 'Resource Deletion not in Scope'})
            return

        if event['RequestType'] in ['Create', 'Update']:
            # Get parameters from the event
            props = event['ResourceProperties']
            # Add your OCI to S3 sync logic here
            logger.info("Executing CFN trigger OCI to S3 sync...")

        #Handle event request type = Scheduled
        if event['RequestType'] == 'Scheduled':
            logger.info("Executing Scheduled Run for OCI to S3 sync...")

        cid_oci_secrets_name = os.environ['OracleSecretName']
        cid_oci_raw_s3 = os.environ['OciRawDataS3Bucket']
        cid_oci_s3_sync_start_date = os.environ['OCIToS3SyncStartDate']
        cid_oci_endpoint_url = os.environ['OracleEndpointURL']
        cid_oci_region = os.environ['OracleRegion']
        cid_oci_file_extension = os.environ['OCICopyFileExtension']
        cid_oci_sync_duration = os.environ['OCIToS3SyncDuration']
        cid_oci_sqs_queue = os.environ['OCISQSQueue']
        cid_oci_export_focus = os.environ['OCIFocusExport']
        cid_oci_export_standard = os.environ['OCIStandardExport']

        #Get Secret from CID OCI Secrets Manager
        cid_oci_secrets = get_secrets(cid_oci_secrets_name)
        # print(cid_oci_secrets)
        cid_oci_access_key_id = cid_oci_secrets['oracle_access_key_id']
        cid_oci_secret_access_key = cid_oci_secrets['oracle_secret_access_secret']
        cid_oci_bucket = cid_oci_secrets['oracle_bucket']

        # Check if the secret is valid
        if cid_oci_access_key_id is None or cid_oci_secret_access_key is None:
            print("Invalid secret.")
            return
                
        # Get the AWS Raw bucket Details
        s3_aws = boto3.client('s3')
        print_files_s3bucket(s3_aws, cid_oci_raw_s3, "")

        s3_oci = boto3.client('s3',
                                endpoint_url=cid_oci_endpoint_url,
                                region_name=cid_oci_region,
                                aws_access_key_id=cid_oci_access_key_id,
                                aws_secret_access_key=cid_oci_secret_access_key)
        
        #Check to download FOCUS / Stanard / Both 
        Prefix=""
        print(f"DEBUG ---- {cid_oci_export_focus},{cid_oci_export_standard}")
        if cid_oci_export_focus == 'Yes' and cid_oci_export_standard == 'Yes':
            Prefix = ""
        elif cid_oci_export_focus == 'Yes':
            Prefix =  "FOCUS Reports/"
        elif cid_oci_export_standard == 'Yes':
            Prefix = "reports/"
        
        # oci_result = s3_oci.list_objects(Bucket=cid_oci_bucket, Prefix=Prefix)
        # oci_files = oci_result.get("Contents")
        oci_files = get_all_s3_objects(s3_oci, cid_oci_bucket, Prefix)

        if oci_files is None:
            print("No files found in the OCI bucket.") #Need to put better contextual messages for multiple scenarios
            return
        else:
            print("Number of files found in the OCI bucket: ", len(oci_files))
            #print_files_s3bucket(s3_oci, cid_oci_bucket)
        
        # Get the timestamp to filter files. 'days' is the age filter and it only copies the files which are younger than the value specificied. 
        # It will only copy the files which have created/changed in the last x days
        timestamp = datetime.datetime.now() - datetime.timedelta(days=int(cid_oci_sync_duration))
        
        #File extension based filtering
        oci_filtered_files = [file for file in oci_files if file['Key'].endswith(cid_oci_file_extension) and 
                        datetime.datetime.strptime(file['LastModified'].strftime('%Y-%m-%d %H:%M:%S'), 
                        '%Y-%m-%d %H:%M:%S') > timestamp]

        print("Number of files found in the OCI bucket after filtering: ", len(oci_filtered_files))


        # Create SQS client
        sqs = boto3.client('sqs')

        # Process each file and send to SQS
        for file in oci_filtered_files:
            obj = s3_oci.get_object(Bucket=cid_oci_bucket, Key=file['Key'])
            data = obj['Body'].read()
            
            # Create message payload
            message = {
                'file_key': file['Key'],
                'file_size': file['Size'],
                'last_modified': file['LastModified'].strftime('%Y-%m-%d %H:%M:%S'),
                'source_bucket': cid_oci_bucket
            }

            # Send message to SQS
            try:
                response = sqs.send_message(
                    QueueUrl=cid_oci_sqs_queue,
                    MessageBody=json.dumps(message)
                )
                print(f"Message sent to SQS for file: {file['Key']}")
            except Exception as e:
                print(f"Error sending message to SQS for file {file['Key']}: {str(e)}")

        if send_response_back:
            # Send response back to CloudFormation
            send_response(event, context, 'SUCCESS', {'Message': 'Successfully executed OCI to S3 sync'})

    except Exception as e:
        logger.info(f"Error in lambda_handler: {str(e)}")
        if send_response_back:
            send_response(event, context, 'FAILED', {'Error': str(e)})
