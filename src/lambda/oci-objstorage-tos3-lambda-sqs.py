import boto3
import os
import datetime
import json
import logging
import urllib3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

#Write code for a function that accepts a string array of secret keys and return secret values in key pair format of secret
# key and secret value. The secret name is oci-secrets
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
    
    cid_oci_secrets_name = os.environ['OracleSecretName']
    cid_oci_raw_s3 = os.environ['OciRawDataS3Bucket']
    cid_oci_s3_sync_start_date = os.environ['OCIToS3SyncStartDate']
    cid_oci_endpoint_url = os.environ['OracleEndpointURL']
    cid_oci_region = os.environ['OracleRegion']
    cid_oci_file_extension = os.environ['OCICopyFileExtension']
    cid_oci_sync_duration = os.environ['OCIToS3SyncDuration']

    # Add SQS queue URL environment variable
    #cid_oci_sqs_queue = os.environ['OCISQSQueue']

    #Get Secret from CID OCI Secrets Manager
    cid_oci_secrets = get_secrets(cid_oci_secrets_name)
    cid_oci_access_key_id = cid_oci_secrets['oracle_access_key_id']
    cid_oci_secret_access_key = cid_oci_secrets['oracle_secret_access_secret']
    cid_oci_bucket = cid_oci_secrets['oracle_bucket']

    # Check if the secret is valid
    if cid_oci_access_key_id is None or cid_oci_secret_access_key is None:
        print("Invalid secret.")
        return
            
    # Get the AWS Raw bucket Details
    s3_aws = boto3.client('s3')
    # print_files_s3bucket(s3_aws, cid_oci_raw_s3, "")

    s3_oci = boto3.client('s3',
                            endpoint_url=cid_oci_endpoint_url,
                            region_name=cid_oci_region,
                            aws_access_key_id=cid_oci_access_key_id,
                            aws_secret_access_key=cid_oci_secret_access_key)

    batch_item_failures = []
    sqs_batch_response = {}
    
    # Process each record from SQS
    for record in event['Records']:
        try:
            # Parse the message body
            message = json.loads(record['body'])
            
            # Get the message ID
            message_id = record['messageId']

            # Extract file information
            file_key = message['file_key']
            source_bucket = message['source_bucket']
            last_modified = message['last_modified']
            file_size = message['file_size']
            
            print(f"Processing file: {file_key} from bucket: {source_bucket}")
                        
            # Prepare metadata
            metadata = {
                'source-bucket': source_bucket,
                'last-modified': last_modified,
                'original-size': str(file_size)
            }
            

            # Upload CUR objects to AWS S3 Bucket
            try:
                
                obj = s3_oci.get_object(Bucket=cid_oci_bucket, Key=file_key)
                data = obj['Body'].read()
                # Replicate the AWS S3 bucket folder structure to the OCI bucket
                folder_structure = os.path.dirname(file_key)
                oci_key = folder_structure + '/' + os.path.basename(file_key)
                response=s3_aws.put_object(Bucket=cid_oci_raw_s3, Key=oci_key, Body=data)
                
                # Check if upload was successful
                if response['ResponseMetadata']['HTTPStatusCode'] == 200:
                    print(f"Successfully uploaded {file_key} to {cid_oci_bucket}")
                else:
                    print(f"Upload failed with {file_key} to {cid_oci_bucket} with response {response}")
                    raise Exception(f"Upload failed with response: {response}")
                    
            except Exception as e:
                logger.info(f"Error uploading file {file_key} to S3: {str(e)}")
                logger.info(f"Unexpected error processing record: {str(e)}")
                batch_item_failures.append({"itemIdentifier": record['messageId']})
                raise e
            
        except Exception as e:
            logger.info(f"Unexpected error processing record: {str(e)}")
            batch_item_failures.append({"itemIdentifier": record['messageId']})
        
    #Check if batch_item_failures is empty or not
    if not batch_item_failures:
        print("All records processed successfully")
    elif batch_item_failures:
        sqs_batch_response["batchItemFailures"] = batch_item_failures
        return sqs_batch_response

    return {
        'statusCode': 200,
        'body': json.dumps('Processing complete')
    }