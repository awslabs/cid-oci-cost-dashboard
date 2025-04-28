# Cloud Intelligence Dashboard for CID OCI FOCUS -- Glue Script

### Glue base
import sys
import boto3
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
import os
from pyspark.sql.functions import input_file_name
from pyspark.sql.functions import col, to_timestamp, date_format, to_date, coalesce
from pyspark.sql.types import *
from awsglue.dynamicframe import DynamicFrame
# Transform Tags column as map
from pyspark.sql.functions import col, udf
from pyspark.sql.types import ArrayType, StringType, MapType
import json
import concurrent.futures
import logging

### Copy Function
def copy_s3_objects(source_bucket, source_folder, destination_bucket, destination_folder, max_workers=10):
    """
    Concurrently copy objects from one S3 location to another with pagination support.
    
    Args:
        source_bucket (str): Source S3 bucket name
        source_folder (str): Source folder/prefix in the bucket
        destination_bucket (str): Destination S3 bucket name
        destination_folder (str): Destination folder/prefix in the bucket
        max_workers (int): Maximum number of concurrent threads
    """
    s3_client = boto3.client('s3')
    logger = logging.getLogger(__name__)
    def copy_object(obj):
        try:
            copy_source = {
                'Bucket': source_bucket,
                'Key': obj['Key']
            }
            target_key = obj['Key'].replace(source_folder, destination_folder, 1)
            
            s3_client.copy_object(
                Bucket=destination_bucket,
                Key=target_key,
                CopySource=copy_source,
                TaggingDirective='COPY'
            )
            logger.info(f"Successfully copied: {obj['Key']} to {target_key}")
            return True
            
        except ClientError as e:
            logger.error(f"Error copying {obj['Key']}: {str(e)}")
            return False

    # Initialize paginator for handling large numbers of objects
    paginator = s3_client.get_paginator('list_objects_v2')
    page_iterator = paginator.paginate(Bucket=source_bucket, Prefix=source_folder)
    
    total_copied = 0
    total_failed = 0
    
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            for page in page_iterator:
                if 'Contents' not in page:
                    logger.info(f"No objects found in {source_folder}")
                    continue
                
                # Submit all objects in the current page to the thread pool
                future_to_key = {
                    executor.submit(copy_object, obj): obj['Key'] 
                    for obj in page['Contents']
                }
                
                # Process completed futures
                for future in concurrent.futures.as_completed(future_to_key):
                    key = future_to_key[future]
                    try:
                        if future.result():
                            total_copied += 1
                        else:
                            total_failed += 1
                    except Exception as e:
                        logger.info(f"Unexpected error copying {key}: {str(e)}")
                        total_failed += 1
        
        logger.info(f"Copy process complete. Successfully copied: {total_copied}, Failed: {total_failed}")
        return total_copied, total_failed
        
    except Exception as e:
        logger.info(f"Error during copy process: {str(e)}")
        raise

### Delete Function
def delete_s3_folder(bucket, folder):
    s3_client = boto3.client('s3')
    
    # Use paginator for handling large number of objects
    paginator = s3_client.get_paginator('list_objects_v2')
    delete_objects = []
    
    try:
        # Iterate through pages of objects
        for page in paginator.paginate(Bucket=bucket, Prefix=folder):
            if 'Contents' in page:
                # Batch objects for deletion (max 1000 per request)
                delete_objects.extend([{'Key': obj['Key']} for obj in page['Contents']])
                
                # Delete in batches of 1000 (AWS limit)
                while delete_objects:
                    batch = delete_objects[:1000]
                    s3_client.delete_objects(
                        Bucket=bucket,
                        Delete={'Objects': batch}
                    )
                    delete_objects = delete_objects[1000:]
                print("INFO: Delete process complete")
            else:
                print(f"INFO: No files in {folder}, delete process skipped.")
    except Exception as e:
        print(f"ERROR: Failed to delete objects from {folder}: {str(e)}")

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

### Parameters fetched from Glue Job
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'source_bucket', 'source_prefix', 'destination_bucket', 'destination_prefix', 'focus_prefix', 'glue_database','glue_table', 'glue_noniso_format'])
print(f"Args:{args}")

###Capturing Variable from Job Arguments
source_bucket = args['source_bucket']
source_prefix = args['source_prefix']
destination_bucket = args['destination_bucket']
destination_prefix = args['destination_prefix']
glue_database = args['glue_database']
glue_table = args['glue_table']
oci_date_format = args['glue_noniso_format']
raw_folder = args['focus_prefix']
raw_bucket=args['source_bucket']
#raw_folder = "FOCUS Reports"

processed_bucket=args['source_bucket']
processed_folder=f"processed/{raw_folder}"

error_bucket=args['source_bucket']
error_folder=f"error/{raw_folder}"

# Appending Full Path for source/FOCUS Report/Year/Month/Date/all-objects and destination/FOCUS2
source_bucket = f"s3://{source_bucket}/{source_prefix}"
destination_bucket = f"s3://{destination_bucket}/{destination_prefix}/"

### Read CSV and Prepare DataFrame 1
try:
    df1 = spark.read \
                .option("header","true") \
                .option("delimiter",",") \
                .option("escape", "\"") \
                .option("compression", "gzip") \
                .csv(source_bucket)
    df1 = df1.withColumn("file_path", input_file_name())
except Exception as e:
    print(f"WARNING: Cannot read CSV file(s) in {source_bucket}. Incorrect path or folder empty.")
    print(f"ERROR: {e}")
    raise e

print("Initial Schema found ==================")
df1.printSchema()
print("=======================================")


print("Adding Partiotion of BILLING_PERIOD====")
try:
    df1 = df1.withColumn("BILLING_PERIOD", date_format("BillingPeriodStart", "yyyy-MM"))
except Exception as e:
    # If the CSV cannot be processed move to error folder
    copy_s3_objects(raw_bucket, raw_folder, error_bucket, error_folder)
    delete_s3_folder(raw_bucket, raw_folder)
    print("Error Happened during Partition Column Addition of BILLING_PERIOD")
    print(f"ERROR: {e}")
    raise e
print("=======================================")

### Remove these Debug Statements (after testing)
# print(df1)
# df1.select("BILLING_PERIOD").distinct().show(10, truncate=False)
# df1.printSchema()

print("Update Date and Cost columns to Timestamp, Double====")
try:
    df2 = df1.withColumn("BilledCost", col("BilledCost").cast(DoubleType())) \
            .withColumn("BillingPeriodEnd", to_timestamp(col("BillingPeriodEnd"))) \
            .withColumn("BillingPeriodStart", to_timestamp(col("BillingPeriodStart"))) \
            .withColumn("EffectiveCost", col("EffectiveCost").cast(DoubleType())) \
            .withColumn("ListCost", col("ListCost").cast(DoubleType())) \
            .withColumn("ListUnitPrice", col("ListUnitPrice").cast(DoubleType())) \
            .withColumn("PricingQuantity", col("PricingQuantity").cast(DoubleType())) \
            .withColumn("UsageQuantity",  col("PricingQuantity").cast(DoubleType())) \
            .withColumn("oci_attributedcost", col("oci_attributedcost").cast(DoubleType()))

#Fix for Issue 10 - Some Customer are seeing yyyy-MM-dd'T'HH:mm:ss.SSS'Z' format for IDCS entries --- Start

            #Uncomment this section if Issue10 is not applicable
            # .withColumn("ChargePeriodEnd", to_timestamp(col("ChargePeriodEnd"), oci_date_format)) \
            # .withColumn("ChargePeriodStart", to_timestamp(col("ChargePeriodStart"), oci_date_format)) \

    #Generate ChargeperiodStart, ChargePeriodEnd with all formats
    df2 = df2 \
        .withColumn("ChargePeriodStart_format1", to_timestamp(col("ChargePeriodStart"), "yyyy-MM-dd'T'HH:mm'Z'")) \
        .withColumn("ChargePeriodStart_format2", to_timestamp(col("ChargePeriodStart"), "yyyy-MM-dd'T'HH:mm:ss'Z'")) \
        .withColumn("ChargePeriodStart_format3", to_timestamp(col("ChargePeriodStart"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")) \
        .withColumn("ChargePeriodEnd_format1", to_timestamp(col("ChargePeriodEnd"), "yyyy-MM-dd'T'HH:mm'Z'")) \
        .withColumn("ChargePeriodEnd_format2", to_timestamp(col("ChargePeriodEnd"), "yyyy-MM-dd'T'HH:mm:ss'Z'")) \
        .withColumn("ChargePeriodEnd_format3", to_timestamp(col("ChargePeriodEnd"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"))            

    #Pick the ones which match not null values
    df2 = df2 \
        .withColumn("ChargePeriodStart", 
                coalesce(
                    col("ChargePeriodStart_format3"),  # Try milliseconds format first
                    col("ChargePeriodStart_format2"),  # Then seconds format
                    col("ChargePeriodStart_format1")   # Then minutes format
                )) \
        .withColumn("ChargePeriodEnd", 
                coalesce(
                    col("ChargePeriodEnd_format3"),  # Try milliseconds format first
                    col("ChargePeriodEnd_format2"),  # Then seconds format
                    col("ChargePeriodEnd_format1")   # Then minutes format
                ))               
#Fix for Issue 10 - Some Customer are seeing yyyy-MM-dd'T'HH:mm:ss.SSS'Z' format for IDCS entries --- End

except Exception as e:
    # If the CSV cannot be processed move to error folder
    copy_s3_objects(raw_bucket, raw_folder, error_bucket, error_folder)
    delete_s3_folder(raw_bucket, raw_folder)
    print("WARNING: Cannot parse columns. Error in CSV file(s). Moved to error folder")
    print(f"ERROR: {e}")
    raise e
print(df2)
print("=======================================")

print("Updating Tags (String) to Tags (Map<String:String>)============")  
# Function handle JSON or instances where curly braces are missing from tag column.
def transform_to_map(resource_tags):
    if resource_tags:
        if resource_tags.startswith('{'):
            return dict(json.loads(resource_tags))
        else:
            return dict(json.loads("{" + resource_tags + "}"))

tagsTransformToMapUDF = udf(lambda x:transform_to_map(x), MapType(StringType(), StringType()))
df2 = df2.withColumn("Tags", tagsTransformToMapUDF(col("Tags")))
print("==============================================================")  

print("Submitting Schema to S3 and Creating Glue Table ==================")
try:
    dyf3 = DynamicFrame.fromDF(df2, glueContext, "dyf3")
    sink = glueContext.getSink(connection_type="s3",path=(destination_bucket),enableUpdateCatalog=True,partitionKeys=["BILLING_PERIOD"])
    sink.setFormat("glueparquet")
    sink.setCatalogInfo(catalogDatabase=(glue_database), catalogTableName=(glue_table))
    sink.writeFrame(dyf3)
except Exception as e:
    copy_s3_objects(raw_bucket, raw_folder, error_bucket, error_folder)
    delete_s3_folder(raw_bucket, raw_folder)
    print("WARNING: Cannot convert file(s) to parquet. Moved to error folder if normal run")
    print(f"ERROR: {e}")
    raise e
print("==================================================================")

copy_s3_objects(raw_bucket, raw_folder, processed_bucket, processed_folder)
delete_s3_folder(raw_bucket, raw_folder)