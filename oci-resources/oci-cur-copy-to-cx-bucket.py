import io
import json
import logging
import oci
from datetime import datetime, timedelta
from fdk import response

# nosemgrep: logging-error-without-handling Message # OCI Owned code - Cannot update
def handler(ctx, data: io.BytesIO = None):
    try:
        # Do not modify these values
        reporting_namespace = 'bling'
        reporting_bucket = 'ocid1.tenancy.oc1..xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx' #Change to CX Tenancy
        yesterday = datetime.now() - timedelta(days=30) #Change to the number of days in past to include CUR FOCUS objects
        prefix_file = f"FOCUS Reports/{yesterday.year}/{yesterday.strftime('%m')}/{yesterday.strftime('%d')}"
        print(f"prefix is {prefix_file}")
        destination_path = '/tmp'   # nosec B108
        
        dest_namespace='XXXXXXXXXXXXXXX' #Change to Cx Objectstorage namespace | https://docs.oracle.com/en-us/iaas/Content/Object/Tasks/understandingnamespaces.htm
        upload_bucket_name = 'Cost_Usage_Reports'  # Replace with your target bucket for uploads


        
        Signer = oci.auth.signers.get_resource_principals_signer() 
        object_storage = oci.object_storage.ObjectStorageClient(config={}, signer=Signer)
        report_bucket_objects = oci.pagination.list_call_get_all_results(object_storage.list_objects, reporting_namespace, reporting_bucket, prefix=prefix_file)
        
        for o in report_bucket_objects.data.objects:
            object_details = object_storage.get_object(reporting_namespace, reporting_bucket, o.name)
            filename = o.name.rsplit('/', 1)[-1]
            local_file_path = destination_path+'/'+filename            
            with open(local_file_path, 'wb') as f:
                for chunk in object_details.data.raw.stream(1024 * 1024, decode_content=False):
                    f.write(chunk)
            with open(local_file_path, 'rb') as file_content:
                object_storage.put_object(
                    namespace_name=dest_namespace,
                    bucket_name=upload_bucket_name,
                    object_name=filename,
                    put_object_body=file_content
                )
    except (Exception, ValueError) as ex:
        logging.getLogger().info('error parsing payload: ' + str(ex))
    return response.Response(
        ctx, response_data=json.dumps(
            {"message": "Processed Files sucessfully"})
    )