# restore.py
#
# Restores thawed data, saving objects to S3 results bucket
# NOTE: This code is for an AWS Lambda function
#
# Copyright (C) 2011-2021 Vas Vasiliadis
# University of Chicago
##

import json
import boto3
from botocore.exceptions import ClientError
import json
from boto3.dynamodb.conditions import Key

VAULT = 'ucmpcs'
REGION = 'us-east-1'
DYNAMO_ANNTABLE = 'rpchiu_annotations'
S3_RESULT_BUCKET = 'gas-results'

def lambda_handler(event, context):
    currEvent = json.dumps(event)
    msg_dict = json.loads(json.loads(currEvent)['Records'][0]['Sns']['Message'])
    archive_id = msg_dict['ArchiveId']
    thaw_job_id = msg_dict['JobId']
    ann_job_id = msg_dict['JobDescription'].split("=")[1]
    dynamodb = boto3.resource('dynamodb', region_name=REGION)
    ann_table = dynamodb.Table(DYNAMO_ANNTABLE)

    try:
        response = ann_table.query(
            KeyConditionExpression=Key('job_id').eq(ann_job_id)
        )
    except ClientError as e:
        print(e)
    resp_dict = response['Items'][0]
    s3_key = resp_dict['s3_key_result_file'] # obtain s3 key
    


    # read data from restored file
    # refer to - https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glacier.html#Glacier.Client.get_job_output
    glacier = boto3.client('glacier', region_name=REGION)
    try:
        response = glacier.get_job_output(vaultName=VAULT, jobId=thaw_job_id)
    except ClientError as e:
        print(e)
        
        
    # upload to s3
    # refer to https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Object.upload_fileobj
    result_file_content = response['body']
    s3 = boto3.client('s3', region_name=REGION)
    try:
        s3.upload_fileobj(result_file_content, S3_RESULT_BUCKET, s3_key)
    except ClientError as e:
        print(e)
    
    
    # delete archive
    # refer to https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glacier.html#Glacier.Client.delete_archive
    try:
        response = glacier.delete_archive(vaultName=VAULT, archiveId=archive_id)
    except ClientError as e:
        print(e)
        
    
    # delete 'results_file_archive_id' & 'thaw_job_id' from dynamo record
    try:
        ann_table.update_item(
            Key={
                'job_id': ann_job_id,
            },
            UpdateExpression='REMOVE thaw_job_id, results_file_archive_id',
            
        )
    except ClientError as e:
        print(e)

### EOF