# archive_script.py
#
# Archive free user data
#
# Copyright (C) 2011-2021 Vas Vasiliadis
# University of Chicago
##
__author__ = 'Vas Vasiliadis <vas@uchicago.edu>'

import boto3
import time
import os
import sys
import json
import psycopg2
from botocore.exceptions import ClientError

# Import utility helpers
sys.path.insert(1, os.path.realpath(os.path.pardir))
import helpers

# Get configuration
from configparser import ConfigParser
config = ConfigParser(os.environ)
config.read('archive_script_config.ini')

'''Capstone - Exercise 7
Archive free user results files
'''
def handle_archive_queue(sqs=None):
  try:
    messages = queue.receive_messages(WaitTimeSeconds=20)
  except ClientError:
    print(e)

  # Read a message from the queue
  for message in messages:
    try:
      result_dict = json.loads(json.loads(message.body)['Message'])
    except Exception:
      print(e)
    
    # Process message
    username = result_dict['Username']
    s3_result_key = result_dict['S3ResultKey']
    job_id = result_dict['JobId']

    user_profile = helpers.get_user_profile(username)
    if user_profile['role'] == 'free_user':
      
      # get s3 result file
      s3 = boto3.resource('s3', region_name=config['aws']['AwsRegionName'])
      try:
        s3_file_obj = s3.Object(config['s3']['S3BucketName'], s3_result_key)
        target_result_file = s3_file_obj.get()['Body'].read()
      except ClientError as e:
        print(e)


      # upload to glacier
      # refer to - https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glacier.html#Glacier.Client.upload_archive
      glacier = boto3.client('glacier', region_name=config['aws']['AwsRegionName'])
      try:
        response = glacier.upload_archive(vaultName=config['glacier']['VaultName'], body=target_result_file)
        archive_id = response['archiveId']
      except ClientError as e:
        print(e)

      # update dynamo DB
      dynamodb = boto3.resource('dynamodb', region_name=config['aws']['AwsRegionName'])
      ann_table = dynamodb.Table(config['dynamodb']['AwsDynamoDbAnnotationsTable'])
      try:
        ann_table.update_item(
            Key={
                'job_id': job_id
            },
            UpdateExpression='SET results_file_archive_id = :a',
            ExpressionAttributeValues={
                ':a': archive_id
            }
        )
      except ClientError as e:
        print(e)


      # delete result vcf file from s3
      # refer to - https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Object.delete
      try:
        s3_file_obj.delete()
      except ClientError as e:
        print(e)


    # Delete message
    try:
        message.delete()
    except ClientError:
        print(e)


if __name__ == '__main__':  
  # Get handles to resources; and create resources if they don't exist
  sqs = boto3.resource('sqs', region_name=config['aws']['AwsRegionName'])
  queue = sqs.get_queue_by_name(QueueName=config['sqs']['ArchiveQueueName'])

  # Poll queue for new results and process them
  while True:
    handle_archive_queue(sqs=sqs)

### EOF