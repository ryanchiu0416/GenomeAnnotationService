# thaw_script.py
#
# Thaws upgraded (premium) user data
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
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key

# Get configuration
from configparser import ConfigParser
config = ConfigParser(os.environ)
config.read('thaw_script_config.ini')

'''Capstone - Exercise 9
Initiate thawing of archived objects from Glacier
'''
def handle_thaw_queue(sqs=None):
  try:
    messages = queue.receive_messages(WaitTimeSeconds=20)
  except ClientError:
    print(e)


  # Read a message from the queue
  for message in messages:
    # Process message
    try:
      result_dict = json.loads(json.loads(message.body)['Message'])
    except Exception:
      print(e)
    user_id_to_thaw = result_dict['user_id_to_thaw']

    # Get all archived jobs' archiveID to begin thawing process
    dynamodb = boto3.resource('dynamodb', region_name=config['aws']['AwsRegionName'])
    ann_table = dynamodb.Table(config['dynamodb']['AwsDynamoDbAnnotationsTable'])
    try:
      response = ann_table.query(
        IndexName = 'user_id_index',
        KeyConditionExpression=Key('user_id').eq(user_id_to_thaw)
      )
    except ClientError as e:
      print(e)

    # initiate thaws on each job that has been archived
    for job in response['Items']:
      if 'results_file_archive_id' in job:
        glacier = boto3.client('glacier', region_name=config['aws']['AwsRegionName'])
        
        isThawing = False
        try:
          initiate_thaw_resp = glacier.initiate_job(
            vaultName=config['glacier']['VaultName'],
            jobParameters={
                'Description': f"key={job['job_id']}", # using description field to pass ann job_id
                'SNSTopic': config['sns']['RestoreTopic'],
                'Type': 'archive-retrieval',
                'ArchiveId': job['results_file_archive_id'],
                'Tier': 'Expedited'
            }
          )
          isThawing = True
        except glacier.exceptions.InsufficientCapacityException as e:
          try:
            initiate_thaw_resp = glacier.initiate_job(
              vaultName=config['glacier']['VaultName'],
              jobParameters={
                  'Description': f"key={job['job_id']}", # using description field to ann job_id
                  'SNSTopic': config['sns']['RestoreTopic'],
                  'Type': 'archive-retrieval',
                  'ArchiveId': job['results_file_archive_id'],
                  'Tier': 'Standard'
              }
            )
            isThawing = True
          except ClientError as e:
            print(e)

        if isThawing:
          # update DB's 'results_file_archive_id' column to a flag.
          try:
            ann_table.update_item(
                Key={
                    'job_id': job['job_id'],
                },
                UpdateExpression='SET thaw_job_id = :a',
                ExpressionAttributeValues={
                    ':a': initiate_thaw_resp['jobId']
                }
            )
          except ClientError as e:
            print(e)



    # Delete message
    try:
        message.delete()
    except ClientError:
        print(e)

  pass

if __name__ == '__main__':  

  # Get handles to resources; and create resources if they don't exist
  sqs = boto3.resource('sqs', region_name=config['aws']['AwsRegionName'])
  queue = sqs.get_queue_by_name(QueueName=config['sqs']['ThawQueueName'])

  # Poll queue for new results and process them
  while True:
    handle_thaw_queue(sqs=sqs)

### EOF