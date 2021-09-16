# ann_load.py
#
# Copyright (C) 2011-2021 Vas Vasiliadis
# University of Chicago
#
# Exercises the annotator's auto scaling
#
##
__author__ = 'Vas Vasiliadis <vas@uchicago.edu>'

import uuid
import time
import sys
import json
import boto3
from botocore.exceptions import ClientError
from botocore.client import Config

# Define constants here; no config file is used for this script
USER_ID = "8d821b8d-6298-4001-979e-01820387b474"
EMAIL = "rpchiu@uchicago.edu"
REGION = 'us-east-1'
S3_INPUT_BUCKET = 'gas-inputs'
AWS_S3_KEY_PREFIX = 'rpchiu/'
AWS_DYNAMODB_ANNOTATIONS_TABLE = 'rpchiu_annotations'

"""Fires off annotation jobs with hardcoded data for testing
"""
def load_requests_queue():

  

  s3 = boto3.resource('s3', 
    region_name=REGION, 
    config=Config(signature_version='s3v4'))

  

  # Generate unique ID to be used as S3 key (name)
  s3_key = AWS_S3_KEY_PREFIX + USER_ID + '/' + \
      str(uuid.uuid4()) + '~test.vcf'
  # Parse redirect URL query parameters for S3 object info
  bucket_name = S3_INPUT_BUCKET

  # supposing `test.vcf` is in the same directory as this file.
  # IS REQUIRED FOR THIS TO SUCCESSFULLY RUN
  s3.Bucket(S3_INPUT_BUCKET).upload_file("test.vcf", s3_key)


  # # Extract the job ID from the S3 key
  job_id, filename = s3_key.split('/')[2].split('~')
  submit_time = int(time.time())

  # Persist job to database
  dynamodb = boto3.resource('dynamodb', region_name=REGION)
  data = {'job_id': job_id, 
          'user_id': USER_ID,
          'input_file_name': filename, 
          's3_inputs_bucket': bucket_name, 
          's3_key_input_file': s3_key, 
          'submit_time': submit_time,
          'job_status': 'PENDING'}

  ann_table = dynamodb.Table(AWS_DYNAMODB_ANNOTATIONS_TABLE)
  try:
      ann_table.put_item(Item=data)
  except ClientError as e:
      print(e)



  AWS_SNS_JOB_REQUEST_TOPIC = 'arn:aws:sns:us-east-1:127134666975:rpchiu_job_requests'
  # Send message to request queue
  try:
      sns = boto3.client('sns', REGION, 
                      config=Config(signature_version='s3v4'))
      response = sns.publish(TopicArn=AWS_SNS_JOB_REQUEST_TOPIC,
                              MessageStructure='json',
                              Message=json.dumps({'default': json.dumps(data)}))
  except ClientError as e:
      print(e)




if __name__ == '__main__':
  while True:
    try:
      load_requests_queue()
      time.sleep(3)
    except ClientError as e:
      print("Irrecoverable error. Exiting.")
      sys.exit()

### EOF