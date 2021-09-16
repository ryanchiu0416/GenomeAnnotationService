# run.py
#
# Copyright (C) 2011-2019 Vas Vasiliadis
# University of Chicago
#
# Wrapper script for running AnnTools
#
##
__author__ = 'Vas Vasiliadis <vas@uchicago.edu>'

import sys
import time
import driver

import os
import boto3
import botocore
import time
from botocore.client import Config
import json



from configparser import ConfigParser
config = ConfigParser(os.environ)
parentPath = os.path.abspath(os.path.join(os.path.abspath(os.path.dirname(__file__)), os.pardir))
config.read(os.path.join(parentPath, 'ann_config.ini'))


"""A rudimentary timer for coarse-grained profiling
"""
class Timer(object):
  def __init__(self, verbose=True):
    self.verbose = verbose

  def __enter__(self):
    self.start = time.time()
    return self

  def __exit__(self, *args):
    self.end = time.time()
    self.secs = self.end - self.start
    if self.verbose:
      print(f"Approximate runtime: {self.secs:.2f} seconds")

if __name__ == '__main__':
  # Call the AnnTools pipeline
  if len(sys.argv) > 1:
    with Timer():
      driver.run(sys.argv[1], 'vcf')
    

      completion_time = int(time.time())
      job_id = sys.argv[2]
      username = sys.argv[3]
      bucket = config['aws']['AwsS3ResultsBucket']
      prefix = config['aws']['AwsS3KeyPrefix']
      complete_prefix = f'{prefix}{username}'


      # upload to S3
      s3 = boto3.client('s3', region_name=config['aws']['AwsRegionName'])
      fileKey = {}
      for file in os.listdir():
        curr_job_id = file.split('~')[0]
        if (file.endswith('annot.vcf') or file.endswith('vcf.count.log')) and curr_job_id == job_id:
          try:
            key = f'{complete_prefix}/{file}'
            if file.endswith('annot.vcf'):
              fileKey['result'] = key
            else:
              fileKey['log'] = key
            s3.upload_file(file, bucket, key)
          except botocore.exceptions.ClientError as e:
            raise(e)
      
      # update db records
      dynamodb = boto3.resource('dynamodb', region_name=config['aws']['AwsRegionName'])
      ann_table = dynamodb.Table(config['aws']['AwsDynamoDbAnnotationsTable'])
      try:
        ann_table.update_item(
            Key={
                'job_id': job_id
            },
            UpdateExpression='SET job_status = :s, s3_key_result_file = :r, s3_key_log_file = :l,' + 
              ' complete_time = :c, s3_results_bucket = :b',
            ConditionExpression='job_status = :t',
            ExpressionAttributeValues={
                ':s': 'COMPLETED',
                ':r': fileKey['result'],
                ':l': fileKey['log'],
                ':c': completion_time,
                ':b': bucket,
                ':t': 'RUNNING'
            }
        )
      except botocore.exceptions.ClientError as e:
        raise(e)




      # publish notification to results topic
      data = {'user_id':username,
              'job_id': job_id}
      sns = boto3.client('sns', region_name=config['aws']['AwsRegionName'], 
                          config=Config(signature_version='s3v4'))
      try:
        response = sns.publish(TopicArn=config['aws']['AwsSnsResultTopic'],
                               MessageStructure='json',
                               Message=json.dumps({'default': json.dumps(data)}))
      except botocore.exceptions.ClientError as e:
        raise(e)


      # send to SFN, which then publishes notification to results_archive topic to archive for free-users after 5 min expires
      # https://boto3.amazonaws.com/v1/documentation/api/1.9.46/reference/services/stepfunctions.html#SFN.Client.start_execution
      sfn = boto3.client('stepfunctions', region_name=config['aws']['AwsRegionName'], 
                          config=Config(signature_version='s3v4'))

      try:
        response = sfn.start_execution(
          stateMachineArn=config['aws']['AwsSnsArchiveTopic'],
          name=job_id,
          input='{\"timer_seconds\": ' + config['gas']['GasFreeUserDownloadTimeFrameSec'] +
           ', \"Username\": \"' + username + '\", \"S3ResultKey\": \"' + fileKey['result'] + 
           '\", \"JobId\": \"' + job_id + '\"}'
        )
      except botocore.exceptions.ClientError as e:
        raise(e)



      # cleanup relevant files
      for file in os.listdir():
        curr_job_id = file.split('~')[0]
        if curr_job_id == job_id:
          os.remove(file) 

  else:
    print("A valid .vcf file must be provided as input to this program.")

### EOF