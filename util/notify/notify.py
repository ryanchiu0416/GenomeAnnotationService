# notify.py
#
# Notify users of job completion
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
config.read('notify_config.ini')

'''Capstone - Exercise 3(d)
Reads result messages from SQS and sends notification emails.
'''
def handle_results_queue(sqs=None):
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

    job_id = result_dict['job_id']
    user_id = result_dict['user_id']
    profile = helpers.get_user_profile(id=user_id)
    detailPageURL = f"{config['gas']['DetailPageURLPrefix']}{job_id}"

    # send Email
    helpers.send_email_ses(recipients=profile['email'], sender=config['gas']['MailDefaultSender'],
                            subject=f'Annotation Job {job_id} is Completed', 
                            body=f'Job ID: {job_id}\nJob Detail URL: {detailPageURL}')


    # Delete message
    try:
      message.delete()
    except ClientError:
      print(e)

  

if __name__ == '__main__':
  
  # Get handles to resources; and create resources if they don't exist
  sqs = boto3.resource('sqs', region_name=config['aws']['AwsRegionName'])
  queue = sqs.get_queue_by_name(QueueName=config['sqs']['ResultQueueName'])

  # Poll queue for new results and process them
  while True:
    handle_results_queue(sqs=sqs)

### EOF
