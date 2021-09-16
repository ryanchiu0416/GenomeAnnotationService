# views.py
#
# Copyright (C) 2011-2020 Vas Vasiliadis
# University of Chicago
#
# Application logic for the GAS
#
##
__author__ = 'Vas Vasiliadis <vas@uchicago.edu>'

import uuid
import time
import json
from datetime import datetime

import boto3
from botocore.client import Config
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError

from flask import (abort, flash, redirect, render_template, 
  request, session, url_for)

from gas import app, db
from decorators import authenticated, is_premium

"""Start annotation request
Create the required AWS S3 policy document and render a form for
uploading an annotation input file using the policy document

Note: You are welcome to use this code instead of your own
but you can replace the code below with your own if you prefer.
"""
@app.route('/annotate', methods=['GET'])
@authenticated
def annotate():
  # Open a connection to the S3 service
  s3 = boto3.client('s3', 
    region_name=app.config['AWS_REGION_NAME'], 
    config=Config(signature_version='s3v4'))

  bucket_name = app.config['AWS_S3_INPUTS_BUCKET']
  user_id = session['primary_identity']

  # Generate unique ID to be used as S3 key (name)
  key_name = app.config['AWS_S3_KEY_PREFIX'] + user_id + '/' + \
    str(uuid.uuid4()) + '~${filename}'

  # Create the redirect URL
  redirect_url = str(request.url) + "/job"

  # Define policy conditions
  encryption = app.config['AWS_S3_ENCRYPTION']
  acl = app.config['AWS_S3_ACL']
  fields = {
    "success_action_redirect": redirect_url,
    "x-amz-server-side-encryption": encryption,
    "acl": acl
  }
  conditions = [
    ["starts-with", "$success_action_redirect", redirect_url],
    {"x-amz-server-side-encryption": encryption},
    {"acl": acl}
  ]

  # Generate the presigned POST call
  try:
    presigned_post = s3.generate_presigned_post(
      Bucket=bucket_name, 
      Key=key_name,
      Fields=fields,
      Conditions=conditions,
      ExpiresIn=app.config['AWS_SIGNED_REQUEST_EXPIRATION'])
  except ClientError as e:
    app.logger.error(f'Unable to generate presigned URL for upload: {e}')
    return abort(500)

  # Render the upload form which will parse/submit the presigned POST
  return render_template('annotate.html',
    s3_post=presigned_post,
    role=session['role'])


"""Fires off an annotation job
Accepts the S3 redirect GET request, parses it to extract 
required info, saves a job item to the database, and then
publishes a notification for the annotator service.

Note: Update/replace the code below with your own from previous
homework assignments
"""
@app.route('/annotate/job', methods=['GET'])
@authenticated
def create_annotation_job_request():

  region = app.config['AWS_REGION_NAME']

  # Parse redirect URL query parameters for S3 object info
  bucket_name = request.args.get('bucket')
  s3_key = request.args.get('key')

  # Extract the job ID from the S3 key
  job_id, filename = s3_key.split('/')[2].split('~')
  user_id = session['primary_identity']
  submit_time = int(time.time())

  # Persist job to database
  dynamodb = boto3.resource('dynamodb', region_name=app.config['AWS_REGION_NAME'])
  data = {'job_id': job_id, 
          'user_id': user_id,
          'input_file_name': filename, 
          's3_inputs_bucket': bucket_name, 
          's3_key_input_file': s3_key, 
          'submit_time': submit_time,
          'job_status': 'PENDING'}

  ann_table = dynamodb.Table(app.config['AWS_DYNAMODB_ANNOTATIONS_TABLE'])
  try:
      ann_table.put_item(Item=data)
  except ClientError as e:
      return abort(500)

  # Send message to request queue
  try:
      sns = boto3.client('sns', region_name=app.config['AWS_REGION_NAME'], 
                        config=Config(signature_version='s3v4'))
      response = sns.publish(TopicArn=app.config['AWS_SNS_JOB_REQUEST_TOPIC'],
                             MessageStructure='json',
                             Message=json.dumps({'default': json.dumps(data)}))
  except ClientError as e:
      return abort(500)

  return render_template('annotate_confirm.html', job_id=job_id)




"""List all annotations for the user
"""
@app.route('/annotations', methods=['GET'])
@authenticated
def annotations_list():
  '''
    refer to: 
    https://stackoverflow.com/questions/35758924/how-do-we-query-on-a-secondary-index-of-dynamodb-using-boto3
  '''


  # Get list of annotations to display
  dynamodb = boto3.resource('dynamodb', region_name=app.config['AWS_REGION_NAME'])
  ann_table = dynamodb.Table(app.config['AWS_DYNAMODB_ANNOTATIONS_TABLE'])
  
  try:
    response = ann_table.query(
      IndexName = 'user_id_index',
      KeyConditionExpression=Key('user_id').eq(session['primary_identity'])
    )
  except ClientError as e:
    print(e)
    return abort(500)

  # preprocess data
  annotations = []
  for item in response['Items']:
    t = convert_epoch_to_localtime(item['submit_time'])
    annotations.append([str(request.url) + f"/{item['job_id']}", item['job_id'], t,
        item['input_file_name'], item['job_status']])
  return render_template('annotations.html', annotations=annotations)






"""Display details of a specific annotation job
"""
@app.route('/annotations/<id>', methods=['GET'])
@authenticated
def annotation_details(id):
  #  get detail of the job
  dynamodb = boto3.resource('dynamodb', region_name=app.config['AWS_REGION_NAME'])
  ann_table = dynamodb.Table(app.config['AWS_DYNAMODB_ANNOTATIONS_TABLE'])
  
  try:
    response = ann_table.query(
      KeyConditionExpression=Key('job_id').eq(id)
    )
  except ClientError as e:
    print(e)
    return abort(500)

  if len(response['Items']) != 1:
    return abort(500)
  elif response['Items'][0]['user_id'] != session['primary_identity']:
    return abort(403)

  resp_dict = response['Items'][0]
  output_detail_lst = []
  

  s3 = boto3.client('s3', 
      region_name=app.config['AWS_REGION_NAME'], 
      config=Config(signature_version='s3v4'))

  # generate presigned URL for download
  # refer to https://boto3.amazonaws.com/v1/documentation/api/latest/guide/s3-presigned-urls.html
  try:
    input_presigned_url = s3.generate_presigned_url('get_object',
                                                Params={'Bucket': app.config['AWS_S3_INPUTS_BUCKET'],
                                                        'Key': resp_dict['s3_key_input_file']},
                                                ExpiresIn=app.config['AWS_SIGNED_REQUEST_EXPIRATION'])
  except ClientError as e:
    app.logger.error(f'Unable to generate input presigned URL for download: {e}')
    return abort(500)


  job_detail_ID_lst = [('RequestID', resp_dict['job_id']),
                    ('Request Time', convert_epoch_to_localtime(resp_dict['submit_time']))]
  input_tup = ('VCF Input File', resp_dict['input_file_name'], input_presigned_url)
  job_detail_Status_lst = [('Status', resp_dict['job_status'])]

  # if job completed, add complete time and output file detail
  if 'complete_time' in resp_dict:
    job_detail_Status_lst.append(('Complete Time', convert_epoch_to_localtime(resp_dict['complete_time'])))

    
    
    try:
      presigned_url = s3.generate_presigned_url('get_object',
                                                Params={'Bucket': app.config['AWS_S3_RESULTS_BUCKET'],
                                                        'Key': resp_dict['s3_key_result_file']},
                                                ExpiresIn=app.config['AWS_SIGNED_REQUEST_EXPIRATION'])
    except ClientError as e:
      app.logger.error(f'Unable to generate result presigned URL for download: {e}')
      return abort(500)


    # check if 5 min has passed (db look up for glacier column?)
    if 'results_file_archive_id' in resp_dict:
      if 'thaw_job_id' in resp_dict:
        output_detail_lst.append(('Annotated Results File', 'Result File Restoring In Progress', str(request.url)))
      else:
        output_detail_lst.append(('Annotated Results File', 'upgrade to Premium for download', '/subscribe'))
    else:
      output_detail_lst.append(('Annotated Results File', 'download', presigned_url))


    output_detail_lst.append(('Annotation Log File', 'view', str(request.url) + '/log'))
  return render_template('annotation.html', jobDetailTop=job_detail_ID_lst, jobDetailBot= job_detail_Status_lst, input_tup= input_tup, outputDetail=output_detail_lst)


"""Display the log file contents for an annotation job
"""
@app.route('/annotations/<id>/log', methods=['GET'])
@authenticated
def annotation_log(id):
  # using id to get s3 key from database
  dynamodb = boto3.resource('dynamodb', region_name=app.config['AWS_REGION_NAME'])
  ann_table = dynamodb.Table(app.config['AWS_DYNAMODB_ANNOTATIONS_TABLE'])
  try:
    response = ann_table.query(
      KeyConditionExpression=Key('job_id').eq(id)
    )
  except ClientError as e:
    print(e)
    return abort(500)

  if len(response['Items']) != 1:
    return abort(500)
  elif response['Items'][0]['user_id'] != session['primary_identity']:
    return abort(403)
  key = response['Items'][0]['s3_key_log_file']


  # read from S3
  # refer to: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#object
  s3 = boto3.resource('s3', region_name=app.config['AWS_REGION_NAME'])
  try:
    log_content_byte = s3.Object(app.config['AWS_S3_RESULTS_BUCKET'], key).get()['Body'].read()
  except ClientError as e:
    return abort(500)
  
  log_content = log_content_byte.decode(app.config['AWS_S3_OBJ_ENCODING'])



  return render_template('view_log.html', log_content=log_content, job_id=id)


"""Subscription management handler
"""
import stripe
from auth import update_profile, get_profile

@app.route('/subscribe', methods=['GET', 'POST'])
@authenticated
def subscribe():
  if (request.method == 'GET'):
    # Display form to get subscriber credit card info
    return render_template('subscribe.html')
  elif (request.method == 'POST'):
    # Process the subscription request
    stripe_token = request.form['stripe_token']
    

    # Create a customer on Stripe
    stripe.api_key = app.config['STRIPE_SECRET_KEY']
    profile = get_profile(identity_id=session.get('primary_identity'))
    
    # refer to Stripe API - https://stripe.com/docs/api/customers/create
    customer = stripe.Customer.create(
      card=stripe_token,
      name=profile.name,
      email=profile.email
    )


    # Subscribe customer to pricing plan
    # refer to API - https://stripe.com/docs/api/subscriptions/create
    sub_resp = stripe.Subscription.create(
      customer=customer.id,
      items=[
        {"price": app.config['STRIPE_PRICE_ID']},
      ],
    )

    # Update user role in accounts database & Update role in the session
    update_profile(
      identity_id=session['primary_identity'],
      role="premium_user"
    )
    session['role'] = "premium_user"


    # Request restoration of the user's data from Glacier
    # ...add code here to initiate restoration of archived user data
    # ...and make sure you handle files not yet archived!

    # publish notification to result_thaw topic

    data = {'user_id_to_thaw':session['primary_identity']}
    sns = boto3.client('sns', region_name=app.config['AWS_REGION_NAME'], 
                        config=Config(signature_version='s3v4'))
    try:
      response = sns.publish(TopicArn=app.config['AWS_SNS_RESULT_THAW_TOPIC'],
                             MessageStructure='json',
                             Message=json.dumps({'default': json.dumps(data)}))
    except botocore.exceptions.ClientError as e:
      return abort(500)



    # Display confirmation page
    return render_template('subscribe_confirm.html', stripe_id=sub_resp.id)


"""Set premium_user role
"""
@app.route('/make-me-premium', methods=['GET'])
@authenticated
def make_me_premium():
  # Hacky way to set the user's role to a premium user; simplifies testing
  update_profile(
    identity_id=session['primary_identity'],
    role="premium_user"
  )
  return redirect(url_for('profile'))


"""Reset subscription
"""
@app.route('/unsubscribe', methods=['GET'])
@authenticated
def unsubscribe():
  # Hacky way to reset the user's role to a free user; simplifies testing
  update_profile(
    identity_id=session['primary_identity'],
    role="free_user"
  )
  return redirect(url_for('profile'))


'''
helper method to convert time from epoch to local timezone
refer to:
https://stackoverflow.com/questions/12400256/converting-epoch-time-into-the-datetime
'''
def convert_epoch_to_localtime(epoch_time):
  return time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(epoch_time))




"""DO NOT CHANGE CODE BELOW THIS LINE
*******************************************************************************
"""

"""Home page
"""
@app.route('/', methods=['GET'])
def home():
  return render_template('home.html')

"""Login page; send user to Globus Auth
"""
@app.route('/login', methods=['GET'])
def login():
  app.logger.info(f"Login attempted from IP {request.remote_addr}")
  # If user requested a specific page, save it session for redirect after auth
  if (request.args.get('next')):
    session['next'] = request.args.get('next')
  return redirect(url_for('authcallback'))

"""404 error handler
"""
@app.errorhandler(404)
def page_not_found(e):
  return render_template('error.html', 
    title='Page not found', alert_level='warning',
    message="The page you tried to reach does not exist. \
      Please check the URL and try again."
    ), 404

"""403 error handler
"""
@app.errorhandler(403)
def forbidden(e):
  return render_template('error.html',
    title='Not authorized', alert_level='danger',
    message="You are not authorized to access this page. \
      If you think you deserve to be granted access, please contact the \
      supreme leader of the mutating genome revolutionary party."
    ), 403

"""405 error handler
"""
@app.errorhandler(405)
def not_allowed(e):
  return render_template('error.html',
    title='Not allowed', alert_level='warning',
    message="You attempted an operation that's not allowed; \
      get your act together, hacker!"
    ), 405

"""500 error handler
"""
@app.errorhandler(500)
def internal_error(error):
  return render_template('error.html',
    title='Server error', alert_level='danger',
    message="The server encountered an error and could \
      not process your request."
    ), 500

### EOF