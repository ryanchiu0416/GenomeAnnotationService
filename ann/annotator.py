import subprocess
import os
import boto3, botocore
import json


from configparser import ConfigParser
config = ConfigParser(os.environ)
config.read(os.path.join(os.path.abspath(os.path.dirname(__file__)), 'ann_config.ini'))


sqs = boto3.resource('sqs', region_name=config['aws']['AwsRegionName'])
queue = sqs.get_queue_by_name(QueueName=config['aws']['AwsSqsRequestQueueName'])

while True:
    try:
        messages = queue.receive_messages(WaitTimeSeconds=20)
    except botocore.exceptions.ClientError as e:
        print(e)

    for m in messages:
        try:
            request_dict = json.loads(json.loads(m.body)['Message'])
        except Exception:
            print(e)


        job_id = request_dict['job_id']
        s3_bucket = request_dict['s3_inputs_bucket']
        s3_file_key = request_dict['s3_key_input_file']
        user_id = request_dict['user_id']
        input_file_name = request_dict['input_file_name']


        # Get the input file S3 object and copy it to a local file
        try:
            if not os.path.exists('/home/ubuntu/gas/ann/jobs'):
                os.mkdir('/home/ubuntu/gas/ann/jobs')
            os.chdir('/home/ubuntu/gas/ann/jobs')
        except OSError as e:
            print(e)


        # download input file
        unique_filename = f'{job_id}~{input_file_name}'
        s3 = boto3.resource('s3', region_name = config['aws']['AwsRegionName'])
        try:
            s3.Bucket(s3_bucket).download_file(s3_file_key, unique_filename)
        except botocore.exceptions.ClientError as e:
            print(f'Cannot obtain the file just uploaded. {e}')


        # prepare for running process
        try:
            f = open(f'/home/ubuntu/gas/ann/jobs/{unique_filename}.output', 'a')
        except OSError as e:
            print(e)


        # Launch annotation job as a background process
        launch_cmd = f'python /home/ubuntu/gas/ann/anntools/run.py /home/ubuntu/gas/ann/jobs/{unique_filename} {job_id} {user_id}'
        try:
            process = subprocess.Popen(launch_cmd, shell=True, stdout=f)
        except Exception as e:
            print(f'File is found but an error occurred when launching the job. {e}')


        dynamodb = boto3.resource('dynamodb', region_name=config['aws']['AwsRegionName'])
        # update status to RUNNING (only when current status is PENDING)
        try:
            ann_table = dynamodb.Table(config['aws']['AwsDynamoDbAnnotationsTable'])
            resp = ann_table.update_item(
                Key={
                    'job_id': job_id
                },
                UpdateExpression='SET job_status = :s',
                ConditionExpression="job_status = :c",
                ExpressionAttributeValues={
                    ':s': 'RUNNING',
                    ':c': 'PENDING'
                }
            )
        except botocore.exceptions.ClientError as e:
            print(f'Job status update error. {e}')


        try:
            # delete current message from sqs only after successful update to DB
            m.delete()
        except botocore.exceptions.ClientError as e:
            print(e)


