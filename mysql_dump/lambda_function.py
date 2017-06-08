import mysql_dump_to_s3
import boto3
import shutil
import os

# because this is checked in to github, we don't have any private stings in here.  We use aws kms to encrypt them.  We allow the IAM
# role that this lambda runs in to decrypt



def lambda_handler(event, context):

    (bucket_name, config_key)  = os.environ['CONFIG'].split(':')
    
    # mysqldump which is located in this zip doesn't get exec bit, so we have to copy it to /tmp and give it one
    # [can't do it inplace, because root directory is read only
    shutil.copyfile("mysqldump", "/tmp/mysqldump")
    os.chmod("/tmp/mysqldump", 0o700)
    client = boto3.resource('s3')
    content_object = client.Object(bucket_name, config_key)
    data = content_object.get()['Body'].read().decode('utf-8')
    env = {}
    for datum in data.split('\n'):
        if ('' == datum):
            continue
        try:
            index = datum.index('=')
            key = datum[0:index].strip()
            value = datum[1 + index:].strip()
            env[key] = value
        except:
            pass

    mysql_dump_to_s3.mysql_dump_to_s3(env['DB_USERNAME'],env['DB_HOST'], env['DB_PASSWORD'], env['DB_DATABASE'], env['S3_BUCKET'],
                                      port=env['DB_PORT'], mysql_dump_program="/tmp/mysqldump")

    
    
    
