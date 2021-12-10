import boto3
import configparser
import os


# get AWS parameters from config file
config = configparser.ConfigParser()
config.read('config')
my_bucket = config['AWS']['s3_bucket']
aws_key_id = config['AWS']['aws_access_key_id']
aws_secret = config['AWS']['aws_secret_access_key']


# create s3 client
s3_client = boto3.client(
    's3'
   ,aws_access_key_id=aws_key_id
   ,aws_secret_access_key=aws_secret
)

# upload all files in data_path to s3
data_path = './processed_data'
for root, dirs, files in os.walk(data_path):
    for each in files:
        full_path = os.path.join(root,each)
        upload = s3_client.upload_file(full_path,my_bucket,each)
        if upload == None:
            # None is returned if upload is successful
            print(f"Upload successful for {full_path}.")
        else:
            print(f"Upload failed for {full_path} with: ", upload)        
