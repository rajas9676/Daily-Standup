"""
This code sample is part of AWS learning journey.
Complete repository can be found at https://github.com/rajas9676/Daily-Standup
"""
import argparse
import boto3
import logging
import os
from botocore.exceptions import ClientError
from spark_scripts.min_temp import process_file


class S3ClientBasics:
    def __int__(self, s3_client):
        self.s3_client = s3_client

    def create_bucket(self):
        self.s3_client.create_bucket(Bucket='test-basics')

    def delete_bucket(self, bucket_name):
        self.s3_client.delete_bucket(Bucket=bucket_name)

    def list_buckets(self):
        result = self.s3_client.list_buckets()
        print('Following buckets are available:\n')
        for bucket in result['Buckets']:
            print(f'{bucket["Name"]}')


class S3ResourceBasics:
    def __init__(self, s3_resource, bucket_name):
        self.s3_resource = s3_resource
        self.bucket_name = bucket_name

    def create_bucket(self):
        try:
            print('Creating bucket:', self.bucket_name)
            self.s3_resource.create_bucket(Bucket=self.bucket_name,
                                           CreateBucketConfiguration={
                                               'LocationConstraint': 'us-west-1'
                                           }
                                           )
        except ClientError as e:
            print(e.response['Error']['Message'])

    def list_buckets(self):
        print('Avilable:', *[b.name for b in self.s3_resource.buckets.all()], sep="\t")

    def delete_bucket(self):
        print('Deleting the bucket & contents.....')
        my_bucket = self.s3_resource.Bucket(self.bucket_name)
        try:
            my_bucket.objects.all().delete()
            my_bucket.delete()
        except ClientError as e:
            logging.error(e)
            return False
        return True

    def upload_file_to_s3(self, file_name):
        print('uploading file to S3......')
        try:
            self.s3_resource.meta.client.upload_file(file_name, Bucket=self.bucket_name, Key=file_name)
        except ClientError as e:
            logging.error(e)
            return False
        return True

    def process_file_from_s3(self):
        print('---------spark script running---------')
        try:
            my_bucket = self.s3_resource.Bucket(self.bucket_name)
            for file in my_bucket.objects.filter(Prefix='dataset').all():
                key = file.key
                body = file.get()['Body'].read()
                process_file(key)
        except ClientError as e:
            logging.error(e)
            return False
        return True


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('bucket_name', help='Enter the unique bucket name to create')
    args = parser.parse_args()
    s3_resource = boto3.resource('s3')
    try:
        resource = S3ResourceBasics(s3_resource, args.bucket_name)
        resource.create_bucket()
        resource.list_buckets()
        resource.upload_file_to_s3('dataset/temp_readings.csv')
        resource.process_file_from_s3()
        resource.delete_bucket()
        resource.list_buckets()
    except ClientError:
        print('error creating object')
