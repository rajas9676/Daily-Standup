"""
This code sample is part of AWS learning journey.
Complete repository can be found at https://github.com/rajas9676/Daily-Standup
"""
import argparse
import boto3
import logging
from botocore.exceptions import ClientError


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
        client = boto3.client('s3')
        try:
            client.delete_bucket(Bucket=self.bucket_name)
        except ClientError as e:
            print(e['Error']['Message'])


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('bucket_name', help='Enter the unique bucket name to create')
    args = parser.parse_args()
    s3_resource = boto3.resource('s3')
    try:
        resource = S3ResourceBasics(s3_resource, args.bucket_name)
        resource.create_bucket()
        resource.list_buckets()
        resource.delete_bucket()
        resource.list_buckets()
    except ClientError:
        print('error creating object')

