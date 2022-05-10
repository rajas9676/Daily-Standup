import boto3
import logging
from botocore.exceptions import ClientError
from decimal import Decimal
import os
import shutil
import wget
import json

logger = logging.getLogger(__name__)


class Movies:
    def __init__(self, dyn_resource):
        self.dynamo_resource = dyn_resource
        self.table = None
        if not os.path.exists('moviedata.json'):
            url = "https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/samples/moviedata.zip"
            print('downloading json data')
            wget.download(url)
            shutil.unpack_archive('moviedata.zip')

    def table_exists(self, tb_name):
        try:
            table = self.dynamo_resource.Table(tb_name)
            table.load()
            self.table = table
            return True
        except ClientError as e:
            if e.response['Error']['Code'] == 'ResourceNotFoundException':
                return False
            else:
                raise logger.error(e.response['Error']['Message'])

    def create_movie_table(self, tb_name):
        try:
            self.table = self.dynamo_resource.create_table(
                TableName=tb_name,
                KeySchema=[{'AttributeName': 'year', 'KeyType': 'HASH'},
                           {'AttributeName': 'title', 'KeyType': 'RANGE'}],
                AttributeDefinitions=[{'AttributeName': 'year', 'AttributeType': 'N'},
                                      {'AttributeName': 'title', 'AttributeType': 'S'}],
                ProvisionedThroughput={'ReadCapacityUnits': 10, 'WriteCapacityUnits': 10})
            self.table.wait_until_exists()
            return self.table
        except ClientError as e:
            raise logger.error(e.response['Error']['Message'])

    def add_movie(self, title, year, plot, rating):
        try:
            self.table.put_item(Item =
                                {'year': year,
                                 'title':title,
                                 'info':{'plot': plot, 'rating': Decimal(str(rating))}})
        except ClientError as e:
            raise logger.error(e)

    def get_movie(self, title, year):
        try:
            response = self.table.get_item(Key={'year':year, 'title':title})
        except ClientError as e:
            raise logger.error(e)
        return response['Item']

    def delete_table(self):
        try:
            self.table.delete()
            self.table = None
        except ClientError as e:
            raise logger.error(e.response['Error']['Message'])

    def write_batch(self, file_name):
        try:
            with open(file_name) as movie_file:
                movie_data = json.load(movie_file, parse_float=Decimal)
            try:
                with self.table.batch_writer() as writer:
                    for movie in movie_data[:100]:
                        writer.put_item(Item=movie)
            except ClientError as err:
                raise logger.error(err.response['Error']['Message'])
        except FileNotFoundError as e:
            raise logger.error(e)


if __name__ == "__main__":
    dynamo_resource = boto3.resource('dynamodb')
    movies = Movies(dynamo_resource)
    table_name = 'Movies'
    if not movies.table_exists(table_name):
        print('===>creating table:', table_name)
        movies.create_movie_table(table_name)
        print('table created successfully:', movies.table.name)
        print('===>copying contents from the file')
        movies.write_batch('moviedata.json')
    else:
        result = movies.get_movie(title='Rush', year=2013)
        print(result)
        movies.delete_table()
        print('table deleted successfully:')
