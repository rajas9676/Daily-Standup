'''
Following script guides thorugh the process of connecting to RDS-MySQL instance using Boto3
'''
import pymysql
import sys
import boto3
import mysql.connector
import os

ENDPOINT = "database-1.cr6wfyiycwkf.us-west-1.rds.amazonaws.com"
PORT = "3306"
USER = "raja"
PWD = "mypassword"
REGION = "us-west-1"
DBNAME = "database-1"
os.environ['LIBMYSQL_ENABLE_CLEARTEXT_PLUGIN'] = '1'

# get credentials from aws explorer
session = boto3.Session(profile_name='default')
client = session.client('rds')
token = client.generate_db_auth_token(DBHostname=ENDPOINT, Port=PORT, DBUsername=USER, Region=REGION)

try:
    conn = mysql.connector.connect(host=ENDPOINT, user=USER, passwd=token, port=PORT, ssl_ca='SSLCERTIFICATE')
    cur = conn.cursor()
    cur.execute("""SELECT now()""")
    query_results = cur.fetchall()
    print(query_results)
except Exception as e:
    print("Database connection failed due to {}".format(e))



