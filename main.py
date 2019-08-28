#Batch Renamer S3 with DB (MySQL)

import mysql.connector
import mimetypes
import boto3

# Declaration variable of AWS S3
awsAccessKey = '<ACCESS KEY>'
awsSecretAccessKey = '<SECRET KEY>'
s3BucketName = '<BUCKET NAME>'

# Create Session for accessing AWS S3 with access key and secret key
session = boto3.Session(aws_access_key_id = awsAccessKey, aws_secret_access_key = awsSecretAccessKey)
s3 = session.resource('s3')

# Create session for mysql connector
from mysql.connector import Error
connection = mysql.connector.connect(host='<HOST>', # please change this
                                        port = 3306,
                                        database='<DB NAME>',
                                        user='<DB USER>',
                                        password='DB PASSWORD')

try:
    # query mysql
    sql_select_Query = "select * from <table>" #change table
    cursor = connection.cursor()
    cursor.execute(sql_select_Query)
    records = cursor.fetchall()

    for row in records:
        oldFileName = "<Old File Name>" # file want to rename
        newFileName = "<New File Name>" # new Name
        try:
            # rename name from oldFileName to newFileName
            s3.Object(s3BucketName, str(newFileName)).copy_from(CopySource = s3BucketName + str(oldFileName))
        except:
            pass
        print(oldFileName)
        print(newFileName)
        print('------------')

except Error as e:
    print("Error reading data from MySQL table", e)

finally:
    if (connection.is_connected()):
        connection.close()
        cursor.close()
        print("MySQL connection is closed")
        