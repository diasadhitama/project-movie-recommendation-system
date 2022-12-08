"""
    author: diasadhitama3@gmail.com
"""

import boto3 #type: ignore
import os
from dotenv import load_dotenv 
import glob

class Upload:

    def upload_file(self):

        load_dotenv()
        key_id      = os.environ['key']
        access_key  = os.environ['acc']
        region      = os.environ['reg']
        endpoint    = boto3.client(
                                    's3',
                                    aws_access_key_id= '{abc}'.format(abc=key_id),
                                    aws_secret_access_key= '{abd}'.format(abd=access_key),
                                    region_name= '{abe}'.format(abe=region)
                                    )
                
        path = "D:\\Bootcamp G2Academy\\FINAL_PROJECT\\csv_files\\"
        files = glob.glob(path+"*.csv")
        
        for file in files:
            endpoint.upload_file(
                                Filename= file,
                                Bucket = 'finalprojectdiasadhitama',
                                Key = file.split("\\")[-1]
                        ) 

        print("success upload files")


        print("All data files uploaded to S3")