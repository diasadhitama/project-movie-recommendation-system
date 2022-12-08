"""
    author: diasadhitama3@gmail.com
"""

import boto3 
import os
from dotenv import load_dotenv 

class Create:

    def create_bucket(self):

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

        
        bucket_name = 'finalprojectdiasadhitama'
        response    = endpoint.create_bucket(Bucket=bucket_name)
        print(response)
        print('success create bucket')
