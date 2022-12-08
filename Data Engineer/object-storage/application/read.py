"""
    author: diasadhitama3@gmail.com
"""

import boto3 #type:ignore
import os #type:ignore
from dotenv import load_dotenv
from sqlalchemy import create_engine  #type:ignore
import pandas as pd

class Read:

    def read_file(self):

        load_dotenv()
        key_id      = os.environ['key']
        access_key  = os.environ['acc']
        user        = os.environ['usr']
        password    = os.environ['pas']
        host        = os.environ['hos']
        port        = os.environ['por']
        database    = os.environ['db']

        # Connect to MySQL

        engine_one = create_engine("mysql+pymysql://{usr}:{pasx}@{hosx}:{port}/{namadb}".format(
                                    usr=user, pasx=password, hosx=host, port=port, namadb=database))

        print('success connect db')

        
        # Read CSV File from S3 Bucket

        df1= pd.read_csv("s3://finalprojectdiasadhitama/collection.csv", sep=';', encoding='latin-1',
                            storage_options={"key": key_id, "secret": access_key})
        df2= pd.read_csv("s3://finalprojectdiasadhitama/company.csv", sep=';', encoding='latin-1',
                            storage_options={"key": key_id, "secret": access_key})
        df3= pd.read_csv("s3://finalprojectdiasadhitama/country.csv", sep=';', encoding='latin-1',
                            storage_options={"key": key_id, "secret": access_key})
        df4= pd.read_csv("s3://finalprojectdiasadhitama/genre.csv", sep=';', encoding='latin-1',
                            storage_options={"key": key_id, "secret": access_key})
        df5= pd.read_csv("s3://finalprojectdiasadhitama/movie_company.csv", sep=';', encoding='latin-1',
                            storage_options={"key": key_id, "secret": access_key})
        df6= pd.read_csv("s3://finalprojectdiasadhitama/movie_genre.csv", sep=';', encoding='latin-1',
                            storage_options={"key": key_id, "secret": access_key})
        df7= pd.read_csv("s3://finalprojectdiasadhitama/movie.csv", sep=';', encoding='latin-1',
                            storage_options={"key": key_id, "secret": access_key})
        df8= pd.read_csv("s3://finalprojectdiasadhitama/movie_keyword.csv", sep=';', encoding='latin-1',
                            storage_options={"key": key_id, "secret": access_key})
        
        print('success read file')                         
        
        print(df7.head())
        print(df8.head())
        
        # Load csv files to Database

        df1.to_sql("collection", engine_one, if_exists='append', index=False)        
        df2.to_sql("company", engine_one, if_exists='append', index=False)        
        df3.to_sql("country", engine_one, if_exists='append', index=False)        
        df4.to_sql("genre", engine_one, if_exists='append', index=False)        
        df5.to_sql("movie_company", engine_one, if_exists='append', index=False)        
        df6.to_sql("movie_genre", engine_one, if_exists='append', index=False)        
        df7.to_sql("movie", engine_one, if_exists='append', index=False)        
        df8.to_sql("movie_keyword", engine_one, if_exists='append', index=False)        

        print('Finish')
        

