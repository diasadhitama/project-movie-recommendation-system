"""
    author: diasadhitama3@gmail.com
"""

import pandas as pd #type:ignore
import mysql.connector #type:ignore
from dotenv import load_dotenv
from sqlalchemy import create_engine #type:ignore
import os

class Transform:

    def transform_data(self):

        load_dotenv()
        user            = os.environ['usr']
        password        = os.environ['pas']
        host            = os.environ['hos']
        port            = os.environ['por']
        database        = os.environ['db']
        datawarehouse   = os.environ['dwh']

        # Connect to Database

        mydb = mysql.connector.connect(
                                        host=host,
                                        user=user,
                                        password=password,
                                        database=database
                                    )


        # Connect to Datawarehouse

        engine_one = create_engine("mysql+pymysql://{usr}:{pasx}@{hosx}:{port}/{namadb}".format(
                                    usr=user, pasx=password, hosx=host, port=port, namadb=datawarehouse))

        print('success connect to db and dwh')


        # Import data from database

        genre = pd.read_sql("select * from genre", mydb)
        print(genre.head())


        # Transform data
        dim_genre = genre.rename(columns= {'id':'sk_genre', 'genre':'genre_name'})
        print(dim_genre.head())


        # Export data to Datawarehouse
        dim_genre.to_sql('dim_genre', engine_one, if_exists='append', index=False)
        print("Success Export Data to Datawarehouse")