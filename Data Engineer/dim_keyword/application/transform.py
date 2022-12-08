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

        # Import data from Database

        keyword = pd.read_sql("select * from movie_keyword", mydb)
        print(keyword.head())


        # Transform data
        # Create sk_keyword and rename columns
        keyword['sk_keyword'] = keyword['id']
        dim_keyword = keyword.rename(columns = {'id':'keyword_id','keyword':'keyword_name'})
        print(dim_keyword.head())

        # Drop column
        dim_keyword = dim_keyword.drop(columns='film_id')
        print(dim_keyword.head())

        # Change columns position
        dim_keyword = dim_keyword.iloc[:,[2,0,1]]
        print(dim_keyword.head())

        # Export data to Datawarehouse
        dim_keyword.to_sql('dim_keyword', engine_one, if_exists='append', index=False)
        print("Success Export Data to Datawarehouse")