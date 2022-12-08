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
        collection = pd.read_sql("select * from collection", mydb)

        print(collection.head())


        # Transform data to make dimention collection        
        # Rename columns
        collection = collection.rename(columns= {'id':'sk_collection'})

        # Change column position
        dim_collection = collection.iloc[:,[0,2,1]]
        
        print(dim_collection.head())


        # Export transform result to Datawarehouse
        dim_collection.to_sql('dim_collection', engine_one, if_exists='append', index=False)
        
        print("Success Export Data to Datawarehouse")