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
        company = pd.read_sql("select * from company", mydb)

        print(company.head())

        # Transform data to make company dimention table
        # Drop columns
        df = company.drop(columns= ['homepage', 'description', 'headquarters', 'logo_path', 'origin_country', 'parent_company'])

        # Rename columns
        dim_company = df.rename(columns= {'id':'sk_company'})
        dim_company.head()

        # Export transform result to Datawarehouse
        dim_company.to_sql('dim_company', engine_one, if_exists='append', index=False)

        print("Success Export Data to Datawarehouse")