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
        country = pd.read_sql("select * from country", mydb)

        print(country.head())


        # Transform data
        # Rename columns
        df = country.rename(columns= {'id':'sk_country', 'country':'country_name', 'code':'country_code'})
        df.head()

        # Change columns position
        dim_language = df.iloc[:,[0,2,1]]
        print(dim_language.head())

        # Lowercase country_code
        dim_language['country_code'] = dim_language['country_code'].str.lower()
        dim_language.head()


        # Export data to Datawarehouse
        dim_language.to_sql('dim_language', engine_one, if_exists='append', index=False)
        print("Success Export Data to Datawarehouse")