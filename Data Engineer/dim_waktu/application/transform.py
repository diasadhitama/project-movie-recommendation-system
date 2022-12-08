"""
    author: diasadhitama3@gmail.com
"""

import pandas as pd #type:ignore
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
        datawarehouse   = os.environ['dwh']


        # Connect to Datawarehouse

        engine_one = create_engine("mysql+pymysql://{usr}:{pasx}@{hosx}:{port}/{namadb}".format(
                                    usr=user, pasx=password, hosx=host, port=port, namadb=datawarehouse))

        print('success connect to dwh')


        # Create sk_waktu
        x = []

        for i in range (1,135):
            x.append(i)
            sk_waktu = pd.Series(x)

        print(sk_waktu.head())


        # Create release_year
        year = []
        for y in range(1895, 2029):
            year.append(y)
            tahun = pd.Series(year)

        print(tahun.head())

        # Create dataframe for dim_waktu
        frame = {'sk_waktu' : sk_waktu, 'release_year' : tahun}
        dim_waktu = pd.DataFrame(frame)
        print(dim_waktu.head())

        # Export dataframe to Datawarehouse
        dim_waktu.to_sql('dim_waktu', engine_one, if_exists='append', index=False)
        print("Success Export Data to Datawarehouse")  

 