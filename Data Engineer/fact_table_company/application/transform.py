"""
    author:diasadhitama3@gmail.com
"""

import pandas as pd
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv


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

        mydb = create_engine("mysql+pymysql://{usr}:{pasx}@{hosx}:{port}/{namadb}".format(
                                    usr=user, pasx=password, hosx=host, port=port, namadb=database))


        # Connect to Datawarehouse

        mydb2 = create_engine("mysql+pymysql://{usr}:{pasx}@{hosx}:{port}/{namadb}".format(
                                    usr=user, pasx=password, hosx=host, port=port, namadb=datawarehouse))


        # Import data from Database

        movie_company = pd.read_sql('select * from movie_company', mydb)
        
        dim_company = pd.read_sql('select * from dim_company', mydb2)
        
        dim_movie = pd.read_sql('select * from dim_movie', mydb2)
        

        # Join table movie_company and dim_company to get sk_company

        sk_company = pd.merge(movie_company, dim_company, left_on='company_id', right_on='company_id')
        

        # Join table sk_company and dim_movie to get sk_movie
        # Create fact_table_company

        fact_table_company = pd.merge(dim_movie, sk_company, left_on='movie_id', right_on='film_id').drop(columns=['id', 'film_id', 'company_id', 'company_name',
                                                                                                            'movie_id', 'movie_name', 'movie_overview',
                                                                                                            'movie_poster_path'], axis=1)
        print(fact_table_company.head())


        # Export data to Datawarehouse

        fact_table_company.to_sql("fact_table_company", mydb2, if_exists='append', index=False)
        
        print("Succes export data to Datawarehouse")