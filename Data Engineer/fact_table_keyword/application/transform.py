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

        movie_keyword = pd.read_sql('select * from movie_keyword', mydb)
        
        dim_keyword = pd.read_sql('select * from dim_keyword', mydb2)
        
        dim_movie = pd.read_sql('select * from dim_movie', mydb2)
        

        # Join table dim_movie and movie_keyword to get sk_movie

        sk_movie = pd.merge(dim_movie, movie_keyword, left_on='movie_id', right_on='film_id')
        

        # Join table sk_movie and dim_keyword to get sk_keyword
        # Generate fact_table_keyword

        fact_tabel_keyword = pd.merge(sk_movie, dim_keyword, left_on='id', right_on='keyword_id').drop(columns=['movie_id', 'movie_name', 'movie_overview',
                                                                                                            'movie_poster_path', 'id', 'film_id',
                                                                                                            'keyword', 'keyword_id', 'keyword_name'], axis=1)
        print(fact_tabel_keyword.head())


        # Export data to Datawarehouse

        fact_tabel_keyword.to_sql("fact_tabel_keyword", mydb2, if_exists='append', index=False)
        
        print("Succes export data to Datawarehouse")