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

        movie_genre = pd.read_sql('select * from movie_genre', mydb)
        
        dim_movie = pd.read_sql('select * from dim_movie', mydb2)
        
        dim_genre = pd.read_sql('select * from dim_genre', mydb2)
        

        # Join table dim_movie and movie_genre to get 'genre_id' from movie_genre table

        movie_genre2 = pd.merge(dim_movie, movie_genre, left_on='movie_id', right_on='film_id')
        

        # Join table movie_genre2 and dim_genre to get sk_genre

        sk_genre = pd.merge(movie_genre2, dim_genre, left_on='genre_id', right_on='genre_id', how='left')
        

        # Drop columns that we don't need to

        fact_table_genre = sk_genre.drop(columns= ['id', 'movie_id', 'movie_name', 'movie_overview', 
                                                    'movie_poster_path', 'film_id', 'genre_id', 'genre_name'])
        print(fact_table_genre.head())


        # Export data to Datawarehouse

        fact_table_genre.to_sql("fact_table_genre", mydb2, if_exists='append', index=False)
        
        print("Succes export data to Datawarehouse")