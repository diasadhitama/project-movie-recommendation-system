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

        movie = pd.read_sql('select * from movie', mydb)
        
        dim_movie = pd.read_sql('select * from dim_movie', mydb2)
        
        dim_language = pd.read_sql('select * from dim_language', mydb2)
        
        dim_waktu = pd.read_sql('select * from dim_waktu', mydb2)
        
        dim_collection = pd.read_sql('select * from dim_collection', mydb2)
        

        # Join table movie and dim_movie to get sk_movie

        sk_movie = pd.merge(movie, dim_movie,left_on='film_id', right_on='movie_id')
        

        # Join table sk_movie and dim_language to get sk_country

        sk_country = pd.merge(sk_movie, dim_language, left_on='original_language', right_on='country_code', how='left')
        

        # Join table sk_country and dim_waktu to get sk_waktu

        sk_waktu = pd.merge(sk_country, dim_waktu, left_on='release_year', right_on='release_year', how='left')
        

        # Join table sk_waktu and dim_collection to get sk_collection

        sk_collection = pd.merge(sk_waktu, dim_collection, left_on='collection_id', right_on='collection_id', how='left')
        

        # Drop columns thath we don't need to

        fact_movie = sk_collection.drop(columns=['id', 'title', 'backdrop_path', 'homepage',
                                                'original_title', 'overview',
                                                'poster_path', 'status', 'tagline', 'movie_id',
                                                'movie_name', 'movie_overview', 'movie_poster_path',
                                                'film_id', 'imdb_id', 'original_language',
                                                'collection_id', 'release_year', 'country_code', 
                                                'country_name', 'collection_name'])
                

        # Change columns position

        fact_table_movie = fact_movie.iloc[:,[6, 9, 7, 8, 3, 0, 2, 1, 4, 5]]
        print(fact_table_movie.head())


        # Export data to Datawarehouse

        fact_table_movie.to_sql("fact_table_movie", mydb2, if_exists='append', index=False)

        print("Succes export data to Datawarehouse")