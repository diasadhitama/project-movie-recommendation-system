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

        movie = pd.read_sql("select * from movie", mydb)
        print(movie.head())


        # Transform data
        # Drop columns
        movie = movie.drop(columns= ['backdrop_path', 'homepage', 'imdb_id', 
                                    'original_language', 'original_title', 
                                    'status', 'tagline', 'collection_id', 
                                    'budget', 'popularity', 'revenue', 
                                    'runtime', 'vote_average', 'vote_count', 'release_year'])
        print(movie.head())

        # Create sk_movie and rename columns
        dim_movie = movie.rename(columns= {'id':'sk_movie', 'film_id':'movie_id', 
                                            'title':'movie_name', 'overview':'movie_overview',
                                            'poster_path':'movie_poster_path'})
        print(dim_movie.head())

        # Export Data to Datawarehouse
        dim_movie.to_sql('dim_movie', engine_one, if_exists='append', index=False)
        print("Success Export Data to Datawarehouse")