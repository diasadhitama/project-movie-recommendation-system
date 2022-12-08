"""
    author: diasadhitama3@gmail.com
"""


import streamlit as st
import pickle
import pandas as pd
import streamlit_authenticator as stauth



# User authentication

names = ['dias', 'adhitama']
usernames = ['user', 'user1']
password = ['user', 'user1']

hashed_password = stauth.Hasher(password).generate()

authenticator = stauth.Authenticate(names, usernames, hashed_password, 'movie_recommendation', 'abcdef123', cookie_expiry_days=30)

name, authentication_status, username = authenticator.login("Login", "main")

if authentication_status == False:
    st.error("Username / Password is Incorrect")

if authentication_status == None:
    st.warning("Please Input your Username and Password")

if authentication_status:


    # Dump File to Read Data from movies.pkl
    movies_data = pickle.load(open('movies.pkl', 'rb'))
    movies      = pd.DataFrame(movies_data)

    # Dump File similarity scores
    similarity  = pickle.load(open('similarity.pkl', 'rb'))

    # Dump File to Read Data from movies-genre.pkl
    movie_genre = pickle.load(open('movies-genre.pkl', 'rb'))
    movies2     = pd.DataFrame(movie_genre)

    # Dump File to Read Data from dim-genre.pkl
    dim_genre   = pickle.load(open('dim-genre.pkl', 'rb'))
    genre       = pd.DataFrame(dim_genre)


    # Generate movie recommendation from selected movie
    def recommend(movie):

        # Find the index of the movies
        movie_index = movies[movies['title']==movie].index[0]
        distances = similarity[movie_index]
        movies_list = sorted(list(enumerate(distances)),reverse=True,key=lambda x:x[1])[0:5]

        # Fetch movies from indeces
        recommend_movie = []
        for i in movies_list:
            recommend_movie.append(movies.iloc[i[0]].title)
        return recommend_movie


    # Generate movie recommendation from selected genre
    class RecommenderSystem:

        def __init__(self, data):
            self.df = data

        def recommend(self, genre=None, top=10):
            df = self.df.copy()
            df = self.demographic_filter(df, genre=genre)
            df = self.imdb_score(df)

            recommend = df.loc[:, ['movie_name', 'genre' ,'runtime', 'vote_average', 'vote_count', 'score']]
            recommend = recommend.sort_values("score", ascending=False)
            recommend = recommend.head(top)
            return recommend

        @staticmethod
        def demographic_filter(df, genre=None):
            df = df.copy()
            if genre is not None:
                df = df[df[genre].any(axis=1)]
            return df
            
        @staticmethod
        def imdb_score(df, q=0.95):
            df = df.copy()
            m = df.vote_count.quantile(q)
            C = (df.vote_average * df.vote_count).sum() / df.vote_count.sum()

            df = df[df.vote_count >= m]
            df["score"] = df.apply(lambda x: (x.vote_average * x.vote_count + C*m) / (x.vote_count + m), axis=1)
            return df


    recsys = RecommenderSystem(data=movies2)

    # Generate title in web
    st.title('Movie Recommendation System')

    # Generate selectbox
    select_movie = st.selectbox(
        "Type Movie or Select a Movie from the Dropdown",
        movies['title'].values,
        key = '1'
    )

    if st.button('Show Movie Recommendation'):
        recommendation_movie = recommend(select_movie)

        for i in recommendation_movie:
            st.write(i)


    select_genre = st.selectbox(
        "Type Genre or Select a Genre from the Dropdown",
        genre['genre_name'].values,
        key = '2'
    )


    if st.button('Show Recommendation'):
        recommendation_genre = recsys.recommend([select_genre])

        for j in range(len(recommendation_genre)):
            st.write(recommendation_genre.iloc[j,0])