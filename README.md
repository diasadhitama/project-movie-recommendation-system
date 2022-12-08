# Project-movie-recommendation-system

This project show all of stages from collecting data to build machine learning to generate movie recommendation system

# DATA ENGINEER


Business Requirements :

- Get insight about finance segment from the movie
- Get insight about statistic data from the movie
- Interactive Dashboard for Analyze Business

Data Requirements :

- Collecting Data from Kaggle Datasets
- Data pipeline for data distribution
- Data lake to store data
- Database for data warehouse
- Data visualization tools that are accessible by the user

In this case, I used Distributed On-Demand ETL. On-demand ETL is an ETL process that can only be used once. Based on the dataset, no data updates have been made because the data is static directly from Kaggle. So there is no need to use scheduling.

Data Architecture :
- Starts from Amazon S3 as data  lake, raw data is collected here
- Extract data to MySQL using the Python programming language
- Transform data from MySQL using Python programming language
- Load data to MySQL as data warehouse
- Tableau for datavisualization


# DATA SCIENCE


Business Requirements :
- Predictive process which is easy to understand
- Machine learning model API accessible to business user

Method of Machine Learning:
- Content Based Filtering
- Demographic Based Filtering


Before entering the movie recommendation system, user need to give user authentication :

![Screenshot (28)](https://user-images.githubusercontent.com/114588103/206570549-6f918ec0-1155-4124-bdaf-d4c923b38313.png)


The preview about model that generated is below :

1. Prediction movie that we search and the similar movie

![Screenshot (21)](https://user-images.githubusercontent.com/114588103/206568212-d967c859-68f0-4f40-9365-53d37f6a15ba.png)

2. Prediction about Top 10 movie by genre

![Screenshot (29)](https://user-images.githubusercontent.com/114588103/206571114-603ae1c8-94ed-4a99-940d-0216dc398ff0.png)
