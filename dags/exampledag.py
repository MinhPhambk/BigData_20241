# my_dag.py
from airflow.decorators import dag, task
from datetime import datetime

from pyspark import SparkContext
from pyspark.sql import SparkSession
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import pandas as pd
    
@dag(
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
)

def my_dag():
    
    @task.pyspark(conn_id="my_spark_conn")
    def crawl_stock(spark: SparkSession, sc: SparkContext) -> pd.DataFrame:
        df_prices = spark.read.option("header", "true").csv('/BSESN.csv')
        if (df_prices):
            return df_prices.toPandas()
        else:
            return pd.DataFrame()
        
    @task.pyspark(conn_id="my_spark_conn")
    def crawl_news(spark: SparkSession, sc: SparkContext) -> pd.DataFrame:
        df_news = spark.read.option("header", "true").csv('/india-news-headlines.csv')
        if (df_news):
            df_news['Date'] = pd.to_datetime(df_news['Date'],format= '%Y%m%d')
            df_news['News'] = df_news.groupby(['Date']).transform(lambda x : ' '.join(x)) 
            df_news = df_news.drop_duplicates() 
            df_news.reset_index(inplace = True, drop = True)
            return df_news.toPandas()
        else:
            return pd.DataFrame()
    
    df_prices = crawl_stock()
    df_news = crawl_news()    

    @task.pyspark(conn_id="my_spark_conn")
    def clean_news_data(spark: SparkSession, sc: SparkContext) -> pd.DataFrame:
        if df_news:
            c = []
            # ps=PorterStemmer()
            for i in range(0,len(df_news['News'])):
                news = re.sub('[^a-zA-Z]',' ',df_news['News'][i])
                news = news.lower()
                news = news.split()
                # news = [ps.stem(word) for word in news if not word in set(stopwords.words('english'))]
                news=' '.join(news)
                c.append(news)
            df_news['News'] = pd.Series(c)
            return df_news
        else: 
            return pd.DataFrame()
    
    df_news = clean_news_data()  

    def getSubjectivity(text):
        if (text):
            return TextBlob(text).sentiment.subjectivity
        else:
            return ""

    def getPolarity(text):
        if (text):
            return TextBlob(text).sentiment.polarity
        else:
            return ""

    @task.pyspark(conn_id="my_spark_conn")
    def adding_subjectivity_and_polarity(spark: SparkSession, sc: SparkContext) -> pd.DataFrame:
        if df_news:
            df_news['Subjectivity'] = df_news['News'].apply(getSubjectivity)
            df_news['Polarity'] = df_news['News'].apply(getPolarity)
            return df_news
        else:
            return pd.DataFrame()

    df_news = adding_subjectivity_and_polarity()

    @task.pyspark(conn_id="my_spark_conn")
    def adding_sentiment_score(spark: SparkSession, sc: SparkContext) -> pd.DataFrame:
        if df_news:
            sia = SentimentIntensityAnalyzer()
            df_news['Compound'] = [sia.polarity_scores(v)['compound'] for v in df_news['News']]
            df_news['Negative'] = [sia.polarity_scores(v)['neg'] for v in df_news['News']]
            df_news['Neutral'] = [sia.polarity_scores(v)['neu'] for v in df_news['News']]
            df_news['Positive'] = [sia.polarity_scores(v)['pos'] for v in df_news['News']]
        else:
            return pd.DataFrame()

    @task.pyspark(conn_id="my_spark_conn")
    def merge_news_and_stock_data(spark: SparkSession, sc: SparkContext) -> pd.DataFrame:
        if df_news:
            df_merge = pd.merge(df_prices, df_news, how='inner', on='Date')
            df_merge = df_merge[['Close','Subjectivity', 'Polarity', 'Compound', 'Negative', 'Neutral' ,'Positive']]
            return df_merge
        else:
            return pd.DataFrame()

    df_merge = merge_news_and_stock_data()
    
    def train_data() -> pd.DataFrame:
        if df_merge:
            # xgb = xgboost.XGBRegressor()
            # xgb.fit(x_train, y_train)
            # return xgb
            return pd.DataFrame()
        else:
            return pd.DataFrame()
    
    xgboost_model = train_data()

my_dag()