from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lag, log
from pyspark.sql.window import Window

def load_data_from_postgres():
    spark = SparkSession.builder \
        .appName("CryptoRiskAnalysis") \
        .config("spark.jars", "C:\Users\sabhusha\Downloads\postgresql-42.7.7.jar") \
        .getOrCreate()

    df = spark.read \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/In_Project") \
        .option("dbtable", "crypto_db") \
        .option("user", "postgres") \
        .option("password", "root") \
        .load()

    return df

def calculate_risk_metrics(df):
    windowSpec = Window.orderBy("date")
    df = df.withColumn("log_return", log(col("close") / lag("close").over(windowSpec)))
    mean_return = df.selectExpr("avg(log_return)").collect()[0][0]
    volatility = df.selectExpr("stddev(log_return)").collect()[0][0]
    sharpe_ratio = mean_return / volatility if volatility else None
    return mean_return, volatility, sharpe_ratio
