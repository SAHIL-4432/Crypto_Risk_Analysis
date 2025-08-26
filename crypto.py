from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, hour, avg, max
import plotly.express as px

# Step 1: Start Spark session
spark = SparkSession.builder.appName("Crypto_Minute_Analysis").getOrCreate()

# Step 2: Load CSV
df = spark.read.csv("dataset.csv", header=True, inferSchema=True)

# Step 3: Clean and transform
df = df.withColumn("timestamp", to_timestamp(col("timestamp")))
df = df.withColumn("hour", hour(col("timestamp")))
df = df.withColumn("date_only", col("timestamp").cast("date"))
df = df.withColumn("volatility", col("high") - col("low"))

# Step 4: Insights

# 1. Average close price per hour
avg_close_per_hour = df.groupBy("hour").agg(avg("close").alias("avg_close")).orderBy("hour")
avg_close_per_hour.show()

# 2. Maximum volatility per day
max_volatility_per_day = df.groupBy("date_only").agg(max("volatility").alias("max_volatility")).orderBy("max_volatility", ascending=False)
max_volatility_per_day.show()

# 3. Top 5 most volatile trading days
top_5_volatile_days = max_volatility_per_day.limit(5)
top_5_volatile_days.show()

# 4. Hourly price trend for a selected date
selected_date = "2014-05-05"
hourly_trend = df.filter(col("date_only") == selected_date).groupBy("hour").agg(avg("close").alias("avg_close")).orderBy("hour")
hourly_trend.show()

# Convert Spark DataFrames to Pandas
avg_close_pd = avg_close_per_hour.toPandas()
hourly_trend_pd = hourly_trend.toPandas()

# Plot average close per hour
fig1 = px.line(avg_close_pd, x="hour", y="avg_close", title="Average Close Price per Hour")
fig1.write_image("avg_close_per_hour.png")

# Plot hourly trend for selected date
fig2 = px.line(hourly_trend_pd, x="hour", y="avg_close", title=f"Hourly Price Trend on {selected_date}")
fig2.write_image("hourly_trend_selected_date.png")


fig1.show()
fig2.show()

