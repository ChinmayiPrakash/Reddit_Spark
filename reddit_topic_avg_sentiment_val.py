# reddit_topic_avg_sentiment_val.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg
from pyspark.sql.types import StructType, StructField, StringType, FloatType
from pyspark.sql import functions as F

# Initialize Spark session
spark = SparkSession.builder.appName("RedditSentimentAnalysis").getOrCreate()

# Define the schema of the data
schema = StructType([
    StructField("text", StringType(), True),
    StructField("senti_val", FloatType(), True)
])

# Read data from Kafka
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "reddit") \
    .load()

# Convert the Kafka data to a structured format (text and sentiment value)
reddit_df = kafka_df.selectExpr("CAST(value AS STRING)") \
    .select(F.from_json("value", schema).alias("data")) \
    .select("data.text", "data.senti_val")

# Compute the average sentiment value in real-time
avg_sentiment_df = reddit_df.groupBy().agg(avg("senti_val").alias("avg_senti_val"))

# Add the status column based on average sentiment
def sentiment_status(avg_sentiment):
    if avg_sentiment > 0:
        return "POSITIVE"
    elif avg_sentiment < 0:
        return "NEGATIVE"
    else:
        return "NEUTRAL"

# Register a UDF for the sentiment status
spark.udf.register("sentiment_status", sentiment_status)

# Add the status column to the DataFrame
status_df = avg_sentiment_df.withColumn("status", F.expr("sentiment_status(avg_senti_val)"))

# Write the output to the console
query = status_df.writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

# Wait for the stream to finish
query.awaitTermination()
