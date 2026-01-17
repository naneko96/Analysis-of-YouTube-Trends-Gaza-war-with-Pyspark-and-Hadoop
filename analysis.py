import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, lower, regexp_replace, split
import pandas as pd
from textblob import TextBlob

# 1. Initialize Spark
spark = SparkSession.builder \
    .appName("GazaBigDataAnalysis") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
    .getOrCreate()

try:
    print("Reading data from HDFS...")
    df = spark.read.option("multiLine", "true").json("webhdfs://localhost:9870/user/youtube/data.json")

    # --- PART A: DISTRIBUTED WORD COUNT (The "Big Data" Requirement) ---
    print("Performing distributed Word Count...")
    comments_df = df.select(explode(col("comments")).alias("comment_text"))
    clean_comments = comments_df.withColumn("clean_text", 
        lower(regexp_replace(col("comment_text"), r"<[^>]+>|http\S+|[^a-zA-Z\s]", "")))
    
    words = clean_comments.withColumn("word", explode(split(col("clean_text"), r"\s+")))
    word_counts = words.filter("length(word) > 4").groupBy("word").count().orderBy(col("count").desc())
    
    # Save Keywords
    word_counts.limit(20).toPandas().to_csv("top_keywords.csv", index=False)

    # --- PART B: SENTIMENT ANALYSIS (Post-Processing) ---
    print("Analyzing sentiments...")
    # We pull the cleaned comments to Pandas to avoid the Docker environment issue
    local_comments = clean_comments.select("clean_text").limit(1000).toPandas()

    def get_sentiment(text):
        if not text: return "Neutral"
        analysis = TextBlob(str(text))
        if analysis.sentiment.polarity > 0.1: return "Positive"
        elif analysis.sentiment.polarity < -0.1: return "Negative"
        else: return "Neutral"

    local_comments['sentiment'] = local_comments['clean_text'].apply(get_sentiment)
    sentiment_summary = local_comments['sentiment'].value_counts().reset_index()
    sentiment_summary.columns = ['sentiment', 'count']
    sentiment_summary.to_csv("sentiment_results.csv", index=False)

    # --- PART C: ENGAGEMENT ANALYSIS ---
    print("Analyzing video engagement...")
    engagement = df.select("title", "views", "likes").orderBy(col("views").desc())
    engagement.limit(10).toPandas().to_csv("engagement_results.csv", index=False)

    print("✅ ALL ANALYSES COMPLETE. Results saved to CSV files.")

except Exception as e:
    print(f"❌ Error: {e}")
finally:
    spark.stop()