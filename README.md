# YouTube Big Data Analysis (Gaza 2024)


A distributed Big Data pipeline for analyzing YouTube comments using a Hadoop Cluster (Docker) and PySpark.

##  Features
- **Distributed Infrastructure:** 1 NameNode + 4 DataNodes using Docker.
- **Data Ingestion:** Automated collection of YouTube video metadata and comments into HDFS.
- **Processing:** PySpark pipeline for cleaning, tokenization, and Word Count.
- **Analytics:** Sentiment analysis (TextBlob) and engagement tracking.

##  Tech Stack
- **Storage:** HDFS (Hadoop 3.x)
- **Engine:** Apache Spark (PySpark)
- **Containerization:** Docker / Docker-Compose
- **Language:** Python 3.x

##  Quick Start
1. **Launch Cluster:** `docker-compose up -d`
2. **Ingest Data:** `python script.py`
3. **Run Analysis:** `python analysis.py`
4. **Visualize:** `python vis.py`
