# PySpark ETL

import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("YTAnalytics").getOrCreate()

raw_df = spark.read.format("json").load("s3://yt-data/raw/") 

filtered_df = raw_df.filter(raw_df.country.isin(["US", "IN", "GB"]))
cleaned_df = filtered_df.na.fill(0) 
enriched_df = cleaned_df.withColumn("engagement", col("likes")/col("views"))

enriched_df.write.parquet("s3://yt-data/transformed/")


# Athena table creation 
CREATE EXTERNAL TABLE yt_data_transformed(
  video_id STRING,
  title STRING,  
  views INT,
  likes INT,
  engagement FLOAT)
STORED AS PARQUET
LOCATION 's3://yt-data/transformed/';

# SQL Queries: 

# Query 1: Find top 10 trending videos by total views:
query1 = """
SELECT *
FROM yt_data_transformed
ORDER BY views DESC
LIMIT 10;
"""

# Query 2: Analyze engagement ratio by category:
query2 = """  
SELECT category, AVG(engagement) AS avg_engagement
FROM yt_data_transformed
GROUP BY category
ORDER BY avg_engagement DESC; 
"""

# Query 3: See view count distribution:
query3 = """
SELECT COUNT(*) AS num_videos,  
       CASE WHEN views BETWEEN 0 AND 1000 THEN '0-1K'
            WHEN views BETWEEN 1001 AND 10000 THEN '1K-10K' 
            WHEN views BETWEEN 10001 AND 100000 THEN '10K-100K'
            ELSE '100K+' END AS view_range
FROM yt_data_transformed 
GROUP BY view_range;
"""

# Query 4: Identify rising channels by new videos posted:
query4 = """
SELECT channel, COUNT(DISTINCT video_id) AS new_videos
FROM yt_data_transformed
WHERE DATE(upload_date) = CURRENT_DATE  
GROUP BY channel
ORDER BY new_videos DESC; 
"""

# Query 5: Analyze engagement by time of day:
query5 = """
SELECT HOUR(upload_time) AS hour_of_day,  
       AVG(engagement) AS avg_engagement
FROM yt_data_transformed
GROUP BY hour_of_day
ORDER BY hour_of_day;
"""

# Query 6: Track engagement over time:
query6 = """
SELECT DATE(upload_date) AS upload_date,   
       AVG(engagement) AS avg_engagement
FROM yt_data_transformed 
GROUP BY upload_date
ORDER BY upload_date;
"""

# Query 7: Identify low performing videos:
query7 = """
SELECT *
FROM yt_data_transformed
WHERE engagement < 0.01
ORDER BY engagement ASC;
"""

# Query 8: See tags associated with most liked videos:
query8 = """
SELECT tags, COUNT(video_id) AS videos 
FROM yt_data_transformed
WHERE likes > 10000 
GROUP BY tags
ORDER BY videos DESC;  
"""

# Query 9: Analyze user comments for insight:
query9 = """ 
SELECT * FROM yt_data_transformed
WHERE LENGTH(user_comments) > 120  
ORDER BY likes DESC
LIMIT 50;
"""


# Query 10: Compare metrics by country:
query10 = """
SELECT country, AVG(views) AS avg_views,  
       AVG(engagement) AS avg_engagement
FROM yt_data_transformed
GROUP BY country
ORDER BY avg_views DESC;
"""