<h1> YouTube Data Analytics on AWS </h1>

This project performs basic data analytics on YouTube dataset using AWS services like S3, Lambda, Athena and QuickSight.

<h2> Data Lake Architecture </h2>

The data is stored in a S3 data lake for scalable and cost-efficient storage. S3 buckets are created for raw zone and processed zone. Lambda functions process and transform data from raw into curated datasets for analytics.

<h2>  Data Ingestion </h2>

The raw YouTube data is downloaded from Kaggle in JSON format. The data covers statistics and metadata for thousands of videos.

The files are uploaded to a S3 bucket in raw zone using AWS CLI: <i> aws s3 cp youtube-data.json s3://raw-youtube-data </i>

<h2> Data Processing </h2>

A Lambda function is triggered on new files arriving in raw S3 bucket. The function:

Reads the raw JSON data
Extracts relevant fields into a Pandas dataframe
Saves dataframe to parquet format in processed bucket
Python libraries like Pandas, AWS Data Wrangler are used for data processing.

<h2> Lambda IAM Role </h2>

The Lambda function is assigned an IAM role with S3 and CloudWatch Logs permissions. This allows the function to read from raw bucket and write to processed bucket.

<h2>Athena Table</h2>

An Athena table is created over the processed S3 data to make it queryable via SQL. The OpenCSV Serde is used to infer schema from the data.

<h2>Analytics</h2>

QuickSight connects to the Athena table to visualize and find insights from the YouTube data:

Top videos by views, likes, duration
Category-wise statistics
Daily/monthly trends
Dashboards are created in QuickSight to share with stakeholders.

Further analytics could extract usage metrics, predict popular videos, and more.
