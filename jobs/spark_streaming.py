from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, FloatType

def start_streaming(spark):
    # MinIO Configuration
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.endpoint", "http://localhost:9000")  # Use 'http://minio:9000' if running within Docker
    hadoop_conf.set("fs.s3a.access.key", "minio_access_key")  # Update to match your MinIO setup
    hadoop_conf.set("fs.s3a.secret.key", "minio_secret_key")  # Update to match your MinIO setup
    hadoop_conf.set("fs.s3a.path.style.access", "true")
    hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

    # Define the socket stream
    stream_df = (spark.readStream.format("socket")
                 .option("host", "localhost")  # Replace with the correct host
                 .option("port", 9999)
                 .load())

    # Define the schema for incoming JSON data
    schema = StructType([
        StructField("review_id", StringType()),
        StructField("user_id", StringType()),
        StructField("business_id", StringType()),
        StructField("stars", FloatType()),
        StructField("date", StringType()),
        StructField("text", StringType())
    ])

    # Parse JSON data
    parsed_df = stream_df.select(
        from_json(col("value"), schema).alias("data")
    ).select("data.*")

    # Write stream to MinIO bucket (bronze) in Parquet format
    query = (parsed_df.writeStream
             .format("parquet")
             .option("path", "s3a://bronze/streaming_data/")  # MinIO bucket path
             .option("checkpointLocation", "s3a://bronze/checkpoints/streaming_data/")  # Checkpoint directory
             .outputMode("append")
             .start())

    query.awaitTermination()


if __name__ == "__main__":
    # Create Spark session
    spark = (SparkSession.builder
             .appName("SocketStreamToMinIO")
             .getOrCreate())

    # Start the streaming process
    start_streaming(spark)
