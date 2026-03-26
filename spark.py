from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import StringType, DoubleType, LongType

spark = SparkSession.builder \
    .appName("ServerFailureDetection") \
    .getOrCreate()

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "server_logs") \
    .load()

schema = StructType([
    StructField("timestamp", StringType(), True),
    StructField("machine", StringType(), True),

    StructField("cpu_usage", DoubleType(), True),
    StructField("gpu_wrk_util", DoubleType(), True),

    StructField("avg_mem", DoubleType(), True),
    StructField("max_mem", DoubleType(), True),

    StructField("avg_gpu_wrk_mem", DoubleType(), True),
    StructField("max_gpu_wrk_mem", DoubleType(), True),

    StructField("read", LongType(), True),
    StructField("write", LongType(), True),
    StructField("read_count", LongType(), True),
    StructField("write_count", LongType(), True)
])

from pyspark.sql.functions import from_json, col

# Parse the JSON data from the Kafka topic using the defined schema
parsed_df = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

# Output the parsed streaming data to the console
query = parsed_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()
