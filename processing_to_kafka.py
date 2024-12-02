from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType
from pyspark.sql import SparkSession
from configs import kafka_config
import pandas as pd
import os
import time

import os
os.environ['PYSPARK_PYTHON'] = r'C:\ProgramData\anaconda3\envs\env_DE11\python.exe'
os.environ['PYSPARK_DRIVER_PYTHON'] = r'C:\ProgramData\anaconda3\envs\env_DE11\python.exe'

# Пакет, необхідний для читання Kafka зі Spark
os.environ[
    'PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.5.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 pyspark-shell'

# Створення SparkSession
spark = (SparkSession.builder
         .appName("KafkaStreaming")
         .master("local[*]")
         .getOrCreate())

# Читання потоку даних із Kafka
# Вказівки, як саме ми будемо під'єднуватися, паролі, протоколи
# maxOffsetsPerTrigger - будемо читати 5 записів за 1 тригер.


df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_config['bootstrap_servers'][0]) \
    .option("kafka.security.protocol", "SASL_PLAINTEXT") \
    .option("kafka.sasl.mechanism", "PLAIN") \
    .option("kafka.sasl.jaas.config",
            'org.apache.kafka.common.security.plain.PlainLoginModule required username="admin" password="VawEzo1ikLtrA8Ug8THa";') \
    .option("subscribe", "building_sensors_rudyi") \
    .option("startingOffsets", "earliest") \
    .option("maxOffsetsPerTrigger", "30") \
    .load()

json_schema = StructType([
    StructField("temperature", DoubleType(), True),
    StructField("humidity", DoubleType(), True),
    StructField("timestamp", StringType(), True)
])

# Маніпуляції з даними
clean_df = df.selectExpr("CAST(key AS STRING) AS key_deserialized", "CAST(value AS STRING) AS value_deserialized", "*") \
    .drop('key', 'value') \
    .withColumnRenamed("key_deserialized", "key") \
    .withColumn("value_json", from_json(col("value_deserialized"), json_schema)) \
    .withColumn("timestamp", from_unixtime(col("value_json.timestamp").cast(DoubleType())).cast("timestamp")) \
    .withColumn("temperature", col("value_json.temperature")) \
    .withColumn("humidity", col("value_json.humidity")) \
    .drop("value_json", "value_deserialized") \
    .withWatermark("timestamp", "10 seconds")

aggregated_df = clean_df.groupBy(
    window(clean_df.timestamp, "1 minute", "30 seconds"),
    clean_df.key
).agg(
    avg(clean_df.temperature).alias("avg_temperature"),
    avg(clean_df.humidity).alias("avg_humidity")
)


conditions_df = spark.read.csv("hw_06/alerts_conditions.csv",
                               header=True)

alerts_df = aggregated_df.crossJoin(conditions_df) \
    .filter(
    ((col("avg_temperature") >= col("temperature_min")) &
     (col("avg_temperature") <= col("temperature_max"))
     ) | (
        (col("avg_humidity") >= col("humidity_min")) &
        (col("avg_humidity") <= col("humidity_max"))
    ))

prepare_to_kafka_df = alerts_df.select(
    col("key"),
    to_json(struct(col("window"), col("avg_temperature").alias('t_avg'),
            col("avg_humidity").alias('h_avg'), col("code"), col("message"), current_timestamp().alias("timestamp"))).alias("value"))

# displaying_df = prepare_to_kafka_df.writeStream \
#     .trigger(processingTime="60 seconds") \
#     .outputMode("append") \
#     .format("console") \
#     .option("truncate", False) \
#     .start() \
#     .awaitTermination()

(prepare_to_kafka_df.writeStream
    .trigger(processingTime='10 seconds')
    .foreachBatch(lambda batch_df, batch_id: print(batch_df))
    .format("kafka")
    .option("kafka.bootstrap.servers", "77.81.230.104:9092")
    .option("topic", "alerts_rudyi")
    .option("kafka.security.protocol", "SASL_PLAINTEXT")
    .option("kafka.sasl.mechanism", "PLAIN")
    .option("kafka.sasl.jaas.config",
            "org.apache.kafka.common.security.plain.PlainLoginModule required username='admin' password='VawEzo1ikLtrA8Ug8THa';")
    .option("checkpointLocation", "/tmp/checkpoints-3")
    .start()
    .awaitTermination())

spark.stop()



