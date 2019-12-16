from pyspark.sql import SparkSession


def SparkConfig() -> object:
    spark = SparkSession \
        .builder \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.kryoserializer.buffer", "512k") \
        .config("spark.kryoserializer.buffer.max", "512m") \
        .config("spark.sql.parquet.filterPushdown", "true") \
        .config("spark.sql.parquet.mergeSchema", "false") \
        .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2") \
        .config("spark.speculation", "false") \
        .config("spark.network.timeout", "10000000") \
        .config("spark.executor.heartbeatInterval", "10000000") \
        .config("spark.executor.memory", "6g") \
        .config("spark.driver.memory", "6g") \
        .config("spark.executor.pyspark.memory", "6g") \
        .master("local[10]") \
        .getOrCreate()
    return spark
