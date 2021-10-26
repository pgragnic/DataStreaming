from pyspark.sql import SparkSession

spark = SparkSession \
    .builder\
    .appName("fuel-level").getOrCreate()

spark.sparkContext.setLogLevel("WARN")

kafkaRawStreamingDF = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers","kafka:19092") \
    .option("subscribe","fuel-level") \
    .option("startingOffsets","earliest") \
    .load()

kafkaStreamingDF = kafkaRawStreamingDF \
    .selectExpr("cast(key as string) key","cast(value as string) value")

kafkaStreamingDF \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .start() \
    .awaitTermination()