from pyspark.sql import SparkSession

spark = SparkSession \
    .builder\
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0") \
    .appName("balance-events").getOrCreate()

kafkaRawStreamingDF = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers","localhost:9092") \
    .option("subscribe","balance-updates") \
    .option("startingOffsets","earliest") \
    .load()

kafkaStreamingDF = kafkaRawStreamingDF.selectExpr("cast(key as string) key","cast(value as string) value")

kafkaStreamingDF \
    .selectExpr("cast(key as string) key","cast(value as string) value") \
    .writeStream \
    .format("console") \
    .start() \
    .awaitTermination()