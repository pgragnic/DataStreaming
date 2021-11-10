from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, to_json, col, unbase64, base64, split, expr
from pyspark.sql.types import StructField, StructType, StringType, BooleanType, ArrayType, DateType

redisMessageSchema = StructType(
    [
        StructField("key", StringType()),
        StructField("value", StringType()),
        StructField("expiredType", StringType()),
        StructField("expiredValue",StringType()),
        StructField("existType", StringType()),
        StructField("ch", StringType()),
        StructField("incr",BooleanType()),
        StructField("zSetEntries", ArrayType( \
            StructType([
                StructField("element", StringType()),\
                StructField("score", StringType())   \
            ]))                                      \
        )

    ]
)

# TO-DO: create a StructType for the CustomerLocation schema for the following fields:
# {"accountNumber":"814840107","location":"France"}
customerLocationSchema = StructType (
    [
        StructField("accountNumber", StringType()),
        StructField("location", StringType())
    ]   
)

# TO-DO: create a spark session, with an appropriately named application name
spark = SparkSession.builder.appName("customer-location").getOrCreate()

#TO-DO: set the log level to WARN
spark.sparkContext.setLogLevel('WARN')

#TO-DO: read the redis-server kafka topic as a source into a streaming dataframe with the bootstrap server localhost:9092, configuring the stream to read the earliest messages possible                                    
redisRawStreamingDF = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:19092") \
    .option("subscribe","redis-server") \
    .option("startingOffsets","earliest") \
    .load()

#TO-DO: using a select expression on the streaming dataframe, cast the key and the value columns from kafka as strings, and then select them
redisStreamingDF = redisRawStreamingDF.selectExpr("cast(key as string) key", "cast(value as string) value")

#TO-DO: using the redisMessageSchema StructType, deserialize the JSON from the streaming dataframe
# TO-DO: create a temporary streaming view called "RedisData" based on the streaming dataframe
# it can later be queried with spark.sql 
redisStreamingDF.withColumn("value", from_json("value", redisMessageSchema)) \
            .select(col("value.*")) \
            .createOrReplaceTempView("RedisData")

#TO-DO: using spark.sql, select key, zSetEntries[0].element as customerLocation from RedisData
redisEncodedStreamingDF = spark.sql("select key, zSetEntries[0].element as customerLocation from RedisData")

#TO-DO: from the dataframe use the unbase64 function to select a column called customerLocation with the base64 decoded JSON, and cast it to a string
redisDecodedEntriesStreamingDF = redisEncodedStreamingDF \
            .withColumn("customerLocation", unbase64(redisEncodedStreamingDF.customerLocation).cast("string"))

#TO-DO: using the customer location StructType, deserialize the JSON from the streaming dataframe, selecting column customer.* as a temporary view called CustomerLocation 
redisDecodedEntriesStreamingDF \
            .withColumn("customerLocation", from_json("customerLocation", customerLocationSchema)) \
            .select(col("customerLocation.*")) \
            .createOrReplaceTempView("CustomerLocation")


#TO-DO: using spark.sql select * from CustomerLocation
customerLocationStartResult = spark.sql("select * from CustomerLocation where location is not null")

#df.filter(col("name").like("%mes%"))

#TO-DO: write the stream to the console, and configure it to run indefinitely, the console output will look something like this:
customerLocationStartResult \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .start()\
    .awaitTermination()

# +-------------+---------+
# |accountNumber| location|
# +-------------+---------+
# |         null|     null|
# |     93618942|  Nigeria|
# |     55324832|   Canada|
# |     81128888|    Ghana|
# |    440861314|  Alabama|
# |    287931751|  Georgia|
# |    413752943|     Togo|
# |     93618942|Argentina|
# +-------------+---------+