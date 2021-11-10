from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, to_json, col, unbase64, base64, split, expr
from pyspark.sql.types import StructField, StructType, StringType, BooleanType, ArrayType, DateType

# this is a manually created schema - before Spark 3.0.0, schema inference is not automatic

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

# TO-DO: create a StructType for the Customer schema for the following fields:
# {"customerName":"Frank Aristotle","email":"Frank.Aristotle@test.com","phone":"7015551212","birthDay":"1948-01-01","accountNumber":"750271955","location":"Jordan"}
customerSchema = StructType(
    [
        StructField("customerName", StringType()),
        StructField("email", StringType()),
        StructField("phone", StringType()),
        StructField("birthDay", StringType()),
        StructField("accountNumber", StringType()),
        StructField("location", StringType())
    ]
)

# TO-DO: create a StructType for the CustomerLocation schema for the following fields:
# {"accountNumber":"814840107","location":"France"}

customerLocationSchema = StructType(
    [
        StructField("accountNumber", StringType()),
        StructField("location", StringType())
    ]
)

# TO-DO: create a spark session, with an appropriately named application name
spark = SparkSession.builder.appName("customer-country").getOrCreate()

#TO-DO: set the log level to WARN
spark.sparkContext.setLogLevel("WARN")

#TO-DO: read the redis-server kafka topic as a source into a streaming dataframe with the bootstrap server localhost:9092, configuring the stream to read the earliest messages possible                                    
redisRawData = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:19092") \
        .option("subscribe","redis-server") \
        .option("startingOffsets","earliest") \
        .load()

#TO-DO: using a select expression on the streaming dataframe, cast the key and the value columns from kafka as strings, and then select them
redisDataStreamingDF = redisRawData.selectExpr("cast(key as string) as key", "cast(value as string) as value")

#TO-DO: using the redisMessageSchema StructType, deserialize the JSON from the streaming dataframe 
# TO-DO: create a temporary streaming view called "RedisData" based on the streaming dataframe
# it can later be queried with spark.sql
redisDataStreamingDF.withColumn("value", from_json("value", redisMessageSchema)) \
            .select(col("value.*")) \
            .createOrReplaceTempView("RedisData")

#TO-DO: using spark.sql, select key, zSetEntries[0].element as redisEvent from RedisData
redisEncodedStreamingDF = spark.sql("select key, zSetEntries[0].element as redisEvent from RedisData")

#TO-DO: from the dataframe use the unbase64 function to select a column called redisEvent with the base64 decoded JSON, and cast it to a string
redisDecodedEntriesStreamingDF1 = redisEncodedStreamingDF \
            .withColumn("redisEvent", unbase64(redisEncodedStreamingDF.redisEvent).cast("string"))

#TO-DO: repeat this a second time, so now you have two separate dataframes that contain redisEvent data
redisDecodedEntriesStreamingDF2 = redisEncodedStreamingDF \
            .withColumn("redisEvent", unbase64(redisEncodedStreamingDF.redisEvent).cast("string"))


#TO-DO: using the customer StructType, deserialize the JSON from the first redis decoded streaming dataframe, selecting column customer.* as a temporary view called Customer 
redisDecodedEntriesStreamingDF1.filter(col("redisEvent").contains("birthDay"))

# Filter DF2 for only those that do not contain the birthDay field (all records other than customer) we will filter out null rows later
redisDecodedEntriesStreamingDF2.filter(~col("redisEvent").contains("birthDay"))

#TO-DO: using the customer location StructType, deserialize the JSON from the second redis decoded streaming dataframe, selecting column customerLocation.* as a temporary view called CustomerLocation 
# Now we are parsing JSON from the redisEvent that contains customer record data
redisDecodedEntriesStreamingDF1\
    .withColumn("customer", from_json("redisEvent", customerSchema))\
    .select(col('customer.*'))\
    .createOrReplaceTempView("Customer")\

# Last we are parsing JSON from the redisEvent that contains customer location data
redisDecodedEntriesStreamingDF2\
    .withColumn("customerLocation", from_json("redisEvent", customerLocationSchema))\
    .select(col('customerLocation.*'))\
    .createOrReplaceTempView("CustomerLocation")\

#TO-DO: using spark.sql select accountNumber as customerAccountNumber, location as homeLocation, birthDay from Customer where birthDay is not null
customerDataBirthday = spark.sql("select accountNumber as customerAccountNumber, location as homeLocation, birthDay from Customer where birthDay is not null")


#TO-DO: select the customerAccountNumber, homeLocation, and birth year (using split)
customerDataBirthyear =  customerDataBirthday \
    .select("customerAccountNumber", "homeLocation",\
    split(customerDataBirthday.birthDay, "-").getItem(0).alias("birthYear"))


#TO-DO: using spark.sql select accountNumber as locationAccountNumber, and location
customerDataLocation = spark.sql("select accountNumber as locationAccountNumber, location from CustomerLocation")


#TO-DO: join the customer and customer location data using the expression: customerAccountNumber = locationAccountNumber
currentAndHomeLocationStreamingDF = customerDataLocation.join(customerDataBirthyear,\
    expr("customerAccountNumber = locationAccountNumber"))

currentAndHomeLocationStreamingDF.writeStream.outputMode("append").format("console").start().awaitTermination()

# TO-DO: write the stream to the console, and configure it to run indefinitely
# can you find the customer(s) who are traveling out of their home countries?
# When calling the customer, customer service will use their birth year to help
# establish their identity, to reduce the risk of fraudulent transactions.
# +---------------------+-----------+---------------------+------------+---------+
# |locationAccountNumber|   location|customerAccountNumber|homeLocation|birthYear|
# +---------------------+-----------+---------------------+------------+---------+
# |            982019843|  Australia|            982019843|   Australia|     1943|
# |            581813546|Phillipines|            581813546| Phillipines|     1939|
# |            202338628|Phillipines|            202338628|       China|     1944|
# |             33621529|     Mexico|             33621529|      Mexico|     1941|
# |            266358287|     Canada|            266358287|      Uganda|     1946|
# |            738844826|      Egypt|            738844826|       Egypt|     1947|
# |            128705687|    Ukraine|            128705687|      France|     1964|
# |            527665995|   DR Congo|            527665995|    DR Congo|     1942|
# |            277678857|  Indonesia|            277678857|   Indonesia|     1937|
# |            402412203|   DR Congo|            402412203|    DR Congo|     1945|
# +---------------------+-----------+---------------------+------------+---------+
