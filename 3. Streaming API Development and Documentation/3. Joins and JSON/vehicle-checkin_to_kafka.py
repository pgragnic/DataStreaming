from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, expr
from pyspark.sql.types import StructField, StructType, StringType, IntegerType


# Define schemas
vehicle_status_schema = StructType (
  [
    StructField("truckNumber", StringType()),
    StructField("destination", StringType()),
    StructField("milesFromShop", IntegerType()),
    StructField("odometerReading", IntegerType())
  ]
)

checkin_schema = StructType (
  [
    StructField("reservationId", StringType()),
    StructField("locationName", StringType()),
    StructField("truckNumber", StringType()),
    StructField("status", StringType())
  ]
)

# Create Spark session
spark = SparkSession \
    .builder\
    .appName("vehicle-checkin")\
    .config("spark.streaming.backpressure.initialRate", "50") \
    .config("spark.streaming.backpressure.enabled", "true") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Create vehicle status dataframe
vehicle_status_RawStreamingDF = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers","kafka:19092") \
    .option("subscribe","vehicle-status") \
    .option("startingOffsets","earliest") \
    .load()

vehicle_status_StreamingDF = vehicle_status_RawStreamingDF.selectExpr("cast(key as string) key", "cast(value as string) value")

vehicle_status_StreamingDF \
              .withColumn("value", from_json("value", vehicle_status_schema)) \
              .select(col("value.*")) \
              .createOrReplaceTempView("vehicle_status_view")

vehicle_status_StreamingStarDF = spark.sql("select truckNumber as statusTruckNumber, \
                                    destination, milesFromShop, odometerReading from vehicle_status_view")

# Create vehicle checkin dataframe
checkin_RawStreamingDF = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers","kafka:19092") \
    .option("subscribe","check-in") \
    .option("startingOffsets","earliest") \
    .load()

checkin_StreamingDF = checkin_RawStreamingDF.selectExpr("cast(key as string) key", "cast(value as string) value")

checkin_StreamingDF \
              .withColumn("value", from_json("value", checkin_schema)) \
              .select(col("value.*")) \
              .createOrReplaceTempView("checkin_view")

checkin_StreamingStarDF = spark.sql("select truckNumber as checkinTruckNumber, \
                            reservationId, locationName, status from checkin_view")

checkin_status_StreamingStarDF = vehicle_status_StreamingStarDF.join(checkin_StreamingStarDF, expr(
        """ statusTruckNumber = checkinTruckNumber """
        ))

#checkin_status_StreamingStarDF \
#    .writeStream \
#    .outputMode("append") \
#    .format("console") \
#    .start() \
#    .awaitTermination()

checkin_status_StreamingStarDF\
  .selectExpr("cast(statusTruckNumber as string) as key", "to_json(struct(*)) as value") \
  .writeStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "kafka:19092")\
  .option("topic", "checkin-status")\
  .option("checkpointLocation","/tmp/kafkacheckpoint")\
  .start()\
  .awaitTermination()
