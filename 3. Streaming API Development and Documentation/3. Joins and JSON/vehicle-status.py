from pyspark.sql import SparkSession

#TO-DO: create a Spark Session, and name the app something relevant
spark = SparkSession \
    .builder\
    .appName("balance-events").getOrCreate()