from pyspark.sql import SparkSession

# TO-DO: create a variable with the absolute path to the text file
# /home/workspace/lesson-1-streaming-dataframes/exercises/starter/Test.txt
path = "test.txt"

# TO-DO: create a Spark session
spark = SparkSession \
    .builder\
    .appName("hello-spark").getOrCreate()

# TO-DO: set the log level to WARN
spark.sparkContext.setLogLevel("WARN")

# TO-DO: using the Spark session variable, call the appropriate
# function referencing the text file path to read the text file 

text_data = spark.read.text(path).cache()

# TO-DO: create a global variable for number of times the letter a is found
a_found = 0
# TO-DO: create a global variable for number of times the letter b is found
b_found = 0

# TO-DO: create a function which accepts a row from a dataframe, which has a column called value
def find_a(row):
    # in the function increment the a count variable for each occurrence of the letter a
    # in the value column
    global a_found
    a_found += row.value.count("a")
    print(f"a found: {a_found}")

# TO-DO: create another function which accepts a row from a dataframe, which has a column called value
def find_b(row):
    # in the function increment the a count variable for each occurrence of the letter a
    # in the value column
    global b_found
    b_found += row.value.count("b")
    print(f"b found: {b_found}")

# TO-DO: use the forEach method to invoke the a counting method
text_data.foreach(find_a)

# TO-DO: use the forEach method to invoke the b counting method
text_data.foreach(find_b)

# TO-DO: stop the spark application
spark.stop()
