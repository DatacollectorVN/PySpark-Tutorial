from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split

### CREATE SPARK SESSION
'''
To create a basic SparkSession, just use SparkSession.builder:
'''
spark = SparkSession \
    .builder \
    .appName("StructuredNetworkWordCount") \
    .getOrCreate()

# stop DEBUG and INFO messages
spark.sparkContext.setLogLevel("ERROR")
### READ DATA
'''
Create DataFrame representing the stream of input lines from connection to localhost:9999
Use readStream.format("socket") from Spark session object to read data from the socket 
and provide options host and port where you want to stream data from.
'''
lines = spark \
    .readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .load()


# for check the data type of lines
print('type(lines):', type(lines))
# expected output: pyspark.sql.dataframe.DataFrame

### PROCESS DATA
'''
Split the lines into words.
Why lines.value ?
- This lines DataFrame represents an unbounded table containing the streaming text data. 
- This table contains one column of strings named “value”, and each line in the streaming text data becomes a row in the table.
'''
words = lines.select(
                    explode(split(lines.value, " ")).alias("word")
                    )

# Generate running word count
wordCounts = words.groupBy("word").count()

# for check the data type of wordcount
print('type(wordCounts):', type(wordCounts))
# expected output: pyspark.sql.dataframe.DataFrame

### QUERY DATA
query = wordCounts \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

# outputMode, read here. https://sparkbyexamples.com/spark/spark-streaming-outputmode/
'''
Use complete as output mode outputMode("complete") when you want to 
aggregate the data and output the entire results to sink every time. 
This mode is used only when you have streaming aggregated data. 
'''

query.awaitTermination()