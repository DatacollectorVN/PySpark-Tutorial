from pyspark.sql import SparkSession
from pyspark.sql.functions import *

from lib.logger import Log4j

if __name__ == "__main__":
    # spark = SparkSession \
    #     .builder \
    #     .appName("Streaming Word Count") \
    #     .master("local[3]") \
    #     .config("spark.streaming.stopGracefullyOnShutdown", "true") \
    #     .config("spark.sql.shuffle.partitions", 3) \
    #     .getOrCreate()
    spark = SparkSession \
        .builder \
        .appName("Streaming Word Count") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .config("spark.sql.shuffle.partitions", 3) \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    logger = Log4j(spark)

    lines_df = spark.readStream \
        .format("socket") \
        .option("host", "localhost") \
        .option("port", "9999") \
        .load()

    # lines_df.printSchema()
    
    # don't use `expr` function.
    # words_df = lines_df.select(explode(split("value", " ")).alias("word"))
    
    # use `expr` function.
    words_df = lines_df.select(expr("explode(split(value,' ')) as word"))
    counts_df = words_df.groupBy("word").count()

    ''' write stream with into checkpoint --> this don't show in terminal.
    '''
    # word_count_query = counts_df.writeStream \
    #     .format("console") \
    #     .outputMode("complete") \
    #     .option("checkpointLocation", "chk-point-dir") \
    #     .start()

    word_count_query = counts_df.writeStream \
        .format("console") \
        .outputMode("complete") \
        .start()

    logger.info("Listening to localhost:9999")
    word_count_query.awaitTermination()