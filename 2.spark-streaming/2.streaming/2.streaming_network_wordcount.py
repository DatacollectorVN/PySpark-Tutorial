from pyspark import SparkContext
from pyspark.streaming import StreamingContext

### CREATE SPARK CONTEXT AND STREAMING CONTEXT
# Create a local StreamingContext with two working thread and batch interval of 1 second
sc = SparkContext("local[2]", "NetworkWordCount")
ssc = StreamingContext(sc, 1) # 1 mean batch interval of 1s.
sc.setLogLevel("ERROR")

### CREATE DStream
# Create a DStream that will connect to hostname:port, like localhost:9999
lines = ssc.socketTextStream("localhost", 9999)

print(f'type(lines): {type(lines)}')

### Process Dstream
# Split each line into words
words = lines.flatMap(lambda line: line.split(" "))
# Count each word in each batch
pairs = words.map(lambda word: (word, 1))
wordCounts = pairs.reduceByKey(lambda x, y: x + y)

print(f'type(wordCounts): {type(wordCounts)}')
# Print the first ten elements of each RDD generated in this DStream to the console
wordCounts.pprint()

ssc.start()             # Start the computation 
ssc.awaitTermination()  # Wait for the computation to terminate