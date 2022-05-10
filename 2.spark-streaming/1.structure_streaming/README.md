# PySpark-Tutorial
My self-learning about PySpark

## 0.1.Spark Submit 
The `spark-submit` command is a utility to run or submit a `Spark` or `PySpark` application program (or job) to the `cluster` by specifying options and configurations, the application you are submitting can be written in `Scala`, `Java`, or `Python` (`PySpark`). `spark-submit` command supports the following.

Read 2 document for more understanding: [spark.apache.org](https://spark.apache.org/docs/latest/submitting-applications.html#master-urls) and [sparkbyexamples.com](https://sparkbyexamples.com/spark/spark-submit-command/).

## 0.2.Netcat
`Netcat` is a command line tool responsible for reading and writing data in the network. To exchange data, `Netcat` uses the network protocols TCP/IP and UDP. The tool originally comes from the world of Unix but is now available for all platforms.

Read [here](https://www.ionos.com/digitalguide/server/tools/netcat/) for more understanding.

2 paramenters we use:

- `-l`: Listen and server mode for incoming connection requests (via port indicated).
- `-k`: At the end of a connection, Netcat waits for a new connection (only possible with GNU Netcat and only in combination with “-l”)

## 1. Structure streaming
`Structured Streaming` is a **scalable** and **fault-tolerant** stream processing engine built on the `Spark SQL` engine. You can express your streaming computation the same way you would express a batch computation on static data. The `Spark SQL` engine will take care of running it incrementally and continuously and updating the final result as streaming data continues to arrive. You can use the Dataset/DataFrame API in Scala, Java, Python or R to express streaming aggregations, event-time windows, stream-to-batch joins, etc. The computation is executed on the same optimized `Spark SQL` engine. Finally, **the system ensures end-to-end exactly-once fault-tolerance guarantees through checkpointing and Write-Ahead Logs**. In short, `Structured Streaming` provides fast, scalable, fault-tolerant, end-to-end exactly-once stream processing without the user having to reason about streaming.

## Quickly example:
Source [here](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)

Read carefully file `1.structured_network_wordcount.py`, we explained detail.

For running structure streaming, you need run 2 terminal:

**First terminal**: running **Netcat server** for simulating the `socket resource` via port `9999`.
```bash
nc -lk 9999
```

**Second terminal**: running structure streaming. You have 2 ways:

*Running without submit `PySpark` application program (or job) to the `cluster manager`*:
```bash
python 1.structured_network_wordcount.py localhost 9999
```

*Note:* You must running **Netcat server** before running structure streaming.

*Submit `PySpark` application program (or job) to the `cluster manager`*:

First of all, check the `bin/spark-submit` location, in our case, we dowloaded `apache spark` via `homebrew`. Therefore, it located at `/opt/homebrew/Cellar/apache-spark/3.2.1/bin/spark-submit`.

Then running:
```bash
/opt/homebrew/Cellar/apache-spark/3.2.1/bin/spark-submit 2.spark-streaming/1.getting-start/1.structured_network_wordcount.py.py localhost 9999
```
#### When running
Any lines typed in the terminal running the netcat server will be counted and printed on screen every second. It will look something like the following

**First batch:**

In **Netcat server** termnial:
```bash
nc -lk 9999
Nathan Ngo
```

In structure streaming terminal:
```bash
+------+-----+
| value|count|
+------+-----+
|Nathan|    1|
|Ngo   |    1|
+------+-----+
```

**Second batch:**
In **Netcat server** termnial:
```bash
nc -lk 9999
Nathan Ngo are learning
```

In structure streaming terminal:
```bash
+--------+-----+
| value  |count|
+--------+-----+
|Nathan  |    2|
|Ngo     |    2|
|are     |    1|
|learning|    1|
+--------+-----+
```

#### Comment
The key idea in `Structured Streaming` is to treat a live data stream as a table that is being `continuously appended`. This leads to a new stream processing model that is very similar to a batch processing model. You will express your streaming computation as standard batch-like query as on a static table, and Spark runs it as an `incremental` query on the unbounded input table. Let’s understand this model in more detail.

## Explaination of Structure streaming 
### Data Stream
Consider the input `data stream` as the `Input Table`. Every data item that is arriving on the stream is like a new row being appended to the `Input Table`.

![plot](https://spark.apache.org/docs/latest/img/structured-streaming-stream-as-a-table.png)

### Query result
A query on the input will generate the `Result Table`. Every trigger interval (say, every 1 second), new rows get appended to the Input Table, which eventually updates the Result Table. Whenever the result table gets updated, we would want to write the changed result rows to an external sink.

![plot](https://spark.apache.org/docs/latest/img/structured-streaming-model.png)

### Output mode
As in the example, we have:
```bash
query = wordCounts \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()
```
The `Output` is defined as what gets written out to the external storage. The output can be defined in a different mode:

- `Complete Mode` - The entire updated `Result Table` will be written to the external storage. It is up to the storage connector to decide how to handle writing of the entire table.

- `Append Mode` - Only the new rows appended in the `Result Table` since the last trigger will be written to the external storage. This is applicable only on the queries where existing rows in the `Result Table` are not expected to change.

- `Update Mode` - Only the rows that were updated in the `Result Table` since the last trigger will be written to the external storage (available since Spark 2.1.1). Note that this is different from the Complete Mode in that this mode only outputs the rows that have changed since the last trigger. If the query doesn’t contain aggregations, it will be equivalent to Append mode.

**Note:** that each mode is applicable on certain types of queries. Read [here](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#output-modes) for more understanding.

## Explaination of Structure streaming in Quickly example

The `line` DataFrame is `input table`.
```bash
lines = spark \
    .readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .load()
```

The `wordCounts` DataFrame is `result table`.
```bash
words = lines.select(
                    explode(split(lines.value, " ")).alias("word")
                    )

# Generate running word count
wordCounts = words.groupBy("word").count()
```

The `query` is static DataFrame.

However, when this query is started, Spark will continuously check for new data from the socket connection. If there is new data, Spark will run an “incremental” query that combines the previous running counts with the new data to compute updated counts.

```bash
query = wordCounts \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

query.awaitTermination()
```

**Over view**

![plot](https://spark.apache.org/docs/latest/img/structured-streaming-example-model.png)