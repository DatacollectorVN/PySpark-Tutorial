# PySpark-Tutorial
My self-learning about PySpark

## log4j.properties file
The `log4j.properties` file is a `log4j configuration `file which stores properties in `key-value pairs`. The `log4j properties` file contains the entire runtime configuration used by `log4j`. This file will contain log4j appenders information, log level information and output file names for file appenders.

Read more [here](https://www.javatpoint.com/log4j-properties)

## log4j.properties file configure loging in Apache Spark
`Spark` uses `log4j` for logging. You can configure it by adding a `log4j.properties` file in the conf directory. One way to start is to copy the existing `log4j.properties.template` located there.

You can see the default sample `log4j` in `log4j.properties`.

## Add `log4j.properties` in `spark-defaults.conf`
This spark application will look for the `log4j.properties` file in the current directory.

`spark-default.conf` is is the default properties file with the Spark properties of your Spark applications.
Read [here](https://spark.apache.org/docs/latest/configuration.html) for more understanding.
### In Macos (M1)
Open `spark-defaults.conf` in path `/opt/homebrew/Cellar/apache-spark/3.2.1/libexec/conf/` and add the command below:
```bash
spark.driver.extraJavaOptions      -Dlog4j.configuration=file:log4j.properties
``` 
