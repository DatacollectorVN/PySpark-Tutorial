# PySpark-Tutorial
My self-learning about PySpark
# Setup requirements
## 1. Macos (M1)
*Update day*: May-02-2022

*Source:* [here](https://sparkbyexamples.com/pyspark/how-to-install-pyspark-on-mac/).

#### Prerequirements:
- Install [HomeBrew](https://brew.sh).
- Install [Miniconda](https://docs.conda.io/en/latest/miniconda.html).

#### Install Java
PySpark uses Java underlying hence you need to have Java on your Mac. Since Java is a third party, you can install it using the Homebrew command brew. *Since Oracle Java is not open source anymore,* I am using the OpenJDK version 11. Run the below command in the terminal to install it.
```bash
brew install openjdk@11
```

#### Install Scala
Since Spark is written in Scala language it is obvious you would need Scala to run Spark programs.
```bash
brew install scala
```

#### Install PySpark on Mac
PySpark is a Spark library written in Python to run Python applications using Apache Spark capabilities. Spark was basically written in Scala and later on due to its industry adaptation its API PySpark was released for Python using Py4J. Py4J is a Java library that is integrated within PySpark and allows python to dynamically interface with JVM objects, hence to run PySpark you also need Java to be installed along with Python, and Apache Spark. So to use PySpark, let’s install PySpark on Mac.
```bash
brew install apache-spark
```

#### Add environment variables
Check the environmen path:
```bash
ls /opt/homebrew/Cellar/apache-spark/<version>/libexec/
```

Add the following environment variables to your .bash_profile or .zshrc
```bash
export SPARK_HOME=/opt/homebrew/Cellar/apache-spark/<version>/libexec/           
export PATH="$SPARK_HOME/bin/:$PATH"
```

### Review binaries permissions
For some reason, some installations are not give execution permission to binaries. Review it and give them if necessary.
```bash
chmod +x /opt/homebrew/Cellar/apache-spark/<version>/libexec/bin/*
```
### Start Spark with Python environment
```bash
pyspark
```
*Note:* `Using Python version 3.8.13` depends on your miniconda environment. 

### Start Spark with Scala environment
```bash
spark-shell
```
