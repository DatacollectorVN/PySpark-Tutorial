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

If you run command `/usr/libexec/java_home`, might be you get error like `The operation couldn’t be completed. Unable to locate a Java Runtime`. The reason why is we didn't install via standard packages. Therefore, we must allow the System Java wrappers to find this jdk.
```bash
sudo ln -sfn /opt/homebrew/opt/openjdk@11/libexec/openjdk.jdk /Library/Java/JavaVirtualMachines/openjdk.jdk
```

*Note:* We use the jdk version 11 so the `openjdk@11`.

After run the above command, to find the java home, run:
```bash
/usr/libexec/java_home
```

*Expected output:* `/opt/homebrew/Cellar/openjdk@11/11.0.15/libexec/openjdk.jdk/Contents/Home`

For utility, we can set the variable JAVA_HOME by add the following environment variables to your .bash_profile or .zshrc.
```bash
export JAVA_HOME=$(/usr/libexec/java_home)
```

Run:
```bash
echo $JAVA_HOME
```

The output is the same to `/usr/libexec/java_home`.
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

Add the following environment variables to your .bash_profile or .zshrc.
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
# Setup requirements - Hadoop (optional)
## 1. Macos (M1)
*Update day*: May-04-2022
#### Step1: Install Hadoop
```bash
brew install hadoop
```
*Note:* If you are using `yarn`, it might be get error like `Error: Cannot install hadoop because conflicting formulae are installed. yarn: because both install 'yarn' binaries`. To resolve this error, unlink `brew` with `yarn`:
```bash
brew unlink yarn
```
*Remember*: you can link with yarn again when using `brew link yarn`.

#### Step2: Move into Hadoop location
```bash
cd /opt/homebrew/Cellar/hadoop/3.3.2/libexec/etc/hadoop
```
*Note:* Depends on you Hadoop version (in my case is `3.3.2`).

#### Step3: Edit `hadoop-env.sh`
Edit `hadoop-env.sh` at line 54 `export JAVA_HOME=<path_of_java_home>`.

*Example:*
```bash
export JAVA_HOME=/opt/homebrew/Cellar/openjdk@11/11.0.15/libexec/openjdk.jdk/Contents/Home
```
*Note:* Check path of Java Home by `/usr/libexec/java_home`.

#### Step4: Edit `core-site.xml`
Add the follwing configuration:
```bash
<configuration>
 <property>
  <name>fs.defaultFS</name>
  <value>hdfs://localhost:9000</value>
 </property>
</configuration>
```

#### Step5: Edit `hdfs-site.xml`
```bash
<configuration>
  <property>
    <name>dfs.replication</name>
    <value>1</value>
  </property>
</configuration>
```