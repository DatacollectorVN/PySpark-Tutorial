# Setup requirements - Hadoop (optional)
## 1. Macos (M1)
*Update day*: May-04-2022.

*Source:* [here](https://codewitharjun.medium.com/install-hadoop-on-macos-efe7c860c3ed).
#### Step1: Install Hadoop
```bash
brew install hadoop
```
*Note:* If you are using `yarn` from `NodeJS`, it might be get error like `Error: Cannot install hadoop because conflicting formulae are installed. yarn: because both install 'yarn' binaries`. To resolve this error, unlink `brew` with `yarn`:
```bash
brew unlink yarn
```
*Remember*: you can link with yarn again when using `brew link yarn`. If you want to use `yarn` from `NodeJS`, you can unlink with hadoop like `brew unlink hadoop` or alias `yarn` by `yarnpkg`.

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

#### Step6: Edit `mapred-site.xml`
```bash
<configuration>
    <property>
       <name>mapreduce.framework.name</name>
       <value>yarn</value>
    </property>
    <property>
    <name>mapreduce.application.classpath</name>   
  <value>
$HADOOP_MAPRED_HOME/share/hadoop/mapreduce/*:$HADOOP_MAPRED_HOME/share/hadoop/mapreduce/lib/*
  </value>
    </property>
</configuration>
```

#### Step7: Edit `yarn-site.xml`
```bash
<configuration>
  <property>
    <name>yarn.nodemanager.aux-services</name>
    <value>mapreduce_shuffle</value>
  </property>
  <property>
    <name>yarn.nodemanager.env-whitelist</name>  
   <value>
JAVA_HOME,HADOOP_COMMON_HOME,HADOOP_HDFS_HOME,HADOOP_CONF_DIR,CLASSPATH_PREPEND_DISTCACHE,HADOOP_YARN_HOME,HADOOP_MAPRED_HOME
  </value>
  </property>
</configuration>
```

*Note:* Steps from Setp3 to Step6 are configurations, you can change it later.

#### Step8: Turn on `Remote Login` in Macos
`System Preference` --> `Sharing` --> tick `Remote Login`.

#### Step9: Run Hadoop
But before doing that just type following command on terminal:
```bash
hadoop namenode -format 
```

##### Start Hadoop
```bash
start-all.sh
```
You can get the permission denied like `localhost: nathanngo@localhost: Permission denied (publickey,password,keyboard-interactive)`.

Stop Hadoop then resolve this problem:
```bash
stop-all.sh
```

Please follow [here](https://stackoverflow.com/questions/22842743/how-to-set-java-home-environment-variable-on-mac-os-x-10-9) to resolve this problem, our steps:

*Generate new keygen:*
```bash
ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa
```

*Register new keygen:*
```bash
cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
```

Then start Hadoop again, you won't see permission denied.

See all the parts of Hadoop have been installed and running:
```bash
jps
```

Access Hadoop via port `9870`, type in web brownser `http://localhost:9870`

# Getting start with Hadoop
You can read he Hadoop HDFS Command Cheatsheet [here](https://images.linoxide.com/hadoop-hdfs-commands-cheatsheet.pdf).

### 1. Create directory in Hadoop
```bash
hdfs dfs -mkdir /user
hdfs dfs -mkdir /user/NathanNgo
```
You can see this via Hadoop web brownser in `http://localhost:9870`.

### 2. Upload file to HDFS
```bash
hdfs dfs -put src-data/5000_points.txt /user/NathanNgo
```
We can load this HDFS file by path `hdfs://localhost:9000//user/NathanNgo/5000_points.txt`. See in `1.getting-start/2.RDD.ipynb`.
*Note:* You must to start Hadoop `start-all.sh` before load this HDFS file.
