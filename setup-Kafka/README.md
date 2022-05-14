# Setup requirements - Kafka
## 1. Macos (M1)
*Update day*: May-14-2022.

*Source:* [here](https://medium.com/@taapasagrawal/installing-and-running-apache-kafka-on-macos-with-m1-processor-5238dda81d51).

#### Step1: Setup requirements
Follow our intruction of installing Java in `setup-PySpark`.

### Step2: Install Kafka
```bash
brew install kafka
```

### Step3: Link Java path in `Kafka`.
If you follow our instruction of installing Java, you need to link Java path to `Kafka`.
```bash
brew link openjdk --force
```

Open `.zshrc` and add openjdk path.

`export PATH="/opt/homebrew/opt/openjdk/bin:$PATH"`

## Verify installing
### Step4: Run Zookeeper and Kafka services 
*First terminal:* Start Zookeeper.
```bash
zookeeper-server-start /opt/homebrew/etc/kafka/zookeeper.properties
```

*Second terminal:* Start Kafka.
```bash
kafka-server-start /opt/homebrew/etc/kafka/server.properties
```

### Step5: Create topic in `Apache Kafka`
*Third terminal:* Create topic with name `foobar`. 
```bash
kafka-topics --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic foobar
```

*Note:* Notice here we aren’t using `--zookeeper` flag as in standard documentations because `Kafka` versions for new MacOS do not support/ require that flag anymore.

### Step5: Initialize `producer console`
*Third terminal:* This console initalizes `producer console` for `foobar` topic with port `9092`.
```bash
kafka-console-producer --broker-list localhost:9092 --topic foobar
> Nathan
> Ngo
```

### Step6: Initialize `consumer console`
*Four terminal:* This console initialize `consumer console`. This will listen to the bootstrap server at port `9092` at topic `foobar`.
```bash
kafka-console-consumer --bootstrap-server localhost:9092 --topic foobar --from-beginning
```

*Expected outputs:*
```bash
Nathan 
Ngo
```

If you can see the outputs kafka is set up and running neatly on you M1 system.