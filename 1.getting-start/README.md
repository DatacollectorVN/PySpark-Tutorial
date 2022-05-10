# PySpark-Tutorial
My self-learning about PySpark

# Run PySpark with Miniconda environment
After you setup *Apache-Spark*, following our tutorial in `setup`.
### 1. Create Miniconda enviroment and install PySpark
```bash
conda create -n pyspark python=3.8 -y
conda activate pyspark
pip install -r requirements.txt
```

### 2. Getting start Pyspark
```bash
cd 1.getting-start
python 1.initalize_spark.py
```

If you run if successfully, mean your setup is success.

### 3. Lecture about PySpark
Read my own [documnet](https://docs.google.com/document/d/1sJwxNw-bUN8SlsU8yFtkYG7kiuNYgUXy/edit?usp=sharing&ouid=103057077167517333764&rtpof=true&sd=true), it clundes some parts:
- 1. Introduction to Big Data.
- 2. Common terminologies in Big Data.
- 3. Apache Hadoop.
- 4. Apache Spark.
- 5. Compare Apache Spark and Hadoop.