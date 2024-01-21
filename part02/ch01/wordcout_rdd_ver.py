from pyspark import SparkContext, RDD
from pyspark.sql import SparkSession
import os

# mac os Spark default IP Setting error complete
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"

if __name__ == '__main__':
    ss: SparkSession = SparkSession.builder. \
        master("local"). \
        appName("wordCount RDD Ver"). \
        getOrCreate()

    sc: SparkContext = ss.sparkContext

    # local data
    text_file: RDD[str] = sc.textFile("data/words.txt")

    # transformation
    counts = text_file.flatMap(lambda line: line.split(" ")).\
        map(lambda word: (word,1)).\
        reduceByKey(lambda count1, count2 : count1 + count2)

    # action trigger
    output = counts.collect()

    for (word, count) in output:
        print(f"{word}:{count}")

