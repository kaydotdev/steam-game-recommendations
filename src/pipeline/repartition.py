import argparse

from pyspark.sql import SparkSession

parser = argparse.ArgumentParser(description="Util to repartition *.JSON lines file into chunks for faster processing.")

parser.add_argument("--input", type=str, default="../.data/raw.json", help="A path to datafile to repartiton.")
parser.add_argument("--output", type=str, default="../.data/dataframe", help="A path to output repartitioned dataset.")
parser.add_argument("--partitions", type=int, default=48, help="Number of partitions in the output.")

args = parser.parse_args()

spark = SparkSession.builder \
    .appName("Steam reviews dataset: Repartition script")\
    .master("local[*]")\
    .config("spark.driver.memory","45G")\
    .config("spark.driver.maxResultSize", "0")\
    .config("spark.kryoserializer.buffer.max", "2000M")\
    .getOrCreate()


df = spark.read.format("json").option("lines", "true").load(args.input)
df.coalesce(args.partitions).write.format("json").mode("overwrite").save(args.output)

spark.stop()

