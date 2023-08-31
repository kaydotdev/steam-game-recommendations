import argparse
import logging
import pathlib
import sys

from pyspark.sql import SparkSession


def main():
    logging.basicConfig(level=logging.INFO, format="(%(asctime)s) [%(levelname)s]: %(message)s.")
    logger = logging.getLogger()

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

    if not pathlib.Path(args.input).is_file():
        logger.error(f"A datafile path does not exist: `{args.input}`")
        sys.exit(1)

    logger.info(f"Partitioning datafile `{args.input}` into `{args.partitions}` chunks")

    df = spark.read.format("json").option("lines", "true").load(args.input)
    df.coalesce(args.partitions).write.format("json").mode("overwrite").save(args.output)

    logger.info(f"Partitions successfully recorded into directory `{args.output}`")

    spark.stop()


if __name__ == "__main__":
    main()

