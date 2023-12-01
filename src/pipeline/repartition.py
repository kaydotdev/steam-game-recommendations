import argparse
import logging
import os
import pathlib
import sys

from pyspark.sql import SparkSession


def main():
    logging.basicConfig(
        level=logging.INFO, format="(%(asctime)s) [%(levelname)s]: %(message)s"
    )
    logger = logging.getLogger()

    parser = argparse.ArgumentParser(
        description=(
            "Util to repartition *.JSON lines file into chunks for faster processing."
        )
    )

    parser.add_argument(
        "-i",
        "--input",
        type=str,
        default=".data/raw.jl",
        help="A path to datafile to repartiton.",
    )
    parser.add_argument(
        "-o",
        "--output",
        type=str,
        default=".data/dataframe",
        help="A path to output repartitioned dataset.",
    )
    parser.add_argument(
        "-m", "--memory", type=str, default="45G", help="Spark driver memory size."
    )
    parser.add_argument(
        "-p",
        "--partitions",
        type=int,
        default=48,
        help="Number of partitions in the output.",
    )

    args = parser.parse_args()

    spark_memory = os.getenv("SPARK_DRIVER_MEMORY") or args.memory

    logger.info(f"Starting Spark session driver with `{spark_memory}` memory")

    spark = (
        SparkSession.builder.appName("Steam reviews dataset: Repartition script")
        .master("local[*]")
        .config("spark.driver.memory", spark_memory)
        .config("spark.driver.maxResultSize", "0")
        .config("spark.kryoserializer.buffer.max", "2000M")
        .getOrCreate()
    )

    if not pathlib.Path(args.input).is_file():
        logger.error(f"A datafile path does not exist: `{args.input}`")
        sys.exit(1)

    logger.info(
        f"Splitting datafile `{args.input}` into `{args.partitions}` partitions"
    )

    df = spark.read.format("json").option("lines", "true").load(args.input)
    df.coalesce(args.partitions).write.format("json").option(
        "ignoreNullFields", False
    ).mode("overwrite").save(args.output)

    logger.info(f"Partitions successfully recorded into directory `{args.output}`")
    logger.info("Closing Spark Session")

    spark.stop()


if __name__ == "__main__":
    main()
