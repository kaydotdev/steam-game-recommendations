import argparse
import logging
import os
import pathlib
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col


def main():
    logging.basicConfig(
        level=logging.INFO, format="(%(asctime)s) [%(levelname)s]: %(message)s"
    )
    logger = logging.getLogger()

    parser = argparse.ArgumentParser(
        description=(
            "Util to split a repartitioned dataset into separated entities: games,"
            " pages and reviews."
        )
    )

    parser.add_argument(
        "-i",
        "--input",
        type=str,
        default=".data/dataframe",
        help="A path to the repartitioned dataset directory.",
    )
    parser.add_argument(
        "-o",
        "--output",
        type=str,
        default=".data/dataframe_entity",
        help="A path to the separated entities dataset directory.",
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
        SparkSession.builder.appName("Steam reviews dataset: Entity spliting script")
        .master("local[*]")
        .config("spark.driver.memory", spark_memory)
        .config("spark.driver.maxResultSize", "0")
        .config("spark.kryoserializer.buffer.max", "2000M")
        .getOrCreate()
    )

    if not pathlib.Path(args.input).is_dir():
        logger.error(f"A dataset directory path does not exist: `{args.input}`")
        sys.exit(1)

    logger.info(
        f"Splitting dataset at `{args.input}` by entity type into `{args.partitions}`"
        " partitions per entity"
    )

    df = spark.read.format("json").option("lines", "true").load(args.input)
    df_count = df.count()

    logger.info(f"Collected samples in the dataset: {df_count}")

    logger.info("Processing `GAME` entity")

    df_game_path = args.output + "/game"
    df_game = df.filter(col("type") == "game").select(
        col("content.app_id").alias("app_id"),
        col("content.title").alias("title"),
        col("content.platforms").alias("platforms"),
        col("content.date_release").alias("date_release"),
        col("content.summary").alias("summary"),
        col("content.price_final").alias("price_final"),
        col("content.price_raw").alias("price_raw"),
        col("content.price_discount_raw").alias("price_discount_raw"),
        col("content.steam_deck_compatible").alias("steam_deck_compatible"),
    )
    df_game_count = df_game.count()
    df_game.coalesce(args.partitions).write.format("json").option(
        "ignoreNullFields", False
    ).mode("overwrite").save(df_game_path)

    logger.info(
        f"{df_game_count} `GAME` entities recorded into directory `{df_game_path}`"
    )

    logger.info("Processing `PAGE` entity")

    df_page_path = args.output + "/page"
    df_page = df.filter(col("type") == "page").select(
        col("content.app_id").alias("app_id"),
        col("content.description").alias("description"),
        col("content.tags").alias("tags"),
    )
    df_page_count = df_page.count()
    df_page.coalesce(args.partitions).write.format("json").option(
        "ignoreNullFields", False
    ).mode("overwrite").save(df_page_path)

    logger.info(
        f"{df_page_count} `PAGE` entities recorded into directory `{df_page_path}`"
    )

    logger.info("Processing `REVIEW` entity")

    df_review_path = args.output + "/review"
    df_review = df.filter(col("type") == "review").select(
        col("content.app_id").alias("app_id"),
        col("content.found_helpful").alias("found_helpful"),
        col("content.title").alias("title"),
        col("content.hours").alias("hours"),
        col("content.content").alias("content"),
        col("content.products_in_account").alias("products_in_account"),
        col("content.miniprofile").alias("miniprofile"),
    )
    df_review_count = df_review.count()
    df_review.coalesce(args.partitions).write.format("json").option(
        "ignoreNullFields", False
    ).mode("overwrite").save(df_review_path)

    logger.info(
        f"{df_review_count} `REVIEW` entities recorded into directory"
        f" `{df_review_path}`"
    )

    df_combined_count = df_page_count + df_game_count + df_review_count
    df_variables = {
        "Pages": df_page_count,
        "Games": df_game_count,
        "Reviews": df_review_count,
        "Combined": df_combined_count,
        "Total": df_count,
    }

    logger.info("+----------+-------+")
    logger.info("|  Entity  | Count |")
    logger.info("+----------+-------+")

    for entity, count in df_variables.items():
        logger.info(f"| {entity:<9}| {count:<5} |")

    logger.info("+----------+-------+")
    logger.info("Closing Spark Session")

    spark.stop()


if __name__ == "__main__":
    main()
