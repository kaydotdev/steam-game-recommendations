import argparse
import logging
import os
import pathlib
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import array, col

from pipeline.udf.common import (
    clean_df,
    joined_strip_list_udf,
    normalize_date_udf,
    strip_udf,
    to_int_udf,
)
from pipeline.udf.game import (
    calculate_discount_udf,
    is_linux_udf,
    is_mac_udf,
    is_win_udf,
    original_price_udf,
    positive_ratio_udf,
    pricing_udf,
    rating_udf,
    reviews_number_udf,
    steam_deck_udf,
    strip_title_udf,
)
from pipeline.udf.review import (
    date_review_udf,
    found_funny_udf,
    found_helpful_udf,
    hours_udf,
    is_recommended_udf,
    products_udf,
)


def main():
    logging.basicConfig(
        level=logging.INFO, format="(%(asctime)s) [%(levelname)s]: %(message)s"
    )
    logger = logging.getLogger()

    parser = argparse.ArgumentParser(
        description="Util to clean and transform prepared dataset."
    )

    parser.add_argument(
        "-i",
        "--input",
        type=str,
        default=".data/dataframe_entity",
        help="A path to the prepared dataset directory.",
    )
    parser.add_argument(
        "-o",
        "--output",
        type=str,
        default=".data/dataframe_transformed",
        help="A path to the transformed dataset directory.",
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
        SparkSession.builder.appName("Steam reviews dataset: Data transformation")
        .master("local[*]")
        .config("spark.driver.memory", spark_memory)
        .config("spark.driver.maxResultSize", "0")
        .config("spark.kryoserializer.buffer.max", "2000M")
        .getOrCreate()
    )

    if not pathlib.Path(args.input).is_dir():
        logger.error(
            f"A prepared dataset directory path does not exist: `{args.input}`"
        )
        sys.exit(1)

    # Processing `Games` table
    df_game_input_path = args.input + "/game"
    df_page_input_path = args.input + "/page"

    logger.info(f"Cleaning `GAME` entity dataset at `{df_game_input_path}`")

    df_games = (
        spark.read.format("json").option("lines", "true").load(df_game_input_path)
    )
    df_games_cleaned = (
        clean_df(df_games, ["app_id", "date_release", "summary", "title"])
        .select(
            to_int_udf(col("app_id")).alias("app_id"),
            strip_title_udf(col("title")).alias("title"),
            normalize_date_udf(col("date_release")).alias("date_release"),
            rating_udf(col("summary")).alias("rating"),
            positive_ratio_udf(col("summary")).alias("positive_ratio"),
            reviews_number_udf(col("summary")).alias("user_reviews"),
            pricing_udf(col("price_final")).alias("price_final"),
            original_price_udf(col("price_raw")).alias("price_original"),
            is_win_udf(col("platforms")).alias("win"),
            is_mac_udf(col("platforms")).alias("mac"),
            is_linux_udf(col("platforms")).alias("linux"),
            steam_deck_udf(col("steam_deck_compatible")).alias("steam_deck"),
        )
        .withColumn(
            "discount",
            calculate_discount_udf(array(col("price_final"), col("price_original"))),
        )
    )

    logger.info(f"Cleaning `PAGE` entity dataset at `{df_page_input_path}`")

    df_pages = (
        spark.read.format("json").option("lines", "true").load(df_page_input_path)
    )
    df_page_cleaned = clean_df(df_pages, ["app_id"]).select(
        to_int_udf(col("app_id")).alias("app_id"),
        strip_udf(col("description")).alias("description"),
        joined_strip_list_udf(col("tags")).alias("tags"),
    )

    df_game_joined = clean_df(
        df_games_cleaned.alias("df_games")
        .join(
            df_page_cleaned.alias("df_page"),
            col("df_games.app_id") == col("df_page.app_id"),
            "left",
        )
        .select(
            col("df_games.app_id").alias("app_id"),
            col("title").alias("title"),
            col("description").alias("description"),
            col("tags").alias("tags"),
            col("date_release").alias("date_release"),
            col("rating").alias("rating"),
            col("positive_ratio").alias("positive_ratio"),
            col("user_reviews").alias("user_reviews"),
            col("price_final").alias("price_final"),
            col("price_original").alias("price_original"),
            col("discount").alias("discount"),
            col("win").alias("win"),
            col("mac").alias("mac"),
            col("linux").alias("linux"),
            col("steam_deck").alias("steam_deck"),
        ),
        ["app_id"],
    ).dropna(subset=("app_id", "date_release"))

    df_game_count = df_game_joined.count()
    df_game_output_path = args.output + "/games"
    df_game_joined.coalesce(args.partitions).write.format("json").option(
        "ignoreNullFields", False
    ).mode("overwrite").save(df_game_output_path)

    logger.info(
        f"{df_game_count} `Games` entities recorded into directory"
        f" `{df_game_output_path}`"
    )

    # Processing `Recommendations` table
    df_review_input_path = args.input + "/review"

    logger.info(f"Cleaning `REVIEW` entity dataset at `{df_review_input_path}`")

    df_review = (
        spark.read.format("json").option("lines", "true").load(df_review_input_path)
    )
    df_review_cleaned = (
        clean_df(
            df_review,
            [
                "app_id",
                "found_helpful",
                "content",
                "title",
                "hours",
                "products_in_account",
                "miniprofile",
            ],
        )
        .select(
            to_int_udf(col("app_id")).alias("app_id"),
            is_recommended_udf(col("title")).alias("is_recommended"),
            found_helpful_udf(col("found_helpful")).alias("helpful"),
            found_funny_udf(col("found_helpful")).alias("funny"),
            date_review_udf(col("content")).alias("date"),
            hours_udf(col("hours")).alias("hours"),
            products_udf(col("products_in_account")).alias("products"),
            to_int_udf(col("miniprofile")).alias("userprofile"),
        )
        .filter((col("funny") >= 0) & (col("helpful") >= 0) & (col("date").isNotNull()))
    )

    df_recommendations = df_review_cleaned.select(
        col("app_id"),
        col("userprofile"),
        col("date"),
        col("is_recommended"),
        col("helpful"),
        col("funny"),
        col("hours"),
    )
    df_recommendations_count = df_recommendations.count()
    df_recommendations_output_path = args.output + "/recommendations"
    df_recommendations.coalesce(args.partitions).write.format("json").option(
        "ignoreNullFields", False
    ).mode("overwrite").save(df_recommendations_output_path)

    logger.info(
        f"{df_recommendations_count} `Recommendations` entities recorded into directory"
        f" `{df_recommendations_output_path}`"
    )

    # Processing `Users` table stage
    df_users = clean_df(
        df_review_cleaned.select(col("userprofile"), col("products")),
        ["userprofile", "products"],
    )
    df_users_count = df_users.count()
    df_users_output_path = args.output + "/users"
    df_users.coalesce(args.partitions).write.format("json").option(
        "ignoreNullFields", False
    ).mode("overwrite").save(df_users_output_path)

    logger.info(
        f"{df_users_count} `Users` entities recorded into directory"
        f" `{df_users_output_path}`"
    )
    logger.info("Closing Spark Session")

    spark.stop()


if __name__ == "__main__":
    main()
