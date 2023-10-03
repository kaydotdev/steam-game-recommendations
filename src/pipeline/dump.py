import argparse
import glob
import logging
import os
import shutil
import sys

from pyspark.ml.feature import StringIndexer
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, monotonically_increasing_id
from pyspark.sql.types import DoubleType, IntegerType

from pipeline.udf.game import (
    description_udf,
    deserialize_tags_udf,
    replace_escape_chr_udf,
)


def dump_table(df: DataFrame, output_dir: str, entity_name: str, fmt="csv"):
    """
    Dump the Dataframe content into a single file and clean all Spark artifacts.
    If Spark fails to write content into the filesystem, an IOError will be raised.

    Args:
        df (DataFrame): Table entity DataFrame to be dumped into the filesystem.
        output_dir (str): The directory where the output file will be stored.
        entity_name (str): The name to be given to the output file.
        fmt (str, optional): The format in which the file will be saved. Defaults to "csv".

    Raises:
        IOError: If the data file does not exist in the temporary directory after saving.
    """

    df_output_dir = os.path.join(output_dir, entity_name)
    df.coalesce(1).write.format(fmt).mode("overwrite").option("header","true").save(df_output_dir)
    df_file = glob.glob(os.path.join(df_output_dir, f"*.{fmt}"))

    if df_file:
        shutil.move(df_file[0], os.path.join(output_dir, f"{entity_name}.{fmt}"))
        shutil.rmtree(df_output_dir)
    else:
        msg = f"Table data file does not exist in the directory '{df_output_dir}'"
        raise OSError(msg)


def main():
    logging.basicConfig(level=logging.INFO, format="(%(asctime)s) [%(levelname)s]: %(message)s")
    logger = logging.getLogger()

    parser = argparse.ArgumentParser(description="Util to dump collected data from a PostgreSQL tables into dedicated CSV files.")

    parser.add_argument("-o", "--output", type=str, default=".data/dump", help="A path to the dump files directory.")
    parser.add_argument("-m", "--memory", type=str, default="45G", help="Spark driver memory size.")
    parser.add_argument("-d", "--spark-db-driver-path", type=str, default="/opt/spark/jar/postgresql-42.5.3.jar", help="A file path to the '*.jar' file of PostgreSQL database driver for Apache Spark.")
    parser.add_argument("-H", "--hostname", type=str, help="Database server address e.g., localhost or an IP address.")
    parser.add_argument("-n", "--db-name", type=str, help="The name of the PostgreSQL database.")
    parser.add_argument("-u", "--user", type=str, help="The username used to authenticate.")
    parser.add_argument("-p", "--password", type=str, help="Password used to authenticate.")

    args = parser.parse_args()

    spark_memory = os.getenv("SPARK_DRIVER_MEMORY") or args.memory

    spark_hostname = os.getenv("SPARK_PIPELINE_DB_HOSTNAME") or args.hostname
    spark_db_name = os.getenv("SPARK_PIPELINE_DB_NAME") or args.db_name
    spark_user = os.getenv("SPARK_PIPELINE_DB_USER") or args.user
    spark_password = os.getenv("SPARK_PIPELINE_DB_PASSWORD") or args.password

    if spark_hostname is None \
            or spark_db_name is None \
            or spark_user is None \
            or spark_password is None:
        logger.error("Database hostname, database name or credentials are empty. " +
                     "Check `SPARK_PIPELINE_DB_HOSTNAME`, `SPARK_PIPELINE_DB_NAME`, " +
                     "`SPARK_PIPELINE_DB_USER`, `SPARK_PIPELINE_DB_PASSWORD` " +
                     "environment variables or set values as arguments")
        sys.exit(1)

    logger.info(f"Starting Spark session driver with `{spark_memory}` memory")

    spark_credentials = {"user": spark_user, "password": spark_password, "driver":"org.postgresql.Driver"}
    spark_jdbc_url = f"jdbc:postgresql://{spark_hostname}:5432/{spark_db_name}"
    spark = SparkSession.builder \
        .appName("Steam reviews dataset: Dumping records from DB")\
        .master("local[*]")\
        .config("spark.driver.extraClassPath", args.spark_db_driver_path)\
        .config("spark.driver.memory", spark_memory)\
        .config("spark.driver.maxResultSize", "0")\
        .config("spark.kryoserializer.buffer.max", "2000M")\
        .getOrCreate()

    logger.info("Dumping table 'Games'")

    df_games = spark.read.jdbc(spark_jdbc_url, 'public."Games"', properties=spark_credentials)
    df_games_table = df_games.select(
        col("app_id"), replace_escape_chr_udf(col("title")).alias("title"),
        col("date_release"), col("win"), col("mac"), col("linux"),
        col("rating"), col("positive_ratio"), col("user_reviews"),
        col("price_final").cast(DoubleType()).alias("price_final"),
        col("price_original").cast(DoubleType()).alias("price_original"),
        col("discount").cast(DoubleType()).alias("discount"),
        col("steam_deck")
    )

    try:
        dump_table(df_games_table, args.output, "games")
    except OSError as e:
        logger.error(f"Failed to dump 'Games' entity: '{e}'")
        sys.exit(1)

    logger.info("Dumping metadata for table 'Games'")

    df_games_metadata = df_games.select(
        col("app_id"), description_udf(col("description")).alias("description"),
        deserialize_tags_udf(col("tags")).alias("tags")
    )

    try:
        dump_table(df_games_metadata, args.output, "games_metadata", fmt="json")
    except OSError as e:
        logger.error(f"Failed to dump metadata for 'Games' entity: '{e}'")
        sys.exit(1)

    logger.info("Dumping table 'Users'")

    df_users = spark.read.jdbc(spark_jdbc_url, 'public."Users"', properties=spark_credentials).alias("df_users")
    df_user_reviews = spark.read.format("jdbc").option("url", spark_jdbc_url)\
        .option("query", 'SELECT "Recommendations".userprofile, COUNT(*) AS reviews FROM "Recommendations" GROUP BY "Recommendations".userprofile')\
        .option("user", spark_user).option("password", spark_password).load().alias("df_user_reviews")

    df_user_with_reviews = df_users.join(df_user_reviews, 
        col("df_users.userprofile") == col("df_user_reviews.userprofile"), 
    "left").select(
        col("df_users.userprofile").alias("userprofile"),
        col("products"),
        col("reviews")
    ).na.fill(value=0, subset=["reviews"])

    stIndexer = StringIndexer(inputCol="userprofile", outputCol="user_id", handleInvalid="error")

    profileIndexer = stIndexer.fit(df_user_with_reviews)
    df_user_with_reviews = profileIndexer.transform(df_user_with_reviews)

    df_users = df_user_with_reviews.select(
        col("user_id").cast(IntegerType()).alias("user_id"),
        col("products"),
        col("reviews")
    )

    try:
        dump_table(df_users, args.output, "users")
    except OSError as e:
        logger.error(f"Failed to dump 'Users' entity: '{e}'")
        sys.exit(1)

    logger.info("Dumping table 'Recommendations'")

    df_recommendations = spark.read.jdbc(spark_jdbc_url, 'public."Recommendations"', properties=spark_credentials)
    df_recommendations = profileIndexer.transform(df_recommendations).withColumn("review_id", monotonically_increasing_id())
    df_recommendations = df_recommendations.select(
        col("app_id"), col("helpful"), col("funny"), col("date"),
        col("is_recommended"), col("hours").cast(DoubleType()).alias("hours"),
        col("user_id").cast(IntegerType()).alias("user_id"), col("review_id")
    )

    try:
        dump_table(df_recommendations, args.output, "recommendations")
    except OSError as e:
        logger.error(f"Failed to dump 'Recommendations' entity: '{e}'")
        sys.exit(1)

    logger.info("Closing Spark Session")

    spark.stop()


if __name__ == "__main__":
    main()

