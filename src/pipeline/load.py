import argparse
import logging
import os
import pathlib
import sys

from pyspark.sql import SparkSession


def main():
    logging.basicConfig(level=logging.INFO, format="(%(asctime)s) [%(levelname)s]: %(message)s.")
    logger = logging.getLogger()

    parser = argparse.ArgumentParser(description="Util to load cleaned dataset into PostgreSQL database with user-password authentication scheme.")

    parser.add_argument("-i", "--input", type=str, default=".data/dataframe_transformed", help="A path to the transformed dataset directory.")
    parser.add_argument("-m", "--memory", type=str, default="45G", help="Spark driver memory size.")
    parser.add_argument("-d", "--spark-db-driver-path", type=str, default="/opt/spark/jar/postgresql-42.5.3.jar", help="A file path to the '*.jar' file of PostgreSQL database driver for Apache Spark.")
    parser.add_argument("-U", "--db-url", type=str, help="Url to the target PostgreSQL database with JDBC format.")
    parser.add_argument("-u", "--user", type=str, help="User required for database authentication.")
    parser.add_argument("-p", "--password", type=str, help="Password required for database authentication.")

    args = parser.parse_args()

    spark_memory = os.getenv("SPARK_DRIVER_MEMORY") or args.memory
    spark_db_url = os.getenv("SPARK_PIPELINE_DB_URL") or args.db_url
    spark_user = os.getenv("SPARK_PIPELINE_DB_USER") or args.user
    spark_password = os.getenv("SPARK_PIPELINE_DB_PASSWORD") or args.password

    if spark_db_url is None \
            or spark_user is None \
            or spark_password is None:
        logger.error(f"Database URL or credentials are empty. " +
                     "Check `SPARK_PIPELINE_DB_URL`, `SPARK_PIPELINE_DB_USER`, `SPARK_PIPELINE_DB_PASSWORD` " +
                     "environment variables or set values as arguments")
        sys.exit(1)

    logger.info(f"Starting Spark session driver with `{spark_memory}` memory")

    spark = SparkSession.builder \
        .appName("Steam reviews dataset: Data loading")\
        .master("local[*]")\
        .config("spark.driver.extraClassPath", args.spark_db_driver_path)\
        .config("spark.driver.memory", spark_memory)\
        .config("spark.driver.maxResultSize", "0")\
        .config("spark.kryoserializer.buffer.max", "2000M")\
        .getOrCreate()

    if not pathlib.Path(args.input).is_dir():
        logger.error(f"A dataset directory path does not exist: `{args.input}`")
        sys.exit(1)

    df_games_input_path = args.input + "/games"
    df_games = spark.read.format("json").option("lines", "true").load(df_games_input_path)
    df_games_count = df_games.count()

    logger.info(f"Loading {df_games_count} `Games` entities to `{spark_db_url}`")

    df_games.write.format("jdbc")\
        .option("url", spark_db_url).option("driver", "org.postgresql.Driver")\
        .option("dbtable", 'public."GamesAppend"').option("user", spark_user)\
        .option("password", spark_password).mode("append").save()

    df_recommendations_input_path = args.input + "/recommendations"
    df_recommendations = spark.read.format("json").option("lines", "true").load(df_recommendations_input_path)
    df_recommendations_count = df_recommendations.count()

    logger.info(f"Loading {df_recommendations_count} `Recommendations` entities to `{spark_db_url}`")

    df_recommendations.write.format("jdbc")\
        .option("url", spark_db_url).option("driver", "org.postgresql.Driver")\
        .option("dbtable", 'public."RecommendationsAppend"').option("user", spark_user)\
        .option("password", spark_password).mode("append").save()

    df_users_input_path = args.input + "/users"
    df_users = spark.read.format("json").option("lines", "true").load(df_users_input_path)
    df_users_count = df_users.count()

    logger.info(f"Loading {df_users_count} `Users` entities to `{spark_db_url}`")

    df_users.write.format("jdbc")\
        .option("url", spark_db_url).option("driver", "org.postgresql.Driver")\
        .option("dbtable", 'public."UsersAppend"').option("user", spark_user)\
        .option("password", spark_password).mode("append").save()

    logger.info("Closing Spark Session")

    spark.stop()


if __name__ == "__main__":
    main()

