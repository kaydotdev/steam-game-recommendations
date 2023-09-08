import argparse
import logging
import os
import pathlib
import sys

import psycopg
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    BooleanType,
    DateType,
    DecimalType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)


def main():
    logging.basicConfig(level=logging.INFO, format="(%(asctime)s) [%(levelname)s]: %(message)s")
    logger = logging.getLogger()

    parser = argparse.ArgumentParser(description="Util to load cleaned dataset into PostgreSQL database with user-password authentication scheme.")

    parser.add_argument("-i", "--input", type=str, default=".data/dataframe_transformed", help="A path to the transformed dataset directory.")
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

    spark_jdbc_url = f"jdbc:postgresql://{spark_hostname}:5432/{spark_db_name}"
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

    games_schema = StructType([
        StructField("app_id", IntegerType(), False),
        StructField("title", StringType(), False),
        StructField("description", StringType(), True),
        StructField("steam_deck", BooleanType(), False),
        StructField("date_release", DateType(), False),
        StructField("win", BooleanType(), False),
        StructField("mac", BooleanType(), False),
        StructField("linux", BooleanType(), False),
        StructField("rating", StringType(), False),
        StructField("positive_ratio", IntegerType(), False),
        StructField("user_reviews", IntegerType(), False),
        StructField("price_final", DecimalType(), False),
        StructField("price_original", DecimalType(), False),
        StructField("discount", DecimalType(), False),
        StructField("tags", StringType(), True)
    ])

    df_games_input_path = args.input + "/games"
    df_games = spark.read.format("json").option("lines", "true")\
                        .schema(games_schema).load(df_games_input_path)
    df_games_count = df_games.count()

    logger.info(f"Loading {df_games_count} `Games` entities to `{spark_jdbc_url}`")

    df_games.write.format("jdbc")\
        .option("url", spark_jdbc_url).option("driver", "org.postgresql.Driver")\
        .option("dbtable", 'public."GamesAppend"').option("user", spark_user)\
        .option("password", spark_password).mode("append").save()

    recommendations_schema = StructType([
        StructField("app_id", IntegerType(), False),
        StructField("userprofile", LongType(), False),
        StructField("date", DateType(), False),
        StructField("is_recommended", BooleanType(), False),
        StructField("helpful", IntegerType(), False),
        StructField("funny", IntegerType(), False),
        StructField("hours", DecimalType(), False)
    ])

    df_recommendations_input_path = args.input + "/recommendations"
    df_recommendations = spark.read.format("json").option("lines", "true")\
                                .schema(recommendations_schema).load(df_recommendations_input_path)
    df_recommendations_count = df_recommendations.count()

    logger.info(f"Loading {df_recommendations_count} `Recommendations` entities to `{spark_jdbc_url}`")

    df_recommendations.write.format("jdbc")\
        .option("url", spark_jdbc_url).option("driver", "org.postgresql.Driver")\
        .option("dbtable", 'public."RecommendationsAppend"').option("user", spark_user)\
        .option("password", spark_password).mode("append").save()

    users_schema = StructType([
        StructField("userprofile", LongType(), False),
        StructField("products", IntegerType(), False)
    ])

    df_users_input_path = args.input + "/users"
    df_users = spark.read.format("json").option("lines", "true")\
                                    .schema(users_schema).load(df_users_input_path)
    df_users_count = df_users.count()

    logger.info(f"Loading {df_users_count} `Users` entities to `{spark_jdbc_url}`")

    df_users.write.format("jdbc")\
        .option("url", spark_jdbc_url).option("driver", "org.postgresql.Driver")\
        .option("dbtable", 'public."UsersAppend"').option("user", spark_user)\
        .option("password", spark_password).mode("append").save()

    logger.info("Closing Spark Session")

    spark.stop()

    # Syncing data from `append` tables into the `master` tables.
    # Here `master` tables represents the final source of truth, containing all collected data from the previous iterations.
    # Unlike `master` tables, `append` tables will be deleted after each iteration.
    with psycopg.connect(host=spark_hostname, dbname=spark_db_name, user=spark_user, password=spark_password) as conn:
        script_path_dir = os.path.dirname(os.path.abspath(__file__))
        cur = conn.cursor()

        logger.info("Ensuring `master` tables exist, otherwise create them")

        with conn.transaction():
            create_script_path = os.path.join(script_path_dir, "sql", "create.sql")

            with open(create_script_path) as file:
                cur.execute(file.read())

        logger.info("Loading to the `master` tables and deleting `append` tables")

        with conn.transaction():
            append_script_path = os.path.join(script_path_dir, "sql", "append.sql")

            with open(append_script_path) as file:
                cur.execute(file.read())

        logger.info(f"Rows inserted into all `master` tables: {cur.rowcount}")


if __name__ == "__main__":
    main()

