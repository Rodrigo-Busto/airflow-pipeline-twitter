from os.path import join
import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as f

def get_tweets_data(df):
    fields = [
        "tweet.author_id",
        "tweet.conversation_id",
        "tweet.created_at",
        "tweet.id",
        "tweet.in_reply_to_user_id",
        "tweet.text",
        "tweet.public_metrics.*"
        ]

    return df.select(f.explode("data").alias("tweet"))\
        .select(fields)

def get_users_data(df):
    return df.select(f.explode("includes.users"))\
        .select("col.*")

def get_meta_data(df):
    return df.select("meta.*")

def export_json(df, dest):
    df.coalesce(1).write.mode("overwrite").json(dest)

def twitter_transform(spark, src, base_dest, process_date):
    df = spark.read.json(src)

    tweets_df = get_tweets_data(df)
    users_df = get_users_data(df)

    table_dest = join(base_dest, "{table_name}", f"process_date={process_date}")

    export_json(tweets_df, table_dest.format(table_name = "tweets"))
    export_json(users_df, table_dest.format(table_name = "users"))

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Spark Tweeter Transformation"
    )

    parser.add_argument("--src", required=True)
    parser.add_argument("--dest", required=True)
    parser.add_argument("--process-date", required=True)
    args = parser.parse_args()

    spark = SparkSession\
        .builder\
        .appName("twitter_transformation")\
        .getOrCreate()

    twitter_transform(spark, args.src, args.dest, args.process_date)
