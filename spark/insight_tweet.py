from pyspark.sql import SparkSession
from pyspark.sql.functions import col, countDistinct, to_date, when, sum, date_format

if(__name__ == "__main__"):
    spark = SparkSession.builder\
        .appName("twitter_insight").getOrCreate()

    tweet = spark.read.json(
        "/home/rodrigo.busto/Desktop/curso/data_lake/"
        "silver/twitter_aluraonline/tweets"
        )

    # alura_id = tweet.where("username='AluraOnline'").select("author_id").show(1)

    alura = tweet.where("author_id = '1566580880'")\
        .select("author_id", "conversation_id")

    tweet = tweet.alias("tweet")\
        .join(
            alura.alias("alura"),
            [
                tweet.conversation_id == alura.conversation_id,
                tweet.author_id != alura.author_id
            ],
            "left"
        )\
        .withColumn(
            "alura_conversation",
            when(col("alura.conversation_id").isNotNull(), 1).otherwise(0)
        )\
        .withColumn(
            "reply_alura",
            when(col("in_reply_to_user_id") == '1566580880', 1).otherwise(0)
        ).groupBy(to_date("created_at").alias("created_date"))\
        .agg(
            countDistinct("id").alias("n_tweets"),
            countDistinct("tweet.conversation_id").alias("n_conversations"),
            sum("alura_conversation").alias("alura_conversation"),
            sum("reply_alura").alias("reply_alura")
        )\
        .withColumn(
            "weekday",
            date_format("created_date", "E")
            )
        
    tweet.coalesce(1)\
        .write.json("/home/rodrigo.busto/Desktop/curso/data_lake/gold/twitter_insight")