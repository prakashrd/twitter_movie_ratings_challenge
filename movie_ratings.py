from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import explode, split, regexp_extract, udf
from pyspark.sql import SparkSession
import re


def extract_movie_year(movie_title):
    year = None
    yy = re.findall("\(\d{4}\)", movie_title)
    if yy:
        p = re.compile('(\(|\))')
        year = int(p.sub('', yy[0]))
    return year


def split_ratings_row(row):
    columns = row.split("::")
    return [columns[0], columns[1], int(columns[2]), columns[3]]


def split_movies_row(row):
    columns = row.split("::")
    return [columns[0], columns[1], extract_movie_year(columns[1]), columns[2]]


if __name__ == '__main__':
    print("Lets start it")

    spark = SparkSession.builder.appName("SimpleApp").getOrCreate()
    extract_year_udf = udf(lambda t: extract_movie_year(t), IntegerType())
    spark.udf.register("extract_movie_year", extract_year_udf)

    ratings_schema = StructType([
                        StructField("user_id", StringType(), False)
                        , StructField("movie_id", StringType(), False)
                        , StructField("rating", IntegerType(), False)
                        , StructField("rating_timestamp", StringType(), False)
                    ])
    movie_schema = StructType([
                    StructField("movie_id", StringType(), False),
                    StructField("movie_title", StringType(), False),
                    StructField("movie_year", IntegerType(), False),
                    StructField("movie_gener", StringType(), True)
                ])

    # delimiter with multiple characters supported in Spark 2.8 and above
    #movies_df = spark.read.csv("ratings.dat", header=False, sep="::", schema=ratings_schema)

    ratings_df = spark.createDataFrame(
                spark.read.text("./data/ratings.dat").rdd.map(lambda line: split_ratings_row(line[0])),
                ratings_schema
            )

    movies_df = spark.createDataFrame(
                spark.read.text("./data/movies.dat").rdd.map(lambda line: split_movies_row(line[0])),
                movie_schema
            )

    # movies_df has some non-ascii characters need to set the encoding to utf-8 to display movies_df
    import sys
    reload(sys)
    sys.setdefaultencoding('utf-8')

    #movies_df.show()
    last_decade_df = movies_df.select("movie_year") \
        .distinct() \
        .sort("movie_year", ascending=False) \
        .limit(10)

    joined_df = movies_df.join(last_decade_df, on=["movie_year"])\
        .join(ratings_df, on=['movie_id']) \
        .withColumn("gener", explode(split('movie_gener', '\|')))

    #joined_df.show(100, False)
    #joined_df.filter(joined_df["movie_gener"] == "").show(100, False)

    joined_df.select("movie_year", "gener", "rating") \
        .filter(joined_df["movie_gener"] != "") \
        .groupby("movie_year", "gener") \
        .sum("rating") \
        .orderBy("movie_year","gener") \
        .sort("movie_year", ascending=False) \
        .show()

    spark.stop()

