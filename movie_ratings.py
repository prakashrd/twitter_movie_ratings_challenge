from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import explode, split
from pyspark.sql import SparkSession
import re
import argparse


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


def get_argument_parser():

    p = argparse.ArgumentParser(
        description="Twitter user feeds movie rating application determine the top "
                    "ranked gener for the past number of years"
    )
    p.add_argument(
        "-ratings_path",
        "--ratings_path",
        required=False,
        type=str,
        help="ratings.dat file path",
        default="./data/ratings.dat")
    p.add_argument(
        "-movies_path",
        "--movies_path",
        required=False,
        type=str,
        help="movies.dat file path",
        default="./data/movies.dat"
    )
    p.add_argument(
        "-num",
        "--no_years",
        required=False,
        type=int,
        default=10,
        help="number of years results to calculate"
    )
    p.add_argument(
        "-o",
        "--output_path",
        required=False,
        type=str,
        default="./output_results"
    )

    return p.parse_args()


if __name__ == '__main__':
    args = get_argument_parser()
    ratings = args.ratings_path
    movies = args.movies_path
    no_years = args.no_years
    output_path = args.output_path

    spark = SparkSession.builder.appName("TwitterMovieRatings").getOrCreate()

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
                spark.read.text(ratings).rdd.map(lambda line: split_ratings_row(line[0])),
                ratings_schema
            )

    movies_df = spark.createDataFrame(
                spark.read.text(movies).rdd.map(lambda line: split_movies_row(line[0])),
                movie_schema
            )

    last_decade_df = movies_df.select("movie_year") \
        .distinct() \
        .sort("movie_year", ascending=False) \
        .limit(no_years)

    joined_df = movies_df.join(last_decade_df, on=["movie_year"])\
        .join(ratings_df, on=['movie_id']) \
        .withColumn("gener", explode(split('movie_gener', '\|')))

    # join the two dataframes on 'movie_id' filter out rows with empty gener and do the aggregation
    result_df = joined_df.select("movie_year", "gener", "rating") \
        .filter(joined_df["movie_gener"] != "") \
        .groupby("movie_year", "gener") \
        .sum("rating") \
        .orderBy("movie_year","gener") \
        .sort("movie_year", ascending=False) \
        .select("movie_year", "gener")

    result_df.show(100, False)
    # final step to spit out the output to a csv file
    result_df\
        .coalesce(1) \
        .write \
        .format("csv") \
        .mode("overwrite") \
        .options(header="true", sep=",") \
        .save(output_path)

    spark.stop()

