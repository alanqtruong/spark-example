package com.example.spark

import org.apache.spark.sql.functions._

/**
  * Simple Spark SQL example showing the top 1000 movies by ratings
  * @author recklessN1nja
  */
object ExampleSparkSQL {

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkConfig.getSparkSession

    // load movies csv file into dataframe
    val moviesDF = sparkSession.read
      .format("csv")
      .option("header", "true")
      .load(SparkConfig.conf.getString("paths.movies"))

    // load ratings csv file into dataframe
    val ratingsDF = sparkSession.read
      .format("csv")
      .option("header", "true")
      .load(SparkConfig.conf.getString("paths.ratings"))

    //average the ratings for each movieId
    val avgRatingsDF = ratingsDF.groupBy("movieId")
      .agg(avg("rating").as("avgRating"))

    //join movies with average rating using movieId and sort by desc
    val movieRatingsDF = moviesDF.join(avgRatingsDF, "movieId")
      .select(col("title"),
        format_number(col("avgRating"), 2).as("Average Rating")
      ).sort(desc("Average Rating"))

    //show top 1000 movies based on average ratings
    movieRatingsDF.show(1000, truncate = false)
  }
}
