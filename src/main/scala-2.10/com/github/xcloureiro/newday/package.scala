package com.github.xcloureiro

import com.github.xcloureiro.newday.Solution.{Movie, Rating}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, SaveMode}

package object newday {

  def loadAndSplitCsvFile(file: String, delimiter: String = "::")(implicit sc: SparkContext): RDD[Array[String]] = {
    sc.textFile(file).map(_.split(delimiter))
  }

  def loadMoviesData(file: String, delimiter: String = "::")(implicit sc: SparkContext): RDD[Movie] = {
    loadAndSplitCsvFile(file).map(m => Movie(m(0).toInt, m(1), m(2).split("\\|")))
  }

  def loadRatingsData(file: String, delimiter: String = "::")(implicit sc: SparkContext): RDD[Rating] = {
    loadAndSplitCsvFile(file).map(r => Rating(r(0).toInt, r(1).toInt, r(2).toShort, r(3).toLong))
  }

  def getMovieRatings(movies: DataFrame, ratings: DataFrame)(implicit sql: HiveContext): DataFrame = {
    import sql.implicits._

    movies.join(ratings, movies("MovieID") === ratings("MovieID"))
      .groupBy(movies("MovieID"), movies("Title"), movies("Genres"))
      .agg(
        max($"Rating").as("max_rating"),
        min($"Rating").as("min_rating"),
        round(avg($"Rating"), 3).as("avg_rating"))
  }

  def getUserTopMovies(movies: DataFrame, ratings: DataFrame)(implicit sql: HiveContext): DataFrame = {
    import sql.implicits._

    val window = Window.partitionBy($"UserID").orderBy($"Rating".desc, $"avg_rating".desc)

    movies.join(ratings, movies("MovieID") === ratings("MovieID"))
      .withColumn("rank", row_number().over(window))
      .where($"rank" <= 3)
      .select(
        $"UserID".as("UserID"),
        ratings("MovieID").as("MovieID"),
        $"Title".as("Title"),
        $"Rating".as("Rating"))
  }

  def overwriteDataFrame(df: DataFrame, filename: String, format: String = "parquet"): Unit = {
    df.repartition(1)
      .write
      .mode(SaveMode.Overwrite)
      .format(format)
      .save(filename)
  }

}
