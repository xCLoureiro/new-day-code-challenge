package com.github.xcloureiro.newday

import org.apache.log4j.Logger
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Try

object Solution {
  @transient lazy val logger = Logger.getLogger(getClass.getName)

  case class Movie(MovieID: Int, Title: String, Genres: Array[String])
  case class Rating(UserID: Int, MovieID: Int, Rating: Short, Timestamp: Long)

  def main(args: Array[String]): Unit = {
    logger info "Starting new-day-code-challenge application..."

    //Parse the input arguments
    val tryMoviesFile = Try(args(0))
    val tryRatingsFile = Try(args(1))
    val tryOutDir = Try(args(2))

    if(tryMoviesFile.isSuccess && tryRatingsFile.isSuccess && tryOutDir.isSuccess) {
      val moviesFile = tryMoviesFile.get
      val ratingsFile = tryRatingsFile.get
      val outDir = tryOutDir.get

      //Initialize Spark variables
      val conf = new SparkConf()
      implicit val sc = new SparkContext(conf)
      implicit val sql = new HiveContext(sc)
      import sql.implicits._

      //Load the datasets: Movies and Ratings
      val movies = loadMoviesData(moviesFile).toDF.cache
      val ratings = loadRatingsData(ratingsFile).toDF.cache

      //Dataframe with movies data and 3 new columns which contain the max, min and average rating for that movie
      val movieRatings = getMovieRatings(movies, ratings).cache

      //Dataframe with Top 3 movies, for each user, based on their rating
      //In case of same highest rating, the selection is made by global average of ratings
      val userTopMovies = getUserTopMovies(movieRatings, ratings)

      //Write all Dataframes in output directory
      overwriteDataFrame(movies, s"$outDir/movies")
      overwriteDataFrame(ratings, s"$outDir/ratings")
      overwriteDataFrame(movieRatings, s"$outDir/movieRatings")
      overwriteDataFrame(userTopMovies, s"$outDir/userTopMovies")

      //Clear the cached Dataframes
      movieRatings.unpersist()
      movies.unpersist()
      ratings.unpersist()

      logger info "Application executed successfully"
    }
    else { logger error "Application usage: <moviesFile> <ratingsFile> <outDir>" }
  }

}
