package com.github.xcloureiro.newday

import java.io.File

import com.github.xcloureiro.newday.Solution.{Movie, Rating}
import org.apache.commons.io.FileUtils

import scala.util.Try

class SolutionSpec extends BaseSpec {

  private val moviesFile =  getClass.getResource("/movies.dat").getPath
  private val ratingsFile = getClass.getResource("/ratings.dat").getPath

  override protected def beforeAll(): Unit = {
    super.beforeAll()
  }


  "loadAndSplitCsvFile" should "return an RDD with all lines present in file" in {
    val rdd = loadAndSplitCsvFile(moviesFile)

    assert(rdd.count() == 3)
  }

  it should "return the data well splitted" in {
    val rdd = loadAndSplitCsvFile(moviesFile)
    val data = rdd.filter(m => m(0).toInt == 1).collect()(0)

    assert(data.length == 3)
    assert(data(0).toString.equals("1"))
    assert(data(1).toString.equals("Toy Story (1995)"))
    assert(data(2).toString.equals("Animation|Children's|Comedy"))
  }

  /* ----------------------------------------------------------- */

  "loadMoviesData" should "return RDD with the type Movie" in {
    val rdd = loadMoviesData(moviesFile)
    val movie = rdd.collect()(0)

    assert(movie.isInstanceOf[Movie])
  }

  it should "return the Movie' data well defined" in {
    val rdd = loadMoviesData(moviesFile)
    val movie = rdd.filter(m => m.MovieID == 1).collect()(0)

    assert(movie.MovieID == 1)
    assert(movie.Title.equals("Toy Story (1995)"))
    assert(movie.Genres.length == 3)
    assert(movie.Genres.mkString("|").equals("Animation|Children's|Comedy"))
  }

  /* ----------------------------------------------------------- */

  "loadRatingsData" should "return RDD with the type Rating" in {
    val rdd = loadRatingsData(ratingsFile)
    val rating = rdd.collect()(0)

    assert(rating.isInstanceOf[Rating])
  }

  it should "return the Rating' data well defined" in {
    val rdd = loadRatingsData(ratingsFile)
    val rating = rdd.filter(r => r.UserID == 1 && r.MovieID == 1).collect()(0)

    assert(rating.UserID == 1)
    assert(rating.MovieID == 1)
    assert(rating.Rating == 5)
    assert(rating.Timestamp == 978300760)
  }

  /* ----------------------------------------------------------- */

  "getMovieRatings" should "return the aggregations of Movies and Ratings" in {
    val sqlCtx = sql
    import sqlCtx.implicits._

    val movies = loadMoviesData(moviesFile).toDF
    val ratings = loadRatingsData(ratingsFile).toDF
    val movieRatings = getMovieRatings(movies, ratings)

    val aggs = movieRatings.filter(movieRatings("MovieID") < 2).collect()
    assert(aggs.length == 1)

    val agg = aggs(0)

    val idxMovieID = agg.fieldIndex("MovieID")
    assert(agg.getInt(idxMovieID) == 1)

    val idxTitle = agg.fieldIndex("Title")
    assert(agg.getString(idxTitle).equals("Toy Story (1995)"))

    val idxGenres = agg.fieldIndex("Genres")
    assert(agg.getList[String](idxGenres).size == 3)

    val idxMaxRating = agg.fieldIndex("max_rating")
    assert(agg.getShort(idxMaxRating) == 5)

    val idxMinRating = agg.fieldIndex("min_rating")
    assert(agg.getShort(idxMinRating) == 1)

    val idxAvgRating = agg.fieldIndex("avg_rating")
    assert(agg.getDouble(idxAvgRating) == 3.0)
  }

  /* ----------------------------------------------------------- */

  "getUserTopMovies" should "return the user top 3 movies" in {
    val sqlCtx = sql
    import sqlCtx.implicits._

    val movies = loadMoviesData(moviesFile).toDF
    val ratings = loadRatingsData(ratingsFile).toDF
    val movieRatings = getMovieRatings(movies, ratings)

    val userTopMovies = getUserTopMovies(movieRatings, ratings)
    val userTop = userTopMovies.filter(userTopMovies("UserID") < 2).orderBy("MovieID").collect()

    assert(userTop.length == 3)

    val idxMovieID = userTop(0).fieldIndex("MovieID")

    val firstMovie = userTop(0).getInt(idxMovieID)
    assert(firstMovie == 1)

    val secondMovie = userTop(1).getInt(idxMovieID)
    assert(secondMovie == 2)

    val thirdMovie = userTop(2).getInt(idxMovieID)
    assert(thirdMovie == 3)
  }

  /* ----------------------------------------------------------- */

  "overwriteDataFrame" should "save the Dataframe correctly" in {
    val sqlCtx = sql
    import sqlCtx.implicits._

    val outFile = "/tmp/out_movies"

    val movies = loadMoviesData(moviesFile).toDF

    overwriteDataFrame(movies, outFile)

    val moviesParquet = sql.read.parquet(outFile)

    assert(movies.count == moviesParquet.count)

    //Remove the tmp file
    val tryRemoveFile = Try(FileUtils.deleteDirectory(new File(outFile)))
    assert(tryRemoveFile.isSuccess)
  }

}
