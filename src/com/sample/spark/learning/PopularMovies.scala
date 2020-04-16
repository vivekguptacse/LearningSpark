package com.sample.spark.learning

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object PopularMovies {

  def main(args: Array[String]): Unit =
  {
    // Initialize the logger
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create the spark Context
    val sc = new SparkContext("local[*]", "popularMovieApplication")

    // Load the Movie Data
    val lines = sc.textFile("resources/ml-100k/u.data")

    // Get the movieId fields extracted from the lines
    val movieIdCountMap = lines.map(x => (x.split("\t")(1),1))

    // reduce the Movieicount map to get how many different users watched the movies.
    val reducedMovieCount = movieIdCountMap.reduceByKey((x,y) => x + y )

    // As now we want the most popolar movie so we need to sort the data based in count so just flip the data
    val flippedMovieCountMap = reducedMovieCount.map(x => (x._2,x._1))

    // Sort the data in descending order
    val sortedPopulatMovie = flippedMovieCountMap.sortByKey(false)

    // Collect the data
    val results = sortedPopulatMovie.collect()

    // Print the result
    results.foreach(println)

  }

}
