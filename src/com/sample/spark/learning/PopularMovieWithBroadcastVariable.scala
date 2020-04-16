package com.sample.spark.learning

import java.nio.charset.CodingErrorAction

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

import scala.io.{Codec, Source}

object PopularMovieWithBroadcastVariable {

  def loadMovieData() : Map[Int, String] =
  {
    // Handle character encoding issue
    implicit val codec = Codec("UTF-8")
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)

    // Create a map of int to String , MovieID to Movie Name
    var movieNames : Map[Int, String] = Map()

    // read the Lines from the u-item file
    val lines = Source.fromFile("resources/ml-100k/u.item").getLines()
    for(line <- lines)
      {
        val fields = line.split('|')

        if(fields.length > 1)
          {
            movieNames += (fields(0).toInt -> fields(1))
          }
      }

    return movieNames
  }

  def main(args: Array[String]): Unit =
  {
    // Initialize the logger
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create the spark Context
    val sc = new SparkContext("local[*]", "popularMovieApplication")

    // Create a broadcast varibale
    val nameDict = sc.broadcast(loadMovieData)

    // Load the Movie Data
    val lines = sc.textFile("resources/ml-100k/u.data")

    // Get the movieId fields extracted from the lines
    val movieIdCountMap = lines.map(x => (x.split("\t")(1).toInt,1))

    // reduce the Movieicount map to get how many different users watched the movies.
    val reducedMovieCount = movieIdCountMap.reduceByKey((x,y) => x + y )

    // As now we want the most popolar movie so we need to sort the data based in count so just flip the data
    val flippedMovieCountMap = reducedMovieCount.map(x => (x._2,x._1))

    // Sort the data in descending order
    val sortedPopulatMovie = flippedMovieCountMap.sortByKey(false)

    // map the Movie name to themovie ID
    val poularMovieName = sortedPopulatMovie.map( x => ( nameDict.value(x._2),x._1))

    // Collect the data
    val results = poularMovieName.collect()

    // Print the result
    results.foreach(println)

  }



}
