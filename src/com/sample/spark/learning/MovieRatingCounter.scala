package com.sample.spark.learning

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

/**
 * Counts how many of each star rating exists in the MovieLens 100k data set.
 */
object MovieRatingCounter {

    /** Main function where actual actions happens */
    def main(args : Array[String]): Unit =
    {
      //Set the log level to only print Error
      Logger.getLogger("org").setLevel(Level.ERROR);

      // Create the spark context
      val sc = new SparkContext("local[*]" , "MovieRatingCounter")

      // Load up all the lines from the rating data to our RDD
      val lines = sc.textFile("resources/ml-100k/u.data")

      // Convert each line to a string, split it out by tabs, and extract the third field.
      // The file format is userID , MovieID , rating , timestamp
      val rating  = lines.map(x => x.toString().split("\t")(2))

      // Count how many times each values(ratings) occurs.
      val result = rating.countByValue()

      // sort the resulting map of (rating, count)  tuples
      val sortedResult = result.toSeq.sortBy(_._1)

      // Prints the result
      sortedResult.foreach(println)

    }

}
