package com.sample.spark.learning

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

/**
 * Find the Average number of friends according to age group
 */
object FriendsByAge {

  /** Main method */
  def main(args : Array[String]): Unit =
  {
    // Set the Logger level to ERROR Level so that unnecessary Logs will not be printed.
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create the spark context.
    val sc = new SparkContext("local[*]" , "FirendsByAgeApplication")

    // Load data
    // data Format is userID, UserName , Age , numberofFriends
    val lines = sc.textFile("resources/fakefriends.csv")

    // Apply map funtion to get the age and friends tuple
    val ageFriendsMap = lines.map(parseLines)

    // First we map the values to 1 so that once we sum up we get to the actual count how many friends group we added
    // and then use reduce by key which will add the values of the tuples.
    val totalByAge = ageFriendsMap.mapValues(x => (x,1)).reduceByKey((x,y) => (x._1 + y._1, x._2 + y._2))

    // Now calcute the average friends by age .
    val averageByAge = totalByAge.mapValues(x => x._1/x._2)

    // Collects the result from the RDD, This actually computing the DAG and executes the tasks.
    val result = averageByAge.collect()

    // Sort and prints the output
    result.sorted.foreach(println)

  }

  /**
   * This methos takes the line as input and return the tuple (age,#friends) after parsing the line.
   *
   * @param line
   * @return (age, #friends) tuple
   */
  def parseLines(line : String): (Int,Int) =
  {
    val linedata = line.split(",")
    val age = linedata(2).toInt
    val numberOfFriends = linedata(3).toInt
    (age,numberOfFriends)
  }

}
