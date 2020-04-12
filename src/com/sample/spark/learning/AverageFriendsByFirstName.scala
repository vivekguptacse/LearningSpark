package com.sample.spark.learning

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

/**
 * * Find the Average number of friends according to first name
 */
object AverageFriendsByFirstName {

  /**
   * Parse Line and return (name , age ) tuple
   * @param line
   * @return   (name , age ) tuple
   */
  def parseLines(line : String): (String, Int) =
  {
    val values = line.split(",")
    val name = values(1).toLowerCase
    val age = values(3).toInt
    (name,age)
  }

  /** Main Method */
  def main(args: Array[String]): Unit =
  {
    // Change the log level to Error
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create the Spark Context
    val sc = new SparkContext("local[*]" , "AvgFriendsByFirstNameApplication")

    // Load data
    val lines = sc.textFile("resources/fakefriends.csv")

    // Map data , Extract only Name and age data from all data
    val nameAgeMap = lines.map(parseLines)

    // First we will map the values to 1 so that when we need to take the average then we need the count as well.
    val totalAgeByName = nameAgeMap.mapValues(x => (x ,1)).reduceByKey((x,y) => (x._1 + y._1 , x._2 + y._2))

    // Find the Average Friends by name
    val avgFriendsByName = totalAgeByName.mapValues(x => x._1/x._2)

    // Collect the result , Actually this will compute the DAG and execute the Task.
    val result = avgFriendsByName.collect()

    // print the values.
    result.sorted.foreach(println)





  }



}
