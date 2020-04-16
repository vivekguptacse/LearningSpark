package com.sample.spark.learning

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object WordCount {

  def main(args : Array[String]): Unit =
  {
    // Initialize the logger
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Initialize the spark Context
    val sc = new SparkContext("local[*]" , "wordCountApplication")

    // Load the book data
    val lines = sc.textFile("resources/book.txt")

    // Map to all the words in the input book
    // use regular Expression
    val wordsMap = lines.flatMap(x => x.split("\\W+"))

    // Normalize the output ot lower case
    val lowerWordMap = wordsMap.map(x => x.toLowerCase)

    // Count the words by value
    val wordsCounts = lowerWordMap.countByValue()

//    wordsCounts.foreach(println)

    // Create the mapping of each work with 1
    val wordCountMap = lowerWordMap.map(x => (x,1))

    // reduce the count value by summing all the occurences of same words
    val reduceWordCount = wordCountMap.reduceByKey((x,y) => x + y)

    val sortedwordCountMap = reduceWordCount.map(x => (x._2 , x._1)).sortByKey()

    // Collect the result
    val results = sortedwordCountMap.collect()

    results.foreach(println)



  }

}
