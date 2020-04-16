package com.sample.spark.learning

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

import scala.math.min

object MinimumTemperature {
  def main(args : Array[String]): Unit =
  {
    // Create the logger
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Intitialize the spark context
    val sc = new SparkContext("local[*]", "MinTemperatureApplication")

    // Load the data
    val lines = sc.textFile("resources/1800.csv")

    // Parse the input data to get the desired data in the tuple of (stationID, entryType and temperature)
    val parsedLines = lines.map(parseLines)

    // filter all lines with TMIN val
    val mintemps = parsedLines.filter(x => x._2 == "TMIN")

    // Remove the EntryType as all the Entry types are TMIN(temperature Minimum)
    val stationTempMap = mintemps.map(x => (x._1, x._3))

    // Now we need to reduce the keys where values are minimum for the particular station.
    val minTempByStation = stationTempMap.reduceByKey((x,y) => min(x,y))

    // Collec the data
    val results = minTempByStation.collect()

    // Print the data
    for(result <- results.sorted)
      {
        val station = result._1
        val temp = result._2
        val formattedTemp = f"$temp%.2f F"
        println(s"$station  minimum temperature : $formattedTemp")
      }
  }

  /**
   * This method parse the input line and extract the stationId, EntityType and Temperature
   * @param line
   * @return (stationID, entryType and temperature)
   */
  def parseLines(line : String) =
  {
    val fields = line.split(",")
    val stationID = fields(0)
    val entryType = fields(2)
    val temperature  = fields(3).toFloat * 0.1f * (9.0f / 5.0f) +32.0f
    (stationID,entryType,temperature)
  }
}
