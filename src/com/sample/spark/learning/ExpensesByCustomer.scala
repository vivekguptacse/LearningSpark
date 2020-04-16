package com.sample.spark.learning

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object ExpensesByCustomer {

  def main(args : Array[String]): Unit =
  {
    // Initialize the log level
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create spark context
    val sc = new SparkContext("local[*]","CustomerExpenseApplication")

    // Load the data
    val lines = sc.textFile("resources/customer-orders.csv")

    // Parse the data and get the curtomerID and Expense tuple
    val custExpMap = lines.map(parseLines);

    // Sum all the values of individual customers expenses.
    val expenseByCustomer = custExpMap.reduceByKey((x,y) => x + y)

    // sort data by customerID
    val expenseByCustSorted = expenseByCustomer.sortByKey()

    // Collect the records
    val results = expenseByCustomer.collect()

    // print the records
//    results.foreach(println)

    // Lets find the which user is having the maximum expense
    val maxExpense = expenseByCustomer.map(x => (x._2,x._1))

    // val maxExpense by Sorted Key
    val maxExpenseSorted = maxExpense.sortByKey(false)

    // Collect the data
    val resultsMaxExpense = maxExpenseSorted.collect()

    // Print the result
    resultsMaxExpense.foreach(println)

  }

  /**
   * This method return the tuples(customerId, Expense)  from the input data
   * @param line
   * @return (customerId, Expense)
   */
  def parseLines(line : String) =
  {
    val fields = line.split(",")
    val customerId = fields(0)
    val expense = fields(2).toFloat
    (customerId, expense)
  }

}
