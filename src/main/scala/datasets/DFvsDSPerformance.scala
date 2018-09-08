package datasets

import org.apache.spark.sql.SparkSession

object DFvsDSPerformance extends App {

  case class Credits(cast: String, crew: String, ID: String)

  val sparkSession = SparkSession.builder()
    .appName("Dataset Basics")
    .master("local")
    .getOrCreate()

  var startTime: Long = 0
  var endTime: Long = 0

  startTime = System.currentTimeMillis()

  val creditsDF = sparkSession.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("/Users/talentorigin/workspace/datasets/movie_dataset/credits.csv")

  val filteredRows = creditsDF.filter("id > 10000")
  println("No. of IDs greater than 10000: " + filteredRows.count())

  endTime = System.currentTimeMillis()

  import sparkSession.implicits._

  val startTimeDS = System.currentTimeMillis()

  val creditsDS = sparkSession.read
    .option("header", "true")
    //    .option("inferSchema", "true")
    .csv("/Users/talentorigin/workspace/datasets/movie_dataset/credits.csv")
    .as[Credits]

  val filteredRowsDS = creditsDS.filter("id > 10000")
  println("No. of IDs greater than 10000 for DS: " + filteredRowsDS.count())

  val endTimeDS = System.currentTimeMillis()

  println("Time to calculate with DF: " + (endTime - startTime) / 1000.0)
  println("Time to calculate with DS: " + (endTimeDS - startTimeDS) / 1000.0)
}