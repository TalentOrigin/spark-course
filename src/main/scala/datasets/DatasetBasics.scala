package datasets

import org.apache.spark.sql.SparkSession

case class Ratings(userID: Integer, movieID: Integer, rating: Double, timestamp: Integer)

object DatasetBasics extends App {
  val sparkSession = SparkSession.builder()
    .appName("Dataset Basics")
    .master("local")
    .getOrCreate()

  import sparkSession.implicits._

  val ratingsDS = sparkSession.read
    .option("header", "true")
    //    .option("inferSchema", "true")
    .csv("/Users/talentorigin/workspace/datasets/movie_dataset/ratings_small.csv")
    .as[Ratings]

  ratingsDS.show()
}