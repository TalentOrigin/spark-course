package datasets

import org.apache.spark.sql.SparkSession

object DatasetBasicOperations extends App {
  val sparkSession = SparkSession.builder()
    .appName("Dataset Basics")
    .master("local")
    .getOrCreate()

  import sparkSession.implicits._

  val ratingsDS = sparkSession.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("/Users/talentorigin/workspace/datasets/movie_dataset/ratings_small.csv")
    .as[Ratings]

  //  val filterDemo = ratingsDS.filter(ratingObj => ratingObj.rating == 4.0)
  //  filterDemo.show()
  //  println("Count of 4.0 start Movies: " + filterDemo.count())

  val whereDemo = ratingsDS.where("rating == 4.0")
  ratingsDS.where("rating == 4.0")
  whereDemo.show()

  case class MovieRating(movieID: Integer, rating: Double)

  val selectedColumns = ratingsDS.select("movieID", "rating").as[MovieRating]
  selectedColumns.show()
}