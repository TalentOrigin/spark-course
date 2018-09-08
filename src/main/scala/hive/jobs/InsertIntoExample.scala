package hive.jobs

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode

object InsertIntoExample extends App {
  val spark = SparkSession.builder()
    .master("yarn")
    .appName("Cloudera Sample Job")
    .config("spark.yarn.jars", "hdfs://192.168.31.14:8020/user/talentorigin/jars/*.jar")
    .enableHiveSupport()
    .getOrCreate()

  val movies = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("datasets/movielens/movie.csv")

  movies.createOrReplaceTempView("tempMovies")

  val tempDF = spark.sql("select title, movieid, genres from tempMovies")

  tempDF.write.mode(SaveMode.Overwrite).insertInto("spark_course.movies")
}