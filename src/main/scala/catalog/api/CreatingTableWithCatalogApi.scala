package catalog.api

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField

object CreatingTableWithCatalogApi extends App {
  val spark = SparkSession.builder()
    .master("yarn")
    .appName("Cloudera Sample Job")
    .config("spark.yarn.jars", "hdfs://192.168.31.14:8020/user/talentorigin/jars/*.jar")
    .enableHiveSupport()
    .getOrCreate()

  val catalog = spark.catalog

  /*
   * 1. name of the columns
   * 2. data type of the column
   * 3. nullable
   */

  val movies = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("datasets/movielens/movie.csv")

  val schema = StructType(
    StructField("movieId", IntegerType, false)
      :: StructField("title", StringType, true)
      :: StructField("genres", StringType, true)
      :: Nil)

  catalog.createTable("spark_course.movies", "parquet", schema, Map("Comments" -> "Table Created with Catalog API"))

  println("Table Successfully Created: " + catalog.tableExists("spark_course.movies"))

  movies.write.insertInto("spark_course.movies")

  spark.table("spark_course.movies").show()

}