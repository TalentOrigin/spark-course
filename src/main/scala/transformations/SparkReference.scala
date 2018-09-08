package transformations

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf

object SparkReference {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Cloudera Sample Job")
      .master("yarn")
      .config("spark.hadoop.fs.defaultFS", "hdfs://192.168.31.14:8020")
      .config("spark.hadoop.yarn.resourcemanager.address", "192.168.31.14:8032")
      .config("spark.yarn.jars", "hdfs://192.168.31.14:8020/user/talentorigin/jars/*.jar")
      .config("spark.hadoop.yarn.application.classpath", "$HADOOP_CONF_DIR,$HADOOP_COMMON_HOME/*,$HADOOP_COMMON_HOME/lib/*,$HADOOP_HDFS_HOME/*,$HADOOP_HDFS_HOME/lib/*,$HADOOP_MAPRED_HOME/*,$HADOOP_MAPRED_HOME/lib/*,$HADOOP_YARN_HOME/*,$HADOOP_YARN_HOME/lib/*")
      .getOrCreate()

    val movieDF = spark.read.options(Map("header" -> "true", "inferSchema" -> "true"))
      .csv("datasets/movielens/movie.csv")

    val toUpperUDF = udf[String, String](toUpper)
    val fetchFirstGenreUDF = udf[String, String](fetchFirstGenre)
    val concatStringsUDF = udf[String, String, String](concatStrings)
    val columnLengthsUDF = udf[Integer, String] (columnLengths)

    //    movieDF.select(fetchFirstGenreUDF(movieDF("genres"))).show()
    //    movieDF.withColumn("PrimaryGenre", fetchFirstGenreUDF(movieDF("genres")))
    //      .drop("genres").show()

    movieDF.createOrReplaceTempView("movies")
    spark.sqlContext.udf.register("toUpper", toUpperUDF)
    spark.sqlContext.udf.register("fetchFirstGenre", fetchFirstGenreUDF)
    spark.sqlContext.udf.register("concatStrings", concatStringsUDF)
    spark.sqlContext.udf.register("columnLengths", columnLengthsUDF)

//    spark.sql("select movieId, title, toUpper(genres), fetchFirstGenre(genres) from movies").show()
//    spark.sql("select movieId, title, toUpper(genres), fetchFirstGenre(genres), columnLengths(concatStrings(title, genres))  from movies").show()

    movieDF.cube("movieId").avg().show()
  }

  def toUpper(str: String): String = str.toUpperCase

  def fetchFirstGenre(str: String): String = str.split('|')(0)

  def concatStrings(s1: String, s2: String): String = s1 + "####" + s2

  def columnLengths(s: String): Integer = s.length
}