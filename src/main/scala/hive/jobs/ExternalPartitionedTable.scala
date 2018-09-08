package hive.jobs

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode

object ExternalPartitionedTable extends App {
  val spark = SparkSession.builder()
    .master("yarn")
    .appName("Cloudera Sample Job")
    .config("spark.yarn.jars", "hdfs://192.168.31.14:8020/user/talentorigin/jars/*.jar")
    .enableHiveSupport()
    .getOrCreate()

  val stocksDF = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("/user/talentorigin/datasets/nse-stocks/nse-stocks-data.csv")

  stocksDF.write
    .mode(SaveMode.Overwrite)
    .option("path", "/user/talentorigin/external_table")
    .partitionBy("series")
    .saveAsTable("spark_course.external_partitioned_table")
}