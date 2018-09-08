package cloudera.jobs

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode

object ReadingHiveTable extends App {
  val spark = SparkSession.builder()
    .master("yarn")
    .appName("Cloudera Sample Job")
    .config("spark.yarn.jars", "hdfs://192.168.31.14:8020/user/talentorigin/jars/*.jar")
    .enableHiveSupport()
    .getOrCreate()

  val stocksData = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("hdfs://192.168.31.14/user/talentorigin/datasets/nse-stocks/nse-stocks-data.csv")
    
    stocksData.show()
    
  stocksData.write
    .partitionBy("series")
    .mode(SaveMode.Overwrite).saveAsTable("talentorigin.nse_stocks_data")

}