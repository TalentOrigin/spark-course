package cloudera.jobs

import org.apache.spark.sql.SparkSession

object ClouderaSampleJob2 extends App {
  val spark = SparkSession.builder()
    .master("yarn")
    .appName("Cloudera Sample Job")
    .config("spark.yarn.jars", "hdfs://35.211.96.134/:8020/user/talentorigin/spark-jars/*.jar")
    .getOrCreate()
    
    println("======================= Job Started ==========================")

//  val stocksDF = spark.read
//    .option("header", "true")
//    .csv("/user/talentorigin/datasets/nse-stocks/nse-stocks-data.csv")
//
//  stocksDF.show()
}