package advanced.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf

object SparkUDFExample2 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Cloudera Sample Job")
      .master("local")
      .config("spark.hadoop.fs.defaultFS", "hdfs://192.168.31.14:8020")
      .config("spark.hadoop.yarn.resourcemanager.address", "192.168.31.14:8032")
      .config("spark.yarn.jars", "hdfs://192.168.31.14:8020/user/talentorigin/jars/*.jar")
      .config("spark.hadoop.yarn.application.classpath", "$HADOOP_CONF_DIR,$HADOOP_COMMON_HOME/*,$HADOOP_COMMON_HOME/lib/*,$HADOOP_HDFS_HOME/*,$HADOOP_HDFS_HOME/lib/*,$HADOOP_MAPRED_HOME/*,$HADOOP_MAPRED_HOME/lib/*,$HADOOP_YARN_HOME/*,$HADOOP_YARN_HOME/lib/*")
      .getOrCreate()

    val stocks = spark.read.options(Map("header" -> "true", "inferSchema" -> "true"))
      .csv("datasets/nse-stocks/nse-stocks-data.csv")

    val averageUDF = udf[Double, Double, Double](avgerage)

    spark.sqlContext.udf.register("average_udf", averageUDF)

    stocks.createOrReplaceTempView("stocks_table")

    spark.sql("select SYMBOL, HIGH, LOW, average_udf(HIGH, LOW) as Avg_Price from stocks_table").show()

  }

  def avgerage(num1: Double, num2: Double): Double = (num1 + num2) / 2.0

}