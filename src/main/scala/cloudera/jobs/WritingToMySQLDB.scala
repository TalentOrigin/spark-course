package cloudera.jobs

import org.apache.spark.sql.SparkSession
import java.util.Properties
import org.apache.spark.sql.SaveMode

object WritingToMySQLDB extends App {

  val spark = SparkSession.builder()
    .master("yarn")
    .appName("Cloudera Sample Job")
    .config("spark.yarn.jars", "hdfs://192.168.31.14:8020/user/talentorigin/jars/*.jar")
    .getOrCreate()

  val url = "jdbc:mysql://192.168.31.14:3306"

  val properties = new Properties()
  properties.put("user", "root")
  properties.put("password", "cloudera")
  Class.forName("com.mysql.jdbc.Driver")

  val nseStocks = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("datasets/nse-stocks/nse-stocks-data.csv")

  println("================== Record Count: " + nseStocks.count())

  val table = "spark2_course.nse_stocks"

  nseStocks.write.mode(SaveMode.Overwrite).jdbc(url, table, properties)

}