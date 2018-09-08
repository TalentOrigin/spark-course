package cloudera.jobs

import org.apache.spark.sql.SparkSession
import java.util.Properties

object QueryPushDown extends App {

  val spark = SparkSession.builder()
    .master("yarn")
    .appName("Cloudera Sample Job")
    .config("spark.yarn.jars", "hdfs://192.168.31.14:8020/user/talentorigin/jars/*.jar")
    .getOrCreate()

  val url = "jdbc:mysql://192.168.31.14:3306"

  val table = "retail_db.customers"

  val properties = new Properties()
  properties.put("user", "root")
  properties.put("password", "cloudera")
  Class.forName("com.mysql.jdbc.Driver")
  
  val query = "select customer_state, count(*) as state_wise_cust_count from retail_db.customers group by customer_state"
  
  val queryResult = spark.read.jdbc(url, s"($query) as cust_table", properties)

  queryResult.show(50)
  
}