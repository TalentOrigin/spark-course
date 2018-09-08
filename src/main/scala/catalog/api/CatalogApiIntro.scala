package catalog.api

import org.apache.spark.sql.SparkSession

object CatalogApiIntro extends App {

  val spark = SparkSession.builder()
    .master("yarn")
    .appName("Cloudera Sample Job")
    .config("spark.yarn.jars", "hdfs://192.168.31.14:8020/user/talentorigin/jars/*.jar")
    .enableHiveSupport()
    .getOrCreate()

  val catalog = spark.catalog

  println(s"Current Database Name: " + catalog.currentDatabase)

  catalog.setCurrentDatabase("spark_course")

  println(s"Current Database Name after setCurrentDatabase: " + catalog.currentDatabase)

  val tables = catalog.listTables()

  tables.show()

  tables.select("name", "tableType", "isTemporary").show()
  
}