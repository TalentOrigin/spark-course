import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType

object SampleJob extends App {
  val spark = SparkSession.builder()
    .appName("Cloudera Sample Job")
    .master("local")
    .getOrCreate()

  val rdd = spark.sparkContext.parallelize(Array("2018-06-31"))
  val javaRDD = rdd.map(Row(_))
  val schema = StructType(StructField("col", StringType, true) :: Nil)
  val df = spark.createDataFrame(javaRDD, schema)
  df.printSchema()
  df.show()
  
  val modDF = df.withColumn("col", df.col("col").cast(TimestampType))
  modDF.printSchema()
  modDF.show()
}