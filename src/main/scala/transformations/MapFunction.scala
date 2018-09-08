package transformations

import org.apache.spark.sql.SparkSession

object MapFunction {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local")
      .appName("Spark Transformations")
      .enableHiveSupport()
      .getOrCreate()

    val numArr = (1 to 100).toArray
    val rdd = spark.sparkContext.parallelize(numArr, 5)

    //    rdd.map(squares(_)).foreach(println)

    //    rdd.mapPartitions(iter => squares(iter)).foreach(println)

    //    rdd.mapPartitionsWithIndex { (part, iter) =>
    //      println("Running Partition: " + part)
    //      squares(iter)
    //    }.foreach(println)

    val strArray = Array("Hello There", "Welcome To TalentOrigin Channel", "Enjoy Apache Spark Tutorial")
    val strRdd = spark.sparkContext.parallelize(strArray, 2)
    val res = strRdd.flatMap(line => line.split(" "))
    val res1 = strRdd.map(line => line.split(" "))
    
    res.foreach(println)
    res1.foreach(println)
  }

  def squares(iter: Iterator[Int]): Iterator[Int] = iter.toArray.map(squares(_)).toIterator
  def squares(num: Int): Int = num * num

}