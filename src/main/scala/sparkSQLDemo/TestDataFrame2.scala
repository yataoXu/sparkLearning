package sparkSQLDemo

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object TestDataFrame2 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("TestDataFrame3")
      .master("local").getOrCreate()

    val fileRDD = spark.sparkContext.textFile("D:\\people.txt")
    val schemaString = "name age"

    val fields = schemaString.split(" ")
      .map(fieldName => StructField(fieldName, StringType, nullable = true))

    val shema = StructType(fields)

    val rowRDD = fileRDD.map(_.split(","))
      .map(attributes => Row(attributes(0), attributes(1).trim))

    val df = spark.createDataFrame(rowRDD, shema)
    df.createOrReplaceTempView("people")
    spark.sql("select * from people").show()
  }
}
