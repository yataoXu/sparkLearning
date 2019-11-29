package sparkSQLDemo

import org.apache.spark.sql.SparkSession


object TestDataFrame3 {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("TestDataFrame3")
      .master("local").getOrCreate()

    val df = spark.read.json("D:\\people.json")
    df.createOrReplaceTempView("people")
    spark.sql("select * from people").show()
  }
}
