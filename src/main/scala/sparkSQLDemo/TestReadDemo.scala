package sparkSQLDemo

import org.apache.spark.sql.SparkSession

object TestReadDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("readDemo")
      .master("local").getOrCreate()
    //方式一
    val df1 = spark.read.json("D:\\people.json")
    val df2 = spark.read.parquet("D:\\users.parquet")
//    //方式二
    val df3 = spark.read.format("json").load("D:\\people.json")
    val df4 = spark.read.format("parquet").load("D:\\users.parquet")


    //方式一
    df1.write.json("D:\\111")
    df1.write.parquet("D:\\222")
    //方式二
    df1.write.format("json").save("D:\\333")
    df1.write.format("parquet").save("D:\\444")
    //方式三
    df1.write.save("D:\\555")
  }
}
