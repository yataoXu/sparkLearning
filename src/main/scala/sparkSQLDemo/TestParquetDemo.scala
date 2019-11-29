//package sparkSQLDemo
//
//import org.apache.spark.sql.SQLContext
//import org.apache.spark.sql.hive.HiveContext
//import org.apache.spark.{SparkConf, SparkContext}
//
//object TestParquetDemo {
//  def main(args: Array[String]): Unit = {
//    val conf = new SparkConf().setAppName("parquetDemo").setMaster("local")
//    val sc = new SparkContext(conf)
//    sc.setLogLevel("WARN")
//    val sqlContext = new SQLContext(sc)
//    val hiveContext = new HiveContext(sc)
//    // 使用sparksql访问parquet
//    parquetFile(sqlContext)
//    hiveTable(hiveContext).show()
//    sc.stop()
//  }
//
//  def hiveTable(hiveContext: HiveContext): Unit = {
//    hiveContext.table("temp")
//  }
//
//  def parquetFile(sqlContext: SQLContext): Unit = {
//    val df = sqlContext.read.parquet("D:\\users.parquet")
//    df.show()
//  }
//}
