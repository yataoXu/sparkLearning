import org.apache.spark.sql.SparkSession

object WordCount {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("wordCount")
      .master("local").getOrCreate()

    val rdd = spark.sparkContext.textFile("D:\\tool\\spark\\spark-2.3.3-bin-hadoop2.7" +
      "\\bin\\spark-shell")
    val wordCount = rdd.flatMap(_.split(" ")).map(x =>(x,1)).reduceByKey(_+_)
    val wordCountsort = wordCount.map(x =>(x._2,x._1)).sortByKey(false)
      .map(x =>(x._2,x._1))
    wordCountsort.collect().foreach{
      i => println(i)
    }
    spark.close()
  }

}
