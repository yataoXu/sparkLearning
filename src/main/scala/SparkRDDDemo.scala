
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object SparkRDDDemo {

  def main(args: Array[String]): Unit = {
//    val spark = SparkSession.builder().appName("RDDDemo")
//      .master("local[*]").getOrCreate()

    val conf = new SparkConf()
    conf.setMaster("local[3]").setAppName("partition")
    conf.set("spark.default.parallelism","4")

    val sc = new SparkContext(conf)
    sc.defaultParallelism
    val rdd1 =sc.parallelize(List(1,2,3,4,5))
    println(rdd1.getNumPartitions)

//    val rdd1 = spark.sparkContext.textFile("C:\\Users\\xu.yatao\\Desktop\\spark.txt")
//    val rdd1 =spark.sparkContext.parallelize(List(1,2,3,4,5))
//    println(rdd1.getNumPartitions)

  }

  def createRDD1{

    val spark = SparkSession.builder().appName("RDDDemo")
      .master("local[*]").getOrCreate()

    val rdd1 =spark.sparkContext.parallelize(List(1,2,3,4,5))
    println(rdd1.count())
    println(rdd1.getNumPartitions)

    val rdd2 =spark.sparkContext.parallelize(List(1,2,3,4,5),3)
    println(rdd2.count())
    println(rdd2.getNumPartitions)
  }
}
