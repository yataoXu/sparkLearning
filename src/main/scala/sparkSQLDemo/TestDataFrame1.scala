package sparkSQLDemo

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

//定义case class，相当于表结构
case class People(var name:String,var age:Int)
object TestDataFrame1 {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("mySpark")

    //setMaster("local") 本机的spark就用local，远端的就写ip
    //如果是打成jar包运行则需要去掉 setMaster("local")因为在参数中会指定。
    conf.setMaster("local")
    val sc = new SparkContext(conf)
    val context = new SQLContext(sc)

    //将本地的数据读入 RDD， 并将 RDD 与 case class 关联
    val peopleRDD = sc.textFile("D:\\people.txt").map(line => People(line.split(",")(0), line.split(",")(1).trim.toInt))
    // 将RDD 转换成 DataFrames
    import context.implicits._
    val df = peopleRDD.toDF
    //将DataFrames创建成一个临时的视图
    df.createOrReplaceTempView("people")
    //使用SQL语句进行查询
    context.sql("select * from people").show()
  }
}

