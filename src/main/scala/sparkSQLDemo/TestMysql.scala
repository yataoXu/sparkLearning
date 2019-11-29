package sparkSQl

import org.apache.spark.SparkConf
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.{Dataset, SQLContext, SparkSession}

import scala.collection.mutable.HashMap


object TestMysql {
  def main(args: Array[String]): Unit = {

    //首先新建一个sparkconf定义参数
//    val conf = new SparkConf().setMaster("local").setAppName("JDBCDataSource")
//    //创建sparkContext，是通往spark集群的唯一通道
//    val sc = new JavaSparkContext(conf)
//    sc.setLogLevel("WARN")
//    //新建一个sparksql
//    val sqlContext = new SQLContext(sc)

//        //操作MySQL
//        val mysql = sqlContext.read.format("jdbc")
//          .option("url","jdbc:mysql://192.168.37.69:3306/evandb?serverTimezone=GMT")
//          .option("dbtable","tbl_user").option("driver","com.mysql.cj.jdbc.Driver").
//          option("user","zhangkun").option("password","Qwe123!@#").load()
//        mysql.show()
//
//
//        val mysql2= sqlContext.read.format("jdbc").options(
//          Map(
//            "driver" -> "com.mysql.cj.jdbc.Driver",
//            "url" -> "jdbc:mysql://192.168.37.69:3306/evandb?serverTimezone=GMT",
//            "dbtable" -> "tbl_user",
//            "user" -> "zhangkun",
//            "password" -> "Qwe123!@#",
//            "fetchsize" -> "3")).load()
//        mysql2.show
//        mysql2.registerTempTable("student")
//        mysql2.sqlContext.sql("select * from student").collect().foreach(println)


    //sparksql连接mysql
    /*
     * 方法1：分别将两张表中的数据加载为DataFrame
     **/


//    Map<String,String> options = new HashMap<String,String>();
//    options.put("url", "jdbc:mysql://localhost:3306/tset");
//    options.put("driver", "com.mysql.jdbc.Driver");
//    options.put("user", "root");
//    options.put("password", "admin");
//    options.put("dbtable", "information");
//    Dataset myinfromation = sqlContext.read().format("jdbc").options(options).load();
//    //如果需要多张表，则需要再put一遍
//    options.put("dbtable", "score");
//    Dataset scores = sqlContext.read().format("jdbc").options(options).load();

     val spark =SparkSession.builder().master("local").appName("dd").getOrCreate()

    //方法2：分别将mysql中两张表的数据加载为DataFrame
    val reader = spark.read.format("jdbc")
    reader.option("url", "jdbc:mysql://192.168.37.69:3306/evandb?serverTimezone=GMT")
    reader.option("driver", "com.mysql.cj.jdbc.Driver")
    reader.option("user", "zhangkun")
    reader.option("password", "Qwe123!@#")
    reader.option("dbtable", "tbl_user")
    val myinformation = reader.load
    myinformation.registerTempTable("tbl_user")
    spark.sql("select * from tbl_user").show()

    reader.option("dbtable", "tbl_user_role")
    val scores = reader.load
    // 将两个DataFrame转换为javapairrdd，执行join操作
    scores.registerTempTable("tbl_user_role")

    spark.sql("select * from tbl_user_role").show()
    // 定义sql语句
    spark.sql("select * from tbl_user_role tur LEFT JOIN tbl_user tu on tur.user_id = tu.id ").show()
  }
}