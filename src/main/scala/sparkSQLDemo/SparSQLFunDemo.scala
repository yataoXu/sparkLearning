package sparkSQLDemo

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object SparSQLFunDemo {

  val spark = SparkSession.builder().master("local[2]").appName("SpakSQLFun")
    .getOrCreate()
  spark.conf.set("spark.sql.shuffle.partitions", 6)
  spark.conf.set("spark.executor.memory", "6g")

  spark.sparkContext.setLogLevel("WARN")


  def main(args: Array[String]): Unit = {
    val filePath = "D:\\working\\sparLearning\\src\\main\\resources\\visit.txt"
    val df_1 = spark.read.option("headler", "false").option("inferschema", "true").csv(filePath)
      .toDF("mac", "phone_brand", "enter_time", "first_time", "last_time", "region", "screen", "stay_long")

    df_1.select("*").show(5)

    //abs 绝对值
    df_1.selectExpr("abs(stay_long) as res_abs").show(5)
    // 手动将第一条记录的第一个字段置为空，则显示第二个字段值　
    df_1.selectExpr("coalesce(mac,screen,stay_long) as res_colesce").show(5)

    val df_numFie = spark.read.json("D:\\people.json")
    df_numFie.select("*").show(5)

    val df_numFie1 = spark.read.json("D:\\peoples.json")
    df_numFie1.select("*").show(5)

    val df_score = df_numFie1.select(df_numFie1("name"), df_numFie1("age"), explode(df_numFie1("myScore"))).toDF("name", "age", "myScore")
    val dfMyScore = df_score.select("name", "age", "myScore.score1", "myScore.score2")
    dfMyScore.select("*").show()
    df_numFie1.createOrReplaceTempView("d1")
    spark.sql(
      """
        |select explode(Array("a","b","c","d"))
        |from d1
      """
        .stripMargin).show(4, false)

    spark.sql(
      """
        |select explode(Map("a","b"))
        |from d1
      """.stripMargin).show(4, false)

    df_1.select(greatest("enter_time", "first_time", "last_time") as ("greatest")).show(3)


    df_1.createOrReplaceTempView("d1")
    spark.sql(
      """
        |select enter_time,first_time,last_time,stay_long,if(stay_long = 0,'x','y') as type from d1
      """.stripMargin).show(3)

    spark.sql(
      """
        |select screen,isnan(screen) as isnull,first_time,last_time,mac,if(isnan(mac),1,2)as type from d1
      """.stripMargin).show(3)

    spark.sql(
      """
        |select mac,json_tuple('{"a":"lihua","b":"wangming"}','a','b')
        |from d1
        |
        """.stripMargin).show(4, false)

    spark.sql(
      """
        |select mac,get_json_object('{"a":"lihua","b":"wangming"}','$.a') as valueOfJson
        |from d1
        |
        """.stripMargin).show(4, false)

    spark.sql(
      """
        |select mac,ascii(mac) as after_convert
        |from d1
      """.stripMargin).show(3, false)

    spark.sql(
      """
        |select mac,base64(mac) as after_convert
        |from d1
      """.stripMargin).show(3, false)

    spark.sql(
      """
        |select first_time,concat(mac,first_time) as after_convert
        |from d1
      """.stripMargin).show(3, false)

    spark.sql(
      """
        |select mac,first_time,concat_ws('<-->',mac,first_time) as after_convert
        |from d1
      """.stripMargin).show(3, false)

    spark.sql(
      """
        |select mac,encode(mac,'ISO-8859-1') as after_convert
        |from d1
      """.stripMargin).show(3, false)

    spark.sql(
      """
        |select mac,decode(encode(mac,'ISO-8859-1'),'ISO-8859-1') as after_convert
        |from d1
      """.stripMargin).show(3, false)

    spark.sql(
      """
        |select mac,format_number(3.1415926,3) as after_convert
        |from d1
      """.stripMargin).show(3, false)

    spark.sql(
      """
        |select mac,format_string('mac is %s ,phone_brand is %s',mac,phone_brand) as after_convert
        |from d1
      """.stripMargin).show(3, false)

    spark.sql(
      """
        |select mac,initcap('asdfSDFcasdfASDf') as after_convert from d1
      """.stripMargin).show(3, false)

    spark.sql(
      """
        |select mac,lower(mac) as lower,upper(mac) as upper
        |from d1
      """.stripMargin).show(3, false)

    spark.sql(
      """
        |select mac,instr(mac,'D') as after_convert
        |from d1
      """.stripMargin).show(3, false)

    spark.sql(
      """
        |select mac,length(mac) as after_convert
        |from d1
      """.stripMargin).show(3, false)

    spark.sql(
      """
        |select mac,first_time,levenshtein(mac,first_time) as after_convert
        |from d1
      """.stripMargin).show(3, false)

    spark.sql(
      """
        |select mac,first_time,locate('E',mac) as after_convert_E,locate('D',mac,8) as after_convert_D
        |from d1
      """.stripMargin).show(3, false)

    spark.sql(
      """
        |select mac,lpad(mac,20,'--') as left,rpad(mac,20,'--') as right
        |from d1
      """.stripMargin).show(3, false)

    spark.sql(
      """
        |select mac,ltrim(mac) as ltrim,rtrim(mac) as rtrim,trim(mac) as trim
        |from d1
      """.stripMargin).show(4, false)

    spark.sql(
      """
        |select mac,'http://facebook.com/path/p1.php?query=1#Ref' as url,
        |parse_url('http://facebook.com/path/p1.php?query=1#Ref','HOST') as host,
        |parse_url('http://facebook.com/path/p1.php?query=1#Ref','PATH') as path,
        |parse_url('http://facebook.com/path/p1.php?query=1#Ref','QUERY') as query,
        |parse_url('http://facebook.com/path/p1.php?query=1#Ref','PROTOCOL') as protocol,
        |parse_url('http://facebook.com/path/p1.php?query=1#引用','REF') as ref
        |from d1
      """.stripMargin).show(4, false)

    spark.sql(
      """
        |select first_time,reverse(mac) as reverse,repeat(mac,2) as repeat_mac
        |from d1
      """.stripMargin).show(4, false)

    spark.sql(
      """
        |select first_time,(first_time) as soundex
        |from d1
      """.stripMargin).show(4, false)

    spark.sql(
      """
        |select first_time,screen
        |from d1 where screen rlike '僵.*'
      """.stripMargin).show(4, false)

    spark.sql(
      """
        |select first_time,screen,split(first_time,' ')[0] as split
        |from d1
      """.stripMargin).show(4, false)

    //　　　|substring_index('http://facebook.com/path/p1.php?query=1#Ref','/','3')
    spark.sql(
      """
        |select first_time,
        |substr(first_time,4) as sunstr_1,
        |substr(first_time,4,10) as substr_2,
        |substring(first_time,1,11) as substring
        |from d1
      """.stripMargin).show(4, false)

    spark.sql(
      """
        |select first_time,
        |translate(first_time,'2018','0000') as translate
        |from d1
      """.stripMargin).show(4, false)

    spark.sql(
      """
        |select first_time,add_months(first_time,3) as after_add from d1
      """.stripMargin).show(3)

    spark.sql(
      """
        |select first_time,current_date() as current from d1
      """.stripMargin).show(3)

    spark.sql(
      """
        |select first_time,current_timestamp() as current_time from d1
      """.stripMargin).show(3, false)

    spark.sql(
      """
        |select first_time,current_timestamp() as current_time,datediff(first_time,current_timestamp) as diff from d1
      """.stripMargin).show(3, false)

    spark.sql(
      """
        |select first_time,date_add(first_time,3) as after_add from d1
      """.stripMargin).show(3, false)

    spark.sql(
      """
        |select first_time,
        |date_format(first_time,'yyyy-MM-dd') as date,
        |date_format(first_time,'yyyy') as year,
        |date_format(first_time,'MM') as month,
        |date_format(first_time,'dd') as day,
        |date_format(first_time,'HH:mm:ss') as time,
        |date_format(first_time,'HH') as hour,
        |date_format(first_time,'mm') as minute,
        |date_format(first_time,'ss') as seconds
        |from d1
      """.stripMargin).show(3, false)

    spark.sql(
      """
        |select first_time,date_sub(first_time,3) as sub
        |from d1
      """.stripMargin).show(3, false)

    spark.sql(
      """
        |select first_time,day(first_time) as sub
        |from d1
      """.stripMargin).show(3, false)

    spark.sql(
      """
        |select first_time,dayofyear(first_time) as day
        |from d1
      """.stripMargin).show(3, false)

    spark.sql(
      """
        |select first_time,dayofmonth(first_time) as month
        |from d1
      """.stripMargin).show(3, false)


    spark.sql(
      """
        |select first_time,from_unixtime(1250111000,'yyyy-MM-dd HH:mm:ss') as after_convert
        |from d1
      """.stripMargin).show(3, false)

    // 将first_time(默认是UTC时区时间)转换为PST时区的时间戳
    spark.sql(
      """
        |select first_time,from_utc_timestamp(first_time,'PST') as after_convert
        |from d1
      """.stripMargin).show(3, false)

    spark.sql(
      """
        |select first_time,hour(first_time) as after_convert
        |from d1
      """.stripMargin).show(3, false)

    spark.sql(
      """
        |select first_time,last_day(first_time) as after_convert
        |from d1
      """.stripMargin).show(3, false)

    spark.sql(
      """
        |select first_time,minute(first_time) as after_convert
        |from d1
      """.stripMargin).show(3, false)

    spark.sql(
      """
        |select first_time,month(first_time) as after_convert
        |from d1
      """.stripMargin).show(3, false)
    spark.sql(
      """
        |select enter_time,first_time,months_between(enter_time,first_time) as months_betweendemo
        |from d1
      """.stripMargin).show(5, false)

    //返回first_time开始，下周的星期二的日期
    spark.sql(
      """
        |select first_time,next_day(first_time,'TU') as after_convert
        |from d1
      """.stripMargin).show(3, false)

    spark.sql(
      """
        |select first_time,now() as after_convert
        |from d1
      """.stripMargin).show(3, false)

    spark.sql(
      """
        |select first_time,quarter(first_time) as after_convert
        |from d1
      """.stripMargin).show(3, false)

    spark.sql(
      """
        |select first_time,second(first_time) as after_convert
        |from d1
      """.stripMargin).show(3, false)

    spark.sql(
      """
        |select first_time,to_date(first_time) as after_convert
        |from d1
      """.stripMargin).show(3, false)


    spark.sql(
      """
        |select first_time,to_unix_timestamp('16/Mar/2017:12:25:01 +0800','dd/MMM/yyyy:HH:mm:ss Z') as after_convert
        |from d1
      """.stripMargin).show(3, false)

    spark.sql(
      """
        |select first_time,to_unix_timestamp(first_time,'yyyy-MM-dd HH:mm:ss') as after_convert
        |from d1
      """.stripMargin).show(3, false)

    spark.sql(
      """
        |select first_time,to_utc_timestamp(first_time,'yyyy-MM-dd HH:mm:ss') as after_convert
        |from d1
      """.stripMargin).show(3, false)

    spark.sql(
      """
        |select first_time,trunc(first_time,'MM') as after_convert
        |from d1
      """.stripMargin).show(3, false)

    spark.sql(
      """
        |select first_time,unix_timestamp(first_time) as after_convert
        |from d1
      """.stripMargin).show(3, false)

    spark.sql(
      """
        |select first_time,weekofyear(first_time) as after_convert
        |from d1
      """.stripMargin).show(3, false)

    spark.sql(
      """
        |select first_time,year(first_time) as after_convert
        |from d1
      """.stripMargin).show(3, false)


    spark.sql(
      """
        |select phone_brand,
        |count(phone_brand) as count,
        |avg(stay_long) as avg,
        |mean(stay_long) as mean,
        |min(stay_long) as min,
        |max(stay_long) as max,
        |sum(stay_long) as sum
        |from d1 group by phone_brand
      """.stripMargin).show(4, false)

    spark.sql(
      """
        |select phone_brand,
        |var_pop(stay_long) as fc,
        |stddev_pop(stay_long) as bzc,
        |skewness(stay_long) as pd,
        |kurtosis(stay_long) as ftz
        |from d1 group by phone_brand
      """.stripMargin).show(4, false)



  }
}
