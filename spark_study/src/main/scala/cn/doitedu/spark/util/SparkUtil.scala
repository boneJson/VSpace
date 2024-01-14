package cn.doitedu.spark.util

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * @author lds
  * @date 2024-01-12  19:36
  */
object SparkUtil {
  def getSparkSession(appName:String = "app",master:String = "local[*]",confMap:Map[String,String]=Map.empty):SparkSession = {

    val conf = new SparkConf()
    conf.setAll(confMap)

    SparkSession.builder()
      .appName(appName)
      .master(master)
      .config(conf)
      .getOrCreate()

  }
}
