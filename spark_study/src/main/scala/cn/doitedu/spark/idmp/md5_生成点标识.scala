package cn.doitedu.spark.idmp


import org.apache.commons.codec.digest.DigestUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}


/**
  * @author lds
  * @date 2024-01-14  15:51
  */
object md5_生成点标识 {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)
    val spark: SparkSession = SparkSession.builder()
      .appName(this.getClass.getSimpleName
      ).master("local")
      .getOrCreate()

    val str1: String = DigestUtils.md2Hex("9wle09325098743985")
    println(str1)
    val str2: String = DigestUtils.md2Hex("9843otherOgt;leorpo")
    println(str2)
    spark.close()
  }
}
