package cn.doitedu.spark.idmp

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * @author lds
  * @date 2024-01-14  14:53
  */
object idmp_结果检验 {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName(this.getClass.getSimpleName
      ).master("local")
      .getOrCreate()
    val df: DataFrame = spark.read.parquet("data/idmp/2020-01-11")
    df.show(10,false)
    spark.close()
  }
}
