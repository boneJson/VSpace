package lds.testBitMap

import java.io.{ByteArrayInputStream, DataInputStream}

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.roaringbitmap.RoaringBitmap

/**
  * @author lds
  * @date 2024-01-10  22:07
  */
object MyS14_SPARKSQL的UDAF自定义函数应用实战 {
  def main(args: Array[String]): Unit = {
    val session: SparkSession = SparkSession.builder()
      .appName("自定义UDAF")
      .master("local")
      .getOrCreate()
    //加载待处理数据
    val schema = StructType(Seq(
      StructField("id", DataTypes.IntegerType),
      StructField("province", DataTypes.StringType),
      StructField("city", DataTypes.StringType),
      StructField("region", DataTypes.StringType),
      StructField("pv", DataTypes.IntegerType),
    ))
    var odsDF: DataFrame = session.read.schema(schema).csv("spark_study/data/udaf/input")
    odsDF.createTempView("df")
    //注册udaf函数
    import org.apache.spark.sql.functions.udaf
    session.udf.register("gen_bitMap", udaf(MyBitMapGenUDAF))
    //自定义并注册udf:自定义udf，一行对一行，定义一个scala函数，然后注册成udf函数即可。
    val card = (bmBytes: Array[Byte]) => {
      val bitmap: RoaringBitmap = RoaringBitmap.bitmapOf()
      val stream: ByteArrayInputStream = new ByteArrayInputStream(bmBytes)
      val dataInputStream = new DataInputStream(stream)
      bitmap.deserialize(dataInputStream)
      bitmap.getCardinality
    }
    session.udf.register("card_bm", card)

    //按省市区统计pv总数和uv总数
    val dwsDF = session.sql(
      """

      select
      province,
      city,
      region,
      sum(pv) as pv_amt,
      card_bm(gen_bitmap(id)) as uv_cnt,
      gen_bitmap(id) as bitmap

      from df
      group by province,city,region


""".stripMargin)
    dwsDF.show(100, false)
    dwsDF.createTempView("tmp")
    //按省市聚合
    println("==============按省市聚合============")
    session.udf.register("merge_bitmap",udaf(MyBitMapOrMergeUDAF))
    session.sql(
      """
        |select
        |province,
        |city,
        |sum(pv_amt) pv_amt,
        |merge_bitmap(bitmap) bitmap,
        |card_bm(merge_bitmap(bitmap)) as uv_cnt
        |from tmp
        |group by province,city
      """.stripMargin).show(100,false)
    session.close()
  }
}
