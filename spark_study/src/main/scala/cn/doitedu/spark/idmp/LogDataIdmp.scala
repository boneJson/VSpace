package cn.doitedu.spark.idmp

import cn.doitedu.spark.util.SparkUtil
import com.alibaba.fastjson.JSON
import org.apache.commons.lang3.StringUtils
import org.apache.spark.graphx.{Edge, Graph, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset

/**
  * @date: 2020/1/11
  * @site: www.doitedu.cn
  * @author: hunter.d 涛哥
  * @qq: 657270652
  * @description:
  * 3类埋点日志的id映射计算程序
  * （没考虑滚动整合）
  */
object LogDataIdmp {

  def main(args: Array[String]): Unit = {

    val spark = SparkUtil.getSparkSession(this.getClass.getSimpleName)

    import spark.implicits._
    // 一、加载3类数据
    val appLog: Dataset[String] = spark.read.textFile("E:\\study_data\\yiee_logs\\2020-01-11\\app")
    val webLog = spark.read.textFile("E:\\study_data\\yiee_logs\\2020-01-11\\web")
    val wxAppLog = spark.read.textFile("E:\\study_data\\yiee_logs\\2020-01-11\\wxapp")


    // 二、提取每一类数据中每一行的标识字段
    val app_ids: RDD[Array[String]] = extractIds(appLog)
    val web_ids: RDD[Array[String]] = extractIds(webLog)
    val wxapp_ids: RDD[Array[String]] = extractIds(wxAppLog)

    val ids: RDD[Array[String]] = app_ids.union(web_ids).union(wxapp_ids)

    // 三、构造图计算中的vertex集合
    val vertices: RDD[(Long, String)] = ids.flatMap(arr => {
      for (biaoshi <- arr) yield (biaoshi.hashCode.toLong, biaoshi)
    })

    // 四、构造图计算中的Edge集合
    val edges: RDD[Edge[String]] = ids.flatMap(arr => {
      // 用双层for循环，来对一个数组中所有的标识进行两两组合成边
      // [a,b,c,d] ==>   a-b  a-c  a-d  b-c  b-d  c-d
      for (i <- 0 to arr.length - 2; j <- i + 1 to arr.length - 1) yield Edge(arr(i).hashCode.toLong, arr(j).hashCode.toLong, "")
    })
      // 将边变成 （边,1)来计算一个边出现的次数
      .map(edge => (edge, 1))
      .reduceByKey(_ + _)
      // 过滤掉出现次数小于经验阈值的边
      .filter(tp => tp._2 > 2)
      .map(tp => tp._1)


    // 五、用点集合+边集合构造图，并调用最大连通子图算法
    val graph = Graph(vertices,edges)

    // VertexRDD[VertexId] ==>  RDD[(点id-Long,组中的最小值)]
    val res_tuples: VertexRDD[VertexId] = graph.connectedComponents().vertices

    // 可以直接用图计算所产生的结果中的组最小值，作为这一组的guid（当然，也可以自己另外生成一个UUID来作为GUID）
    // 保存结果
    res_tuples.toDF("biaoshi_hashcode","guid").write.parquet("data/idmp/2020-01-11")


    spark.close()

  }


  /**
    * 从一个日志ds中提取各类标识id
    *
    * @param logDs
    * @return
    */
  def extractIds(logDs: Dataset[String]): RDD[Array[String]] = {

    logDs.rdd.map(line => {

      // 将一行数据解析成json对象
      val jsonObj = JSON.parseObject(line)

      // 从json对象中取user对象
      val userObj = jsonObj.getJSONObject("user")
      val uid = userObj.getString("uid")

      // 从user对象中取phone对象
      val phoneObj = userObj.getJSONObject("phone")
      val imei = phoneObj.getString("imei")
      val mac = phoneObj.getString("mac")
      val imsi = phoneObj.getString("imsi")
      val androidId = phoneObj.getString("androidId")
      val deviceId = phoneObj.getString("deviceId")
      val uuid = phoneObj.getString("uuid")

      Array(uid, imei, mac, imsi, androidId, deviceId, uuid).filter(StringUtils.isNotBlank(_))
    })
  }


}
