package cn.doitedu.spark.idmp

import cn.doitedu.spark.util.SparkUtil
import com.alibaba.fastjson.JSON
import org.apache.commons.lang3.StringUtils
import org.apache.spark.graphx.{Edge, Graph, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DataTypes, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row}


/**
  * @date: 2020/1/12
  * @site: www.doitedu.cn
  * @author: hunter.d 涛哥
  * @qq: 657270652
  * @description: 考虑上一日的idmp字典整合的idmapping程序
  *
  */
object LogDataIdmpV2 {

  def main(args: Array[String]): Unit = {
    if(args.length<5){
      println(
        """
          |help
          |请输入正确的参数：
          |1. app埋点日志原始文件输入路径
          |2. web埋点日志原始文件输入路径
          |3. wex埋点日志原始文件输入路径
          |4. 上日idmp映射字典所在路径
          |5. 当日idmp映射字典输出路径
          |6. spark运行模式的master
          |
        """.stripMargin)
      sys.exit(1)
    }


    val spark = SparkUtil.getSparkSession(this.getClass.getSimpleName,args(5))

    import spark.implicits._
    // 一、加载3类数据
    val appLog: Dataset[String] = spark.read.textFile(args(0))
    val webLog = spark.read.textFile(args(1))
    val wxAppLog = spark.read.textFile(args(2))


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

    // 五、将上一日的idmp映射字典，解析成点、边集合
    val schema = new StructType()
      .add("biaoshi_hashcode",DataTypes.LongType)
      .add("guid",DataTypes.LongType)
     val preDayIdmp: DataFrame = spark.read.schema(schema).parquet(args(3))
    // 构造点集合 preDayIdmpVertices
    val preDayIdmpVertices: RDD[(VertexId, String)] = preDayIdmp.rdd.map({
      case Row(idFlag: VertexId, guid: VertexId) =>
        (idFlag, "")
    })


    // 构造边集合
    val preDayEdges = preDayIdmp.rdd.map(row => {
      val idFlag = row.getAs[VertexId]("biaoshi_hashcode")
      val guid = row.getAs[VertexId]("guid")
      Edge(idFlag, guid, "")
    })


    // 六、将当日的点集合union上日的点集合，当日的边集合union上日的边集合，构造图，并调用连通子图算法
    val graph = Graph(vertices.union(preDayIdmpVertices), edges.union(preDayEdges))

    // VertexRDD[VertexId] ==>  RDD[(点id-Long,组中的最小值)]
    val res_tuples: VertexRDD[VertexId] = graph.connectedComponents().vertices


    // 八、将结果跟上日的映射字典做对比，调整guid
    // 1.将上日的idmp映射结果字典收集到driver端，并广播
    val idMap = preDayIdmp.rdd.map(row => {
      val idFlag = row.getAs[VertexId]("biaoshi_hashcode")
      val guid = row.getAs[VertexId]("guid")
      (idFlag, guid)
    }).collectAsMap()
    val bc = spark.sparkContext.broadcast(idMap)

    // 2.将今日的图计算结果按照guid分组,然后去跟上日的映射字典进行对比
    val todayIdmpResult: RDD[(VertexId, VertexId)] = res_tuples.map(tp => (tp._2, tp._1))
      .groupByKey()
      .mapPartitions(iter=>{
        //使用mapPartiion,一个分区取一次广播变量 从广播变量中取出上日的idmp映射字典
        val idmpMap = bc.value
        iter.map(tp => {
          // 当日的guid计算结果
          var todayGuid = tp._1
          // 这一组中的所有id标识
          val ids = tp._2

          // 遍历这一组id，挨个去上日的idmp映射字典中查找
          var find = false
          for (elem <- ids if !find) {
            val maybeGuid: Option[Long] = idmpMap.get(elem)
            // 如果这个id在昨天的映射字典中找到了，那么就用昨天的guid替换掉今天这一组的guid
            if (maybeGuid.isDefined) {
              todayGuid = maybeGuid.get
              find = true
            }
          }

          (todayGuid,ids)
        })
      })
      .flatMap(tp=>{
        val ids = tp._2
        val guid = tp._1
        for (elem <- ids) yield (elem,guid)
      })





    // 可以直接用图计算所产生的结果中的组最小值，作为这一组的guid（当然，也可以自己另外生成一个UUID来作为GUID）
    // 保存结果
    todayIdmpResult.coalesce(1).toDF("biaoshi_hashcode", "guid").write.parquet(args(4))


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
