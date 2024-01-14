package lds.testBitMap

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}

import org.apache.spark.sql.Encoders
import org.apache.spark.sql.expressions.Aggregator
import org.roaringbitmap.RoaringBitmap

/**
  * @author lds
  * @date 2024-01-10  21:27
  */
object MyBitMapGenUDAF extends Aggregator[Int, Array[Byte], Array[Byte]] {
  //初始化buffer
  override def zero: Array[Byte] = {
    val bm: RoaringBitmap = RoaringBitmap.bitmapOf()
    serBitMap(bm)
  }

  //各分区局部聚合逻辑
  override def reduce(b: Array[Byte], a: Int): Array[Byte] = {
    //将序列化后的buff反序列化成RoaringBitMap对象
    val bitmap: RoaringBitmap = deSerBitMap(b)
    //向缓存的bitmap中添加新元素
    bitmap.add(a)
    //将更新后的缓存序列化成字节数组返回
    serBitMap(bitmap)
  }

  //全局聚合
  override def merge(b1: Array[Byte], b2: Array[Byte]): Array[Byte] = {
    val bitmap1: RoaringBitmap = deSerBitMap(b1)
    val bitmap2: RoaringBitmap = deSerBitMap(b2)
    //合并两个bitmap(或操作)
    bitmap1.or(bitmap2)
    serBitMap(bitmap1)
  }

  //返回最终输出结果的方法
  override def finish(reduction: Array[Byte]): Array[Byte] = {
    reduction
  }

  override def bufferEncoder = {
    Encoders.BINARY
  }

  override def outputEncoder = {
    Encoders.BINARY
  }

  /**
    * 自定义方法:实现bitmap序列化
    *
    * @param bm
    * @return
    */
  private def serBitMap(bm: RoaringBitmap): Array[Byte] = {
    val stream: ByteArrayOutputStream = new ByteArrayOutputStream()
    val dataOutputStream = new DataOutputStream(stream)
    bm.serialize(dataOutputStream)
    stream.toByteArray
  }

  /**
    * 自定义方法:反序列化,将Array[Byte]反序列化为bitmap
    *
    * @param byteArray
    * @return
    */
  private def deSerBitMap(byteArray: Array[Byte]): RoaringBitmap = {
    val bitmap: RoaringBitmap = RoaringBitmap.bitmapOf()
    val stream: ByteArrayInputStream = new ByteArrayInputStream(byteArray)
    val dataInputStream = new DataInputStream(stream)
    bitmap.deserialize(dataInputStream)
    bitmap
  }
}
