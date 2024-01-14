package lds.testBitMap

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}

import lds.testBitMap.MyBitMapGenUDAF.{deSerBitMap, serBitMap}
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.expressions.Aggregator
import org.roaringbitmap.RoaringBitmap

/**
  * @author lds
  * @date 2024-01-10  22:50
  */
object MyBitMapOrMergeUDAF extends Aggregator[Array[Byte], Array[Byte], Array[Byte]] {
  override def zero = {
    //构造一个空bitmap
    var bitmap: RoaringBitmap = RoaringBitmap.bitmapOf()
    serBitMap(bitmap)
  }

  override def reduce(b: Array[Byte], a: Array[Byte]) = {
    val bitmap1: RoaringBitmap = deSerBitMap(b)
    val bitmap2: RoaringBitmap = deSerBitMap(a)
    //合并两个bitmap(或操作)
    bitmap1.or(bitmap2)
    serBitMap(bitmap1)
  }

  override def merge(b1: Array[Byte], b2: Array[Byte]) = {
    reduce(b1, b2)
  }

  override def finish(reduction: Array[Byte]) = reduction

  override def bufferEncoder = Encoders.BINARY

  override def outputEncoder = Encoders.BINARY

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
