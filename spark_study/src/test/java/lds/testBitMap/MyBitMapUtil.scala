package lds.testBitMap

import org.roaringbitmap.RoaringBitmap

/**
  * @author lds
  * @date 2024-01-10  21:22
  */
object MyBitMapUtil {
  def main(args: Array[String]): Unit = {
    val bm1: RoaringBitmap = RoaringBitmap.bitmapOf(1,2,4,5,6)
    val bm2 = RoaringBitmap.bitmapOf(1,2,3,7,8,9)

    bm1.or(bm2)
    //
    println(bm1.getCardinality)

    println("========测试string类型===========")
    "1".hashCode()
    val bma: RoaringBitmap = RoaringBitmap.bitmapOf("1".hashCode(),"2".hashCode)
    val bmb =RoaringBitmap.bitmapOf("3".hashCode)
    bma.or(bmb)
    print(bma.getCardinality)
  }
}
