package index2

import com.google.common.io.{ LittleEndianDataOutputStream }
import java.io._
import java.nio._

class CSRWriter(cells: File, cidx: File, ridx: File) {

  val cellsOs = new LittleEndianDataOutputStream(new BufferedOutputStream(new FileOutputStream(cells, true)))
  val cidxOs = new LittleEndianDataOutputStream(new BufferedOutputStream(new FileOutputStream(cidx, true)))
  val ridxOs = new LittleEndianDataOutputStream(new BufferedOutputStream(new FileOutputStream(ridx, true)))

  val longBuffer = Array.ofDim[Byte](8)

  def convertLong(buf: Array[Byte]) =
    ByteBuffer.wrap(buf).order(ByteOrder.LITTLE_ENDIAN).getLong

  def close = {
    cellsOs.close
    cidxOs.close
    ridxOs.close
  }

  var nextRow: Long = ridx.length / 8
  if (nextRow == 0L) {
    ridxOs.writeLong(0L)
  }
  var i: Long = if (nextRow == 0L) 0L else {
    val rf = new RandomAccessFile(ridx, "r")
    try {
      rf.seek(ridx.length - 8)
      rf.readFully(longBuffer)
      convertLong(longBuffer)
    } finally {
      rf.close
    }

  }

  def appendSortedRow(r1: Long, bs: Vector[(Long, Double)]): Unit = {
    if (r1 < nextRow) throw new RuntimeException("Not an append")

    while (nextRow != r1) {
      ridxOs.writeLong(i)
      nextRow += 1
    }
    bs.foreach {
      case (c1, v1) =>
        if (v1 == 0.0) throw new RuntimeException("trying to append 0")
        cellsOs.writeDouble(v1)
        cidxOs.writeLong(c1)
    }
    ridxOs.writeLong(i + bs.size)
    nextRow += 1
    i = i + bs.size

  }

}

class CSRReader(cells: File, cidx: File, ridx: File) {

  val cellsrf = new RandomAccessFile(cells, "r")
  val cidxrf = new RandomAccessFile(cidx, "r")
  val ridxrf = new RandomAccessFile(ridx, "r")

  val longBuffer = Array.ofDim[Byte](8)

  def convertLong(buf: Array[Byte]) =
    ByteBuffer.wrap(buf).order(ByteOrder.LITTLE_ENDIAN).getLong

  def readLong(rf: RandomAccessFile): Long = {
    rf.readFully(longBuffer)
    convertLong(longBuffer)
  }

  def close = {
    cellsrf.close
    cidxrf.close
    ridxrf.close
  }

  def readColIdx(r: Long): Array[Long] = {
    if (ridx.length > (r + 1) * 8) {
      val (i1, i2) = {
        ridxrf.seek(r * 8)
        val i1 = readLong(ridxrf)
        val i2 = readLong(ridxrf)
        (i1, i2)
      }
      cidxrf.seek(i1 * 8)
      val len = (i2 - i1).toInt
      val buf = Array.ofDim[Byte](len * 8)
      cidxrf.readFully(buf)
      val buf2 = ByteBuffer.wrap(buf).order(ByteOrder.LITTLE_ENDIAN).asLongBuffer
      val buf3 = Array.ofDim[Long](len)
      var i = 0
      while (i < len) {
        buf3(i) = buf2.get
        i += 1
      }
      buf3
    } else Array()
  }

  def readColIdxIntersect(r: Long, intersect: Array[Long], forceBinary: Boolean = false): Array[Long] = {
    import scala.collection.mutable.ArrayBuffer

    def intersectSorted(a: Array[Long], b: Array[Long], acc: ArrayBuffer[Long], i: Int, j: Int): Array[Long] = {
      if (i >= a.size || j >= b.size) acc.toArray
      else if (a(i) < b(j)) intersectSorted(a, b, acc, i + 1, j)
      else if (b(j) < a(i)) intersectSorted(a, b, acc, i, j + 1)
      else {
        acc.append(a(i))
        intersectSorted(a, b, acc, i + 1, j + 1)
      }
    }

    def readLongAtIndex(i: Long): Long = {
      cidxrf.seek(i * 8)
      cidxrf.readFully(longBuffer)
      ByteBuffer.wrap(longBuffer).order(ByteOrder.LITTLE_ENDIAN).getLong
    }

    def binarySearch(i1: Long, v1: Long, i2: Long, v2: Long, q: Long): Long = {
      if (i1 > i2 || v1 > q || v2 < q) -1
      else if (v1 == q) i1
      else if (v2 == q) i2
      else {
        val m = (i1 + i2) / 2
        if (m == i1) -1
        else {
          val vm = readLongAtIndex(m)
          if (vm < q) binarySearch(m, vm, i2, v2, q)
          else if (vm > q) binarySearch(i1, v1, m, vm, q)
          else m
        }
      }
    }

    if (ridx.length > (r + 1) * 8) {
      val (i1, i2) = {
        ridxrf.seek(r * 8)
        val i1 = readLong(ridxrf)
        val i2 = readLong(ridxrf)
        (i1, i2)
      }
      val len = i2 - i1

      if ((intersect.size < 1000 && len > 100000) || forceBinary) {
        val mbuf = scala.collection.mutable.ArrayBuffer[Long]()
        val i2v = readLongAtIndex(i2 - 1)
        var i = 0
        var j = i1
        var jv = readLongAtIndex(j)
        while (i < intersect.size) {
          val q = intersect(i)
          // val t1 = System.nanoTime
          val qi = binarySearch(j, jv, i2 - 1, i2v, q)
          // println(System.nanoTime - t1)

          if (qi >= 0) {
            mbuf.append(q)
            j = qi
            jv = q
          }
          i += 1
        }
        mbuf.toArray
      } else {
        val q = readColIdx(r)
        val mbuf = scala.collection.mutable.ArrayBuffer[Long]()
        intersectSorted(q, intersect, mbuf, 0, 0)
      }
    } else Array()
  }

  def readValues(r: Long): Array[Double] = {
    if (ridx.length > (r + 1) * 8) {
      val (i1, i2) = {
        ridxrf.seek(r * 8)
        val i1 = readLong(ridxrf)
        val i2 = readLong(ridxrf)
        (i1, i2)
      }
      cellsrf.seek(i1 * 8)
      val len = (i2 - i1).toInt
      val buf = Array.ofDim[Byte](len * 8)
      cellsrf.readFully(buf)
      val buf2 = ByteBuffer.wrap(buf).order(ByteOrder.LITTLE_ENDIAN).asDoubleBuffer
      val buf3 = Array.ofDim[Double](len)
      var i = 0
      while (i < len) {
        buf3(i) = buf2.get
        i += 1
      }
      buf3
    } else Array()
  }

  def readLength(r: Long): Int = {
    if (ridx.length > (r + 1) * 8) {
      val (i1, i2) = {
        ridxrf.seek(r * 8)
        val i1 = readLong(ridxrf)
        val i2 = readLong(ridxrf)
        (i1, i2)
      }

      (i2 - i1).toInt

    } else 0
  }

}
