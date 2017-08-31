package index2

import java.io._
import java.nio.{ ByteBuffer, ByteOrder }

object Helpers {
  def writeLong(l: Long, os: OutputStream, buf: Array[Byte]) = {
    val ar = ByteBuffer.wrap(buf).order(ByteOrder.LITTLE_ENDIAN).putLong(l)
    os.write(buf)
  }

  def writeLong(l: Long, os: RandomAccessFile, buf: Array[Byte]) = {
    val ar = ByteBuffer.wrap(buf).order(ByteOrder.LITTLE_ENDIAN).putLong(l)
    os.write(buf)
  }

  def readLong(raf: RandomAccessFile, buf: Array[Byte]) = {
    raf.readFully(buf)
    ByteBuffer.wrap(buf).order(ByteOrder.LITTLE_ENDIAN).getLong
  }

  def readInt(raf: RandomAccessFile, buf: Array[Byte]) = {
    raf.readFully(buf)
    ByteBuffer.wrap(buf).order(ByteOrder.LITTLE_ENDIAN).getInt
  }

  def readLong(is: InputStream, buf: Array[Byte]) = {
    fill(is, buf)
    ByteBuffer.wrap(buf).order(ByteOrder.LITTLE_ENDIAN).getLong
  }

  def readInt(is: InputStream, buf: Array[Byte]) = {
    fill(is, buf)
    ByteBuffer.wrap(buf).order(ByteOrder.LITTLE_ENDIAN).getInt
  }

  def writeInt(l: Int, os: OutputStream, buf: Array[Byte]) = {
    val ar = ByteBuffer.wrap(buf).order(ByteOrder.LITTLE_ENDIAN).putInt(l)
    os.write(buf)
  }

  def fill(is: InputStream, ar: Array[Byte]) = {
    val size = ar.size
    var len = size
    while (len > 0) {
      val c = is.read(ar, size - len, len)
      if (c < 0) throw new RuntimeException("unexpected end of stream")
      len -= c
    }
  }
}
