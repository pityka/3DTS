package index2

import java.io._
import Helpers._
import org.xerial.snappy.Snappy

case class RowId(id: Long)

class SimpleLogWriter(f: File, compress: Boolean) {
  if (!f.canRead) {
    new java.io.FileOutputStream(f).close
  }
  val rf = new RandomAccessFile(f, "r")
  rf.seek(0)
  val magic = 0x983287ae
  val buf = Array.ofDim[Byte](8)
  val bufInt = Array.ofDim[Byte](4)

  assert(rf.length == 0 || readLong(rf, buf) == magic)

  if (rf.length == 0) {
    val rf = new RandomAccessFile(f, "rwd")
    rf.seek(0)
    writeLong(magic, rf, buf)
    writeLong(16L, rf, buf)
    rf.close
  }

  var lastId = {
    rf.seek(8)
    val lastId1 = readLong(rf, buf)
    val lastId = if (lastId1 != f.length) {
      println("File corrupt. Scan to the end. " + f)
      val (id, doc, size) = scanToLast.get

      println("Found last record: " + id + " " + size + " " + f + " " + doc)
      var lastid = id.id + size
      val rf = new RandomAccessFile(f, "rwd")
      rf.seek(8)
      writeLong(lastid, rf, buf)

      lastid
    } else lastId1
    truncate(lastId)
    lastId
  }

  private def scanToLast: Option[(RowId, Doc, Int)] = {
    // resourceLeak!!
    val is = new BufferedInputStream(new FileInputStream(f), 16 * 1024 * 1024)
    is.skip(16)
    val iter = new Iterator[(RowId, Array[Byte])] {
      var pre: Option[(RowId, Array[Byte])] = None
      var rowid = 16L
      def readahead = try {
        val l = readInt(is, bufInt)
        val ar = Array.ofDim[Byte](l)
        fill(is, ar)
        pre = Some((RowId(rowid), ar))
        rowid += l + 4

      } catch {
        case e: Exception => pre = None
      }

      readahead

      def hasNext = pre.isDefined
      def next = {
        val r = pre.get
        readahead
        r
      }
    }

    if (iter.isEmpty) None
    else {
      var k = iter.next
      while (iter.hasNext) {
        k = iter.next
      }
      val (rowid, ar) = k
      if (compress) Some((rowid, Doc(new String(Snappy.uncompress(ar))), ar.size))
      else Some((rowid, Doc(new String(ar)), ar.size))
    }

  }

  private def truncate(size: Long): Unit = {
    val rw = new RandomAccessFile(f, "rw");
    if (size != f.length) {
      println("File corrupt. Truncating to " + size + " " + f)
    }
    try {
      rw.setLength(size);
    } finally {
      rw.close();
    }
  }

  val os = new BufferedOutputStream(new FileOutputStream(f, true), 1024 * 1024 * 16)

  def close = {
    rf.close
    os.close
    val rw = new RandomAccessFile(f, "rwd")
    rw.seek(8)
    writeLong(lastId, rw, buf)
    rw.close
  }

  def flush = {
    os.flush
    val rw = new RandomAccessFile(f, "rwd")
    rw.seek(8)
    writeLong(lastId, rw, buf)
    rw.close
  }

  def appendRaw(bs: Array[Byte]): RowId = {
    writeInt(bs.size, os, bufInt)
    os.write(bs)
    val thisid = lastId
    lastId += 4 + bs.size
    RowId(thisid)
  }

  def append(d: Doc): RowId =
    if (compress) appendRaw(Snappy.compress(d.document.getBytes("UTF-8")))
    else appendRaw(d.document.getBytes("UTF-8"))

}

class SimpleLogReader(f: File, compressed: Boolean) {
  val rf = new RandomAccessFile(f, "r")
  rf.seek(0)
  val magic = 0x983287ae
  val buf = Array.ofDim[Byte](8)
  val bufInt = Array.ofDim[Byte](4)

  assert(rf.length == 0 || readLong(rf, buf) == magic)

  def get(r: RowId) = {
    {
      rf.seek(r.id)
      val l = readInt(rf, bufInt)
      val a = Array.ofDim[Byte](l)
      rf.readFully(a)
      if (compressed) Some(Doc(new String(Snappy.uncompress(a))))
      else Some(Doc(new String(a)))
    }
  }

  def close = rf.close

  def iterator = {
    // resourceLeak!!
    val is = new BufferedInputStream(new FileInputStream(f), 16 * 1024 * 1024)
    is.skip(16)
    new Iterator[(RowId, Doc)] {
      var pre: Option[(RowId, Doc)] = None
      var rowid = 16L
      def readahead = try {
        val l = readInt(is, bufInt)
        val ar = Array.ofDim[Byte](l)
        fill(is, ar)
        pre = if (compressed) Some((RowId(rowid), Doc(new String(Snappy.uncompress(ar)))))
        else Some((RowId(rowid), Doc(new String(ar))))
        rowid += l + 4

      } catch {
        case e: Exception => pre = None
      }

      readahead

      def hasNext = pre.isDefined
      def next = {
        val r = pre.get
        readahead
        r
      }
    }
  }

}
