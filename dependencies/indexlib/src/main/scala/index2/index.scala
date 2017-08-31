package index2

import java.io._
import com.spotify.sparkey._
import scala.util._
import scala.collection.JavaConversions._

case class Doc(document: String)
case class DocId(id: Long)

class IndexReader(f: File, compressed: Boolean = false) {
  override def toString = s"IndexReader($f)"
  val csrCells = new File(f.getAbsolutePath + ".csr.cells")
  val csrRows = new File(f.getAbsolutePath + ".csr.rows")
  val csrCols = new File(f.getAbsolutePath + ".csr.cols")
  val sparkeyFile = new File(f.getAbsolutePath + ".sparkey")
  val docLogFile = new File(f.getAbsolutePath + ".docs.log")

  val csr = new CSRReader(csrCells, csrCols, csrRows)
  val sparkey = Sparkey.open(sparkeyFile)
  val docReader = if (docLogFile.canRead) Some(new SimpleLogReader(docLogFile, compressed)) else None

  def termIterator : Iterator[String] = {
    sparkey.iterator.map(_.getKeyAsString)
  }

  def getRowId(term: String): Option[Long] = {
    val buf = sparkey.getAsByteArray(term.getBytes("UTF-8"))
    if (buf == null) None
    else
      Some(java.nio.ByteBuffer.wrap(buf).order(java.nio.ByteOrder.LITTLE_ENDIAN).getLong)

  }

  def getDocIds(term: String): Vector[DocId] = {
    getRowId(term).toVector.flatMap { rid =>
      csr.readColIdx(rid).toVector.map(d => DocId(d))
    }
  }

  def getDocIds(term: String, max: Int): Vector[DocId] = {
    getRowId(term).toVector.flatMap { rid =>
      if (max >= 0) csr.readColIdx(rid).take(max).toVector.map(d => DocId(d))
      else csr.readColIdx(rid).toVector.map(d => DocId(d))
    }
  }

  def getDocIds(term: String, intersect: Vector[DocId], max: Int): Vector[DocId] = {
    getRowId(term).toVector.flatMap { rid =>
      if (max >= 0) csr.readColIdxIntersect(rid, intersect.map(_.id).toArray).take(max).toVector.map(d => DocId(d))
      else csr.readColIdxIntersect(rid, intersect.map(_.id).toArray).toVector.map(d => DocId(d))

    }
  }

  def getDocs(term: String): Vector[Doc] = getDocIds(term).flatMap(getDoc)

  def getDocCount(term: String) =
    getRowId(term).map(rid => csr.readLength(rid)).getOrElse(0)

  def getDoc(docid: DocId): Option[Doc] = {
    docReader.get.get(RowId(docid.id))
  }

  def close = {
    csr.close
    sparkey.close
  }

}

class IndexWriter(f: File, uniqueDocuments: Boolean, compressed: Boolean = false) {

  def fastSplit1WideSeparatorIterator(str: String, sep: Char): Iterator[String] = new Iterator[String] {
    private var i = 0

    def hasNext = i <= str.length

    def next = {
      val j = i
      while (i < str.length && str.charAt(i) != sep) {
        i += 1
      }
      val ret = str.substring(j, i)
      i = i + 1

      ret
    }

  }

  override def toString = s"IndexWriter($f)"

  val sep = new String(Array[Byte](-53))

  val termLogFile = new File(f.getAbsolutePath + ".terms.log")
  var termLogWriter: Option[SimpleLogWriter] = None

  val docLogFile = new File(f.getAbsolutePath + ".docs.log")
  var docLogWriter: Option[SimpleLogWriter] = None

  val term2rid = new File(f.getAbsolutePath + ".term2rid")

  def close = {
    docLogWriter.foreach(_.close)
    termLogWriter.foreach(_.close)
  }

  def flushAppenders = {
    docLogWriter.foreach(_.flush)
    termLogWriter.foreach(_.flush)
  }

  def add(doc: Doc, terms: Seq[String]): Unit = {

    if (uniqueDocuments) {
      if (docLogWriter.isEmpty) {
        docLogWriter = Some(new SimpleLogWriter(docLogFile, compressed))
      }
      if (termLogWriter.isEmpty) {
        termLogWriter = Some(new SimpleLogWriter(termLogFile, false))
      }
      val docid = docLogWriter.get.append(doc)
      terms.foreach { term =>
        termLogWriter.get.append(Doc(term + sep + docid.id))
      }
    } else {
      if (termLogWriter.isEmpty) {
        termLogWriter = Some(new SimpleLogWriter(termLogFile, false))
      }
      terms.foreach { term =>
        termLogWriter.get.append(Doc(doc.document + sep + term))
      }
    }
  }

  def add(d: Seq[(Doc, Seq[String])]): Unit = d.foreach(x => add(x._1, x._2))

  def makeIndex(sortBatch: Int, maxOpenFiles: Int) = {
    flushAppenders

    if (!uniqueDocuments) {
      docLogWriter = Some(new SimpleLogWriter(docLogFile, compressed))

    }

    val termReader = if (termLogFile.canRead) Some(new SimpleLogReader(termLogFile, false)) else None

    val unsorted: Iterator[String] = termReader.map(_
      .iterator
      .map(_._2.document)) getOrElse List[String]().iterator

    implicit object MyFormat extends iterator.GroupStringFormat[String, String] {
      def write(t: (String, Seq[String])): String =
        stringReplace(t._1 + sep + t._2.mkString(sep), "\n", " ")
      def read(l: String): (String, Vector[String]) = {
        val spl = fastSplit1WideSeparatorIterator(l, sep.head)
        val k = spl.next
        val rs = spl.toVector
        (k, rs)
      }
    }

    val split: Iterator[(String, String)] = unsorted.map { line =>
      val spl = fastSplit1WideSeparatorIterator(line, sep.head)
      val f1 = spl.next
      val f2 = spl.next
      (f1, f2)
    }

    var termidxCounter = 0L
    val (grouped: Iterator[(String, Long, Seq[Long])], handle) =
      if (uniqueDocuments) {

        val (gr: Iterator[(String, Seq[String])], handle) = iterator.group(split, sortBatch, maxOpenFiles, distinct = false)

        val it = gr.map {
          case (term, docids) =>
            val idx = termidxCounter //zipwithindex is int
            termidxCounter += 1
            Try((term, idx, docids.map(_.toLong))).toOption
        }.filter(_.isDefined).map(_.get)

        (it, handle)

      } else {

        val (gr: Iterator[(String, Seq[String])], handle) = iterator.group(split, sortBatch, maxOpenFiles, distinct = true)

        val sortedByDoc = gr.flatMap {
          case (doc, terms) =>
            val docid: Long = docLogWriter.get.append(Doc(doc)).id
            terms.sorted.map { term =>
              (term, docid.toString)
            }
        }

        val tmp = iterator.iterToDisk(sortedByDoc.map(x => x._1 -> List(x._2)))
        handle.close

        val (sortedByDoc2, handle2) = iterator.iterFromDisk(tmp)(MyFormat)

        val (gr2: Iterator[(String, Seq[String])], handle3) = iterator.group(sortedByDoc2.map(x => x._1 -> x._2.head), sortBatch, maxOpenFiles, distinct = false)

        val it = gr2.map {
          case (term, docids) =>
            val idx = termidxCounter //zipwithindex is int
            termidxCounter += 1
            Try((term, idx, docids.map(_.toLong))).toOption
        }.filter(_.isDefined).map(_.get)

        (it, new Closeable { def close = { handle2.close; handle3.close } })

      }

    val csrCells = new File(f.getAbsolutePath + ".csr.cells")
    val csrRows = new File(f.getAbsolutePath + ".csr.rows")
    val csrCols = new File(f.getAbsolutePath + ".csr.cols")
    val sparkeyFile = new File(f.getAbsolutePath + ".sparkey")

    Sparkey.getLogFile(sparkeyFile).delete
    Sparkey.getIndexFile(sparkeyFile).delete
    csrCells.delete
    csrRows.delete
    csrCols.delete

    val csr = new CSRWriter(csrCells, csrCols, csrRows)

    val sparkey = Sparkey.createNew(sparkeyFile)

    val buffer = Array.ofDim[Byte](8)
    val bb = java.nio.ByteBuffer.wrap(buffer).order(java.nio.ByteOrder.LITTLE_ENDIAN)
    grouped.foreach {
      case (term, termidx, docidx) =>
        csr.appendSortedRow(termidx, docidx.toVector.sorted.map(x => x -> 1.0))
        bb.rewind
        bb.putLong(termidx)
        sparkey.put(term.getBytes("UTF-8"), buffer)

    }

    csr.close
    sparkey.flush
    sparkey.writeHash
    termReader.foreach(_.close)
    sparkey.close
    handle.close
    docLogWriter.foreach(_.flush)

    (csrCells, csrRows, csrCols) -> sparkeyFile
  }

  def stringReplace(source: String, target: String, replacement: String): String = {

    val out = new StringBuilder(source.size * 2)
    // last index we need to check for match
    val lastIdx = source.length - target.length

    // check for match at given index, at char offset along target
    @scala.annotation.tailrec
    def matches(idx: Int, offset: Int): Boolean =
      if (offset >= target.length)
        true
      else if (target.charAt(offset) == source.charAt(idx + offset).toLower)
        matches(idx, offset + 1)
      else false

    // search source and append to builder
    @scala.annotation.tailrec
    def search(idx: Int): Unit =
      if (idx > lastIdx)
        out.append(source.substring(idx))
      else if (matches(idx, 0)) {
        out.append(replacement)
        search(idx + target.length)
      } else {
        out.append(source.charAt(idx))
        search(idx + 1)
      }

    search(0)
    out.toString
  }

}
