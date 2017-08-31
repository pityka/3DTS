package index2

import java.io._
import scala.collection.concurrent.{TrieMap => CTrieMap}
case class Table(name: String, uniqueDocuments: Boolean, compressedDocuments: Boolean)
case class TableManager(base: File) {

  base.mkdirs

  val writers = new CTrieMap[Table, IndexWriter]()
  val readers = new CTrieMap[Table, IndexReader]()

  val history = new File(base.getAbsolutePath, "_history")

  def allWriters = writers.values.toSeq

  def writer(t: Table): IndexWriter =
    writers.getOrElseUpdate(
      t,
      {
        val f = new File(base, t.name)
        new IndexWriter(f, t.uniqueDocuments, t.compressedDocuments)
      }
    )

  def reader(t: Table): IndexReader =
    readers.getOrElseUpdate(
      t,
      {
        val f = new File(base, t.name)
        new IndexReader(f, t.compressedDocuments)
      }
    )
    
  private def useResource[A <: { def close(): Unit }, B](param: A)(f: A => B): B =
   try { f(param) } finally {
     param.close()
   }

  def appendHistory(s: String) =
    useResource(new BufferedWriter(new FileWriter(history, true)))(_.write(s + "\n"))

  def readHistory: Set[String] = if (history.canRead) {
    val s = scala.io.Source.fromFile(history)
    val r = s.getLines.toSet
    s.close
    r
  } else Set()

}
