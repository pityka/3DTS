package index2

import org.scalatest._
import java.io._
import iterator._

class IndexSpec extends FunSpec with Matchers with BeforeAndAfterAll {

  describe("iterator") {
    // it("remove dups") {
    //   iterator.eliminateDuplicates(List(1, 1, 1, 2, 3).iterator).toList should equal(List(1, 2, 3))
    //   iterator.eliminateDuplicates(List(1, 2, 3, 3, 3).iterator).toList should equal(List(1, 2, 3))
    //   iterator.eliminateDuplicates(List(1, 2, 3).iterator).toList should equal(List(1, 2, 3))
    //   iterator.eliminateDuplicates(List(1, 2, 2, 2, 3).iterator).toList should equal(List(1, 2, 3))
    //   iterator.eliminateDuplicates(List(1, 1, 1, 2, 2, 2, 3, 3, 3).iterator).toList should equal(List(1, 2, 3))
    //   iterator.eliminateDuplicates(List(1, 1, 1, 2, 3, 3, 3).iterator).toList should equal(List(1, 2, 3))
    // }
    it("group1") {
      implicit val format = new GroupStringFormat[Int, Char] {
        def write(t: (Int, Seq[Char])): String = t._1 + ":" + t._2.mkString
        def read(l: String) = l.split(":")(0).toInt -> l.split(":")(1).toVector
      }
      val testdata = List(
        1 -> 'a',
        2 -> 'b',
        10 -> 'a',
        10 -> 'd',
        2 -> 'c',
        1 -> 'a',
        1 -> 'a',
        2 -> 'b'
      )
      iterator.group(testdata.iterator, 3)._1.toList.map(x => x._1 -> x._2.sorted) should equal(testdata.groupBy(_._1).toList.map(x => x._1 -> x._2.map(_._2).sorted).sortBy(_._1))

    }
    it("group2") {
      implicit val format = new GroupStringFormat[Int, Char] {
        def write(t: (Int, Seq[Char])): String = t._1 + ":" + t._2.mkString
        def read(l: String) = l.split(":")(0).toInt -> l.split(":")(1).toVector
      }
      val testdata = List(
        1 -> 'a',
        2 -> 'b',
        10 -> 'a',
        10 -> 'd',
        2 -> 'c',
        1 -> 'a',
        1 -> 'a',
        2 -> 'b',
        -1 -> 'x'
      )
      iterator.group(testdata.iterator, 1)._1.toList.map(x => x._1 -> x._2.sorted) should equal(testdata.groupBy(_._1).toList.map(x => x._1 -> x._2.map(_._2).sorted).sortBy(_._1))

    }
    it("group3") {
      implicit val format = new GroupStringFormat[Int, Char] {
        def write(t: (Int, Seq[Char])): String = t._1 + ":" + t._2.mkString
        def read(l: String) = l.split(":")(0).toInt -> l.split(":")(1).toVector
      }
      val testdata = List(
        1 -> 'a',
        2 -> 'b',
        10 -> 'a',
        10 -> 'd',
        2 -> 'c',
        1 -> 'a',
        1 -> 'a',
        2 -> 'b',
        -1 -> 'x'
      )
      iterator.group(testdata.iterator, 100)._1.toList.map(x => x._1 -> x._2.sorted) should equal(testdata.groupBy(_._1).toList.map(x => x._1 -> x._2.map(_._2).sorted).sortBy(_._1))

    }
  }

  describe("test indexer") {
    it("empty") {
      val file = File.createTempFile("dfsdf", "dfs")
      val writer = new IndexWriter(file, true)
      writer.close
      writer.makeIndex(3, 2)
      val reader = new IndexReader(file)
      reader.getDocIds("dfd") should equal(Vector())
    }
    it("1") {
      val file = File.createTempFile("dfsdf", "dfs")
      val writer = new IndexWriter(file, true)
      writer.add(Doc("d1"), Seq("t1", "t2"))
      writer.close
      writer.makeIndex(100000, 1)
      val reader = new IndexReader(file)
      reader.getDocIds("dfd") should equal(Vector())
      reader.getDocIds("t1") should equal(Vector(DocId(16L)))
      reader.getDocIds("t2") should equal(Vector(DocId(16L)))
      reader.getDocIds("t1").flatMap(reader.getDoc) should equal(Vector(Doc("d1")))
    }
    it("2") {
      val file = File.createTempFile("dfsdf", "dfs")
      val writer = new IndexWriter(file, true)
      writer.add(Doc("d1"), Seq("t1", "t2"))
      writer.add(Doc("d2"), Seq("t1", "t3"))
      writer.close
      writer.makeIndex(100000, 1)
      val reader = new IndexReader(file)
      reader.getDocIds("dfd") should equal(Vector())
      reader.getDocIds("t1") should equal(Vector(DocId(16L), DocId(22L)))
      reader.getDocIds("t2") should equal(Vector(DocId(16L)))
      reader.getDocIds("t3") should equal(Vector(DocId(22L)))
      reader.getDocIds("t1").flatMap(reader.getDoc) should equal(Vector(Doc("d1"), Doc("d2")))
      reader.getDocIds("t2").flatMap(reader.getDoc) should equal(Vector(Doc("d1")))
      reader.getDocIds("t3").flatMap(reader.getDoc) should equal(Vector(Doc("d2")))
      reader.getDocCount("t1") should equal(2)
      reader.getDocCount("t3") should equal(1)
      reader.getDocCount("t2") should equal(1)
    }
    it("3, duplicate docs") {
      val file = File.createTempFile("dfsdf", "dfs")
      val writer = new IndexWriter(file, false)
      writer.add(Doc("d1"), Seq("t1", "t2"))
      writer.add(Doc("d2"), Seq("t1", "t3"))
      writer.add(Doc("d1"), Seq("t1", "t2"))
      writer.makeIndex(1, 1)
      val reader = new IndexReader(file)
      reader.getDocIds("dfd") should equal(Vector())
      reader.getDocIds("t1") should equal(Vector(DocId(16L), DocId(22L)))
      reader.getDocIds("t2") should equal(Vector(DocId(16L)))
      reader.getDocIds("t3") should equal(Vector(DocId(22L)))
      reader.getDocIds("t1").flatMap(reader.getDoc) should equal(Vector(Doc("d1"), Doc("d2")))
      reader.getDocIds("t2").flatMap(reader.getDoc) should equal(Vector(Doc("d1")))
      reader.getDocIds("t3").flatMap(reader.getDoc) should equal(Vector(Doc("d2")))
      reader.getDocCount("t1") should equal(2)
      reader.getDocCount("t3") should equal(1)
      reader.getDocCount("t2") should equal(1)
    }
    it("4, duplicate docs without taking care") {
      val file = File.createTempFile("dfsdf", "dfs")
      val writer = new IndexWriter(file, true)
      writer.add(Doc("d1"), Seq("t1", "t2"))
      writer.add(Doc("d2"), Seq("t1", "t3"))
      writer.add(Doc("d1"), Seq("t1", "t2"))
      writer.makeIndex(1, 1)
      val reader = new IndexReader(file)
      reader.getDocIds("dfd") should equal(Vector())
      reader.getDocIds("t1") should equal(Vector(DocId(16L), DocId(22L), DocId(28L)))
      reader.getDocIds("t2") should equal(Vector(DocId(16L), DocId(28L)))
      reader.getDocIds("t3") should equal(Vector(DocId(22L)))
      reader.getDocIds("t1").flatMap(reader.getDoc) should equal(Vector(Doc("d1"), Doc("d2"), Doc("d1")))
      reader.getDocIds("t2").flatMap(reader.getDoc) should equal(Vector(Doc("d1"), Doc("d1")))
      reader.getDocIds("t3").flatMap(reader.getDoc) should equal(Vector(Doc("d2")))
      reader.getDocCount("t1") should equal(3)
      reader.getDocCount("t3") should equal(1)
      reader.getDocCount("t2") should equal(2)
    }
  }

  describe("test csr") {
    it("1") {
      val cells = new File("test1.cells")
      val ridx = new File("test1.ridx")
      val cidx = new File("test1.cidx")
      cells.delete
      ridx.delete
      cidx.delete
      val writer = new CSRWriter(cells, cidx, ridx)
      writer.appendSortedRow(0L, Vector(0L -> 1, 1L -> 2))
      writer.close
      val reader = new CSRReader(cells, cidx, ridx)
      reader.readColIdx(0L).toVector should equal(Vector(0L, 1L))
      reader.readValues(0L).toVector should equal(Vector(1, 2))
    }
    it("2") {
      val cells = new File("test2.cells")
      val ridx = new File("test2.ridx")
      val cidx = new File("test2.cidx")
      cells.delete
      ridx.delete
      cidx.delete
      val writer = new CSRWriter(cells, cidx, ridx)
      // writer.appendSortedRow(0L, Vector(0L -> 1, 1L -> 2))
      writer.close
      val reader = new CSRReader(cells, cidx, ridx)
      reader.readColIdx(0L).toVector should equal(Vector())
      reader.readValues(0L).toVector should equal(Vector())
    }
    it("3") {
      val cells = new File("test3.cells")
      val ridx = new File("test3.ridx")
      val cidx = new File("test3.cidx")
      cells.delete
      ridx.delete
      cidx.delete
      val writer = new CSRWriter(cells, cidx, ridx)
      writer.appendSortedRow(0L, Vector(0L -> 1, 3L -> 2))
      writer.close
      val reader = new CSRReader(cells, cidx, ridx)
      reader.readColIdx(0L).toVector should equal(Vector(0L, 3L))
      reader.readValues(0L).toVector should equal(Vector(1, 2))
    }
    it("4") {
      val cells = new File("test4.cells")
      val ridx = new File("test4.ridx")
      val cidx = new File("test4.cidx")
      cells.delete
      ridx.delete
      cidx.delete
      val writer = new CSRWriter(cells, cidx, ridx)
      writer.appendSortedRow(2L, Vector(0L -> 1, 3L -> 2))
      writer.close
      val reader = new CSRReader(cells, cidx, ridx)
      reader.readColIdx(0L).toVector should equal(Vector())
      reader.readColIdx(1L).toVector should equal(Vector())
      reader.readColIdx(2L).toVector should equal(Vector(0L, 3L))
      reader.readColIdx(3L).toVector should equal(Vector())

      reader.readValues(0L).toVector should equal(Vector())
      reader.readValues(1L).toVector should equal(Vector())
      reader.readValues(2L).toVector should equal(Vector(1, 2))
      reader.readValues(3L).toVector should equal(Vector())

      reader.readLength(0L) should equal(0)
      reader.readLength(1L) should equal(0)
      reader.readLength(2L) should equal(2)
      reader.readLength(3L) should equal(0)
    }

    it("5") {
      val cells = new File("test5.cells")
      val ridx = new File("test5.ridx")
      val cidx = new File("test5.cidx")
      cells.delete
      ridx.delete
      cidx.delete
      val writer = new CSRWriter(cells, cidx, ridx)
      writer.appendSortedRow(2L, Vector(0L -> 1, 3L -> 2))
      writer.appendSortedRow(4L, Vector(2L -> 3, 4L -> 5))
      writer.close
      val reader = new CSRReader(cells, cidx, ridx)
      reader.readColIdx(0L).toVector should equal(Vector())
      reader.readColIdx(1L).toVector should equal(Vector())
      reader.readColIdx(2L).toVector should equal(Vector(0L, 3L))
      reader.readColIdx(3L).toVector should equal(Vector())
      reader.readColIdx(4L).toVector should equal(Vector(2L, 4L))

      reader.readValues(0L).toVector should equal(Vector())
      reader.readValues(1L).toVector should equal(Vector())
      reader.readValues(2L).toVector should equal(Vector(1, 2))
      reader.readValues(3L).toVector should equal(Vector())
      reader.readValues(4L).toVector should equal(Vector(3, 5))

      reader.readLength(0L) should equal(0)
      reader.readLength(1L) should equal(0)
      reader.readLength(2L) should equal(2)
      reader.readLength(3L) should equal(0)
    }

    it(" intersect 1") {

      val cells = new File("test6.cells")
      val ridx = new File("test6.ridx")
      val cidx = new File("test6.cidx")
      cells.delete
      ridx.delete
      cidx.delete
      val writer = new CSRWriter(cells, cidx, ridx)
      writer.appendSortedRow(2L, Vector(0L -> 1, 3L -> 2))
      writer.appendSortedRow(4L, Vector(2L -> 3, 4L -> 5, 5L -> 6, 8L -> 9, 10L -> 10, 11L -> 1, 12L -> 13, 13L -> 14, 16L -> 16))
      writer.appendSortedRow(5L, Vector())
      writer.appendSortedRow(6L, Vector(0L -> 1))
      writer.close
      val reader = new CSRReader(cells, cidx, ridx)
      reader.readColIdx(0L).toVector should equal(Vector())
      reader.readColIdx(1L).toVector should equal(Vector())
      reader.readColIdx(2L).toVector should equal(Vector(0L, 3L))
      reader.readColIdx(3L).toVector should equal(Vector())
      reader.readColIdx(4L).toVector should equal(Vector(2L, 4L, 5L, 8L, 10L, 11L, 12L, 13L, 16L))
      reader.readColIdx(5L).toVector should equal(Vector())
      reader.readColIdx(6L).toVector should equal(Vector(0L))

      reader.readValues(0L).toVector should equal(Vector())
      reader.readValues(1L).toVector should equal(Vector())
      reader.readValues(2L).toVector should equal(Vector(1, 2))
      reader.readValues(3L).toVector should equal(Vector())
      reader.readValues(4L).toVector should equal(Vector(3, 5, 6, 9, 10, 1, 13, 14, 16))
      reader.readValues(5L).toVector should equal(Vector())
      reader.readValues(6L).toVector should equal(Vector(1))

      reader.readLength(0L) should equal(0)
      reader.readLength(1L) should equal(0)
      reader.readLength(2L) should equal(2)
      reader.readLength(3L) should equal(0)
      reader.readLength(4L) should equal(9)
      reader.readLength(5L) should equal(0)
      reader.readLength(6L) should equal(1)

      reader.readColIdxIntersect(4L, Array(2L)).toVector should equal(Vector(2L))
      reader.readColIdxIntersect(4L, Array(16L)).toVector should equal(Vector(16L))
      reader.readColIdxIntersect(4L, Array(8L)).toVector should equal(Vector(8L))
      reader.readColIdxIntersect(4L, Array(8L, 9L)).toVector should equal(Vector(8L))
      reader.readColIdxIntersect(4L, Array(1L)).toVector should equal(Vector())
      reader.readColIdxIntersect(4L, Array(17L)).toVector should equal(Vector())
      reader.readColIdxIntersect(4L, Array(15L)).toVector should equal(Vector())
      reader.readColIdxIntersect(4L, Array(14L)).toVector should equal(Vector())
      reader.readColIdxIntersect(4L, Array(3L)).toVector should equal(Vector())
      reader.readColIdxIntersect(4L, Array(4L)).toVector should equal(Vector(4L))
      reader.readColIdxIntersect(4L, 0 to 20 map (_.toLong) toArray).toVector should equal(Vector(2L, 4L, 5L, 8L, 10L, 11L, 12L, 13L, 16L))
      reader.readColIdxIntersect(4L, 0 to 10 map (_.toLong) toArray).toVector should equal(Vector(2L, 4L, 5L, 8L, 10L))
      reader.readColIdxIntersect(4L, 10 to 20 map (_.toLong) toArray).toVector should equal(Vector(10L, 11L, 12L, 13L, 16L))

      reader.readColIdxIntersect(4L, Array(2L), true).toVector should equal(Vector(2L))
      reader.readColIdxIntersect(4L, Array(16L), true).toVector should equal(Vector(16L))
      reader.readColIdxIntersect(4L, Array(8L), true).toVector should equal(Vector(8L))
      reader.readColIdxIntersect(4L, Array(8L, 9L), true).toVector should equal(Vector(8L))
      reader.readColIdxIntersect(4L, Array(1L), true).toVector should equal(Vector())
      reader.readColIdxIntersect(4L, Array(17L), true).toVector should equal(Vector())
      reader.readColIdxIntersect(4L, Array(15L), true).toVector should equal(Vector())
      reader.readColIdxIntersect(4L, Array(14L), true).toVector should equal(Vector())
      reader.readColIdxIntersect(4L, Array(3L), true).toVector should equal(Vector())
      reader.readColIdxIntersect(4L, Array(4L), true).toVector should equal(Vector(4L))
      reader.readColIdxIntersect(4L, 0 to 20 map (_.toLong) toArray, true).toVector should equal(Vector(2L, 4L, 5L, 8L, 10L, 11L, 12L, 13L, 16L))
      reader.readColIdxIntersect(4L, 0 to 10 map (_.toLong) toArray, true).toVector should equal(Vector(2L, 4L, 5L, 8L, 10L))
      reader.readColIdxIntersect(4L, 10 to 20 map (_.toLong) toArray, true).toVector should equal(Vector(10L, 11L, 12L, 13L, 16L))

    }
  }

}
