package index2
import java.io._

package object iterator {

  def merge[T](iter1: Iterator[T], iter2: Iterator[T])(implicit o: Ordering[T]): Iterator[T] = new Iterator[T] {

    var head1: Option[T] = if (iter1.hasNext) Some(iter1.next) else None
    var head2: Option[T] = if (iter2.hasNext) Some(iter2.next) else None

    def forward1 = {
      val r = head1.get
      head1 = if (iter1.hasNext) Some(iter1.next) else None
      r
    }

    def forward2 = {
      val r = head2.get
      head2 = if (iter2.hasNext) Some(iter2.next) else None
      r
    }

    def hasNext = head1.isDefined || head2.isDefined

    def next = if (head1.isEmpty && head2.isEmpty) Iterator.empty.next
    else if (head1.isEmpty) forward2
    else if (head2.isEmpty) forward1
    else if (o.lt(head1.get, head2.get)) forward1
    else forward2

  }

  def assertOrder[T](it: Iterator[T], msg: String)(implicit o: Ordering[T]): Iterator[T] = new Iterator[T] {

    var peek: Option[T] = if (it.hasNext) Some(it.next) else None

    var i = 0L

    def hasNext = peek.isDefined

    def next = {
      i += 1
      if (peek.isEmpty) Iterator.empty.next
      else {
        val r = peek.get

        peek = if (it.hasNext) Some(it.next) else None
        peek.foreach { p =>
          assert(o.lteq(r, p), s"Assert order failed at line $i $r not less than $p " + msg)
        }

        r
      }
    }

  }

  def mergeIterators[T](its: Seq[Iterator[T]])(implicit o: Ordering[T]): Iterator[T] =
    if (its.isEmpty) List[T]().iterator
    else assertOrder(its.zipWithIndex.map(i => assertOrder(i._1, "Input iterator: " + i._2)).reduce(merge(_, _)), "merged iter")

  def spansByProjectionEquality[T, K](iter: Iterator[T])(p: T => K): Iterator[Vector[T]] = new SpanIterator(iter, p)

  trait GroupStringFormat[K, R] {
    def write(t: (K, Seq[R])): String
    def read(l: String): (K, Vector[R])
  }

  def iterToDisk[K, R](iter: Iterator[(K, Seq[R])])(implicit format: GroupStringFormat[K, R]) = {
    val tmp = File.createTempFile("sort", ".data")
    val writer = new SimpleLogWriter(tmp, false)
    iter.foreach { x =>
      writer.append(Doc(format.write(x)))
    }
    writer.close
    tmp
  }

  def iterFromDisk[K, R](f: File)(implicit format: GroupStringFormat[K, R]) = {
    val reader = new SimpleLogReader(f, false)
    val it = reader.iterator.map { doc =>
      format.read(doc._2.document)
    }
    (it, reader)
  }

  def group[K, R](iterator: Iterator[(K, R)], batch: Int, maxOpenFiles: Int = 100, distinct: Boolean = false)(implicit o: Ordering[K], format: GroupStringFormat[K, R]): (Iterator[(K, Seq[R])], Closeable) = {

    val ord = Ordering.by((t: (K, Seq[R])) => t._1)
    val ord2 = Ordering.by((t: (K, Vector[R])) => t._1)

    val proj = (t: (K, R)) => t._1

    val batches: List[File] = iterator.grouped(batch).map { list =>

      val sorted: Iterator[Seq[(K, R)]] =
        spansByProjectionEquality(list.sortBy(_._1).iterator)(proj)

      val separated: Iterator[(K, Seq[R])] = sorted.map { seq =>
        val k: K = seq.head._1
        val rs: Seq[R] = if (distinct) seq.map(_._2).distinct else seq.map(_._2)
        (k, rs)
      }

      iterToDisk(assertOrder(separated, "batch")(ord))
    }.toList

    println("Total batches: " + batches.size)
    batches.foreach(println)

    def mergeBatches(batches: List[File]): (Iterator[(K, Seq[R])], Closeable) = {
      val list = batches.map {
        case file =>
          val (it: Iterator[(K, Seq[R])], source) = iterFromDisk(file)
          (assertOrder(it, "file: " + file)(ord), source, file)
      }

      val iterators = list.map(_._1)

      val merged: Iterator[(K, Seq[R])] = mergeIterators(iterators)(ord)

      val g: Iterator[(K, Seq[R])] = spansByProjectionEquality(merged)(_._1).map { seq =>
        val k = seq.head._1
        val rs = seq.flatMap(_._2)
        val d = if (distinct) rs.distinct else rs
        (k, d)
      }

      (g, new Closeable { def close = list.foreach { x => x._2.close; x._3.delete } })
    }

    val groupedBatches = batches.grouped(maxOpenFiles).toList

    if (groupedBatches.size == 1) mergeBatches(groupedBatches.head)
    else {
      val batch2 = groupedBatches.map { files =>
        val (iter, handle) = mergeBatches(files)
        val tmp = iterToDisk(iter)
        handle.close
        println("Tier2 batch: " + tmp)
        tmp
      }
      mergeBatches(batch2)
    }

  }

}
