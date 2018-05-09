package sd

import upickle.default.{Writer, Reader}
import java.io.{Closeable, File}

package object iterator {
  def merge[T](iter1: Iterator[T], iter2: Iterator[T])(
      implicit o: Ordering[T]): Iterator[T] = new Iterator[T] {

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

    def next =
      if (head1.isEmpty && head2.isEmpty) Iterator.empty.next
      else if (head1.isEmpty) forward2
      else if (head2.isEmpty) forward1
      else if (o.lt(head1.get, head2.get)) forward1
      else forward2

  }

  def outerJoinSorted[T](_iter1: Iterator[T], _iter2: Iterator[T])(
      implicit o: Ordering[T]): Iterator[(Option[T], Option[T])] =
    new Iterator[(Option[T], Option[T])] {

      val iter1 = assertOrder(_iter1)
      val iter2 = assertOrder(_iter2)

      var head1: Option[T] = if (iter1.hasNext) Some(iter1.next) else None
      var head2: Option[T] = if (iter2.hasNext) Some(iter2.next) else None

      var list1: List[T] = Nil
      var list2: List[T] = Nil

      var copylist2: List[T] = Nil

      def forwardCopy = {
        val h = copylist2.head
        copylist2 = copylist2.tail
        h
      }

      def forward1 = {
        head1 = if (iter1.hasNext) Some(iter1.next) else None
      }

      def forward2 = {
        head2 = if (iter2.hasNext) Some(iter2.next) else None
      }

      def fill1 =
        while (head1.isDefined && (list1.isEmpty || o.equiv(head1.get,
                                                            list1.head))) {
          list1 = head1.get :: list1
          forward1
        }

      def fill2 = {
        while (head2.isDefined && (list2.isEmpty || o.equiv(head2.get,
                                                            list2.head))) {
          list2 = head2.get :: list2
          forward2
        }
        copylist2 = list2
      }

      def forwardList1 = {
        val h = list1.head
        list1 = list1.tail
        if (list1.isEmpty) {
          fill1
        }
        h
      }

      def forwardList2 = {
        val h = list2.head
        list2 = list2.tail
        if (list2.isEmpty) {
          fill2
        }
        h
      }

      fill1
      fill2

      def hasNext = !list1.isEmpty || !list2.isEmpty

      def next =
        if (list1.isEmpty && list2.isEmpty) Iterator.empty.next
        else if (list1.isEmpty) (None, Some(forwardList2))
        else if (list2.isEmpty) (Some(forwardList1), None)
        else if (o.lt(list1.head, list2.head)) (Some(forwardList1), None)
        else if (o.equiv(list1.head, list2.head)) {

          val r = (Some(list1.head), Some(forwardCopy))

          if (copylist2.isEmpty) {
            copylist2 = list2
            list1 = list1.tail
            if (list1.isEmpty) {
              fill1
              if (!list1.isEmpty && !copylist2.isEmpty && !o.equiv(
                    list1.head,
                    copylist2.head)) {
                list2 = Nil
                fill2
              } else if (list1.isEmpty) {
                list2 = Nil
                fill2
              }

            }
          }

          r

        } else (None, Some(forwardList2))

    }

  def assertOrder[T](it: Iterator[T])(implicit o: Ordering[T]): Iterator[T] =
    new Iterator[T] {

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
            assert(o.lteq(r, p),
                   s"Assert order failed at line $i $r not less than $p")
          }

          r
        }
      }

    }

  def mergeIterators[T](its: Seq[Iterator[T]])(
      implicit o: Ordering[T]): Iterator[T] =
    assertOrder(its.reduce(merge(_, _)))

  def sortAsJson[T](iterator: Iterator[T], batch: Int)(
      implicit o: Ordering[T],
      pickler: Writer[T],
      unpickler: Reader[T]): (Iterator[T], Closeable) = {

    def dump(it: Iterator[T]): File = {
      fileutils.openFileWriter { writer =>
        it.foreach { t =>
          writer.write(upickle.default.write(t))
          writer.write("\n")
        }
      }._1
    }

    def mergeBatches(list: List[File]): (Iterator[T], Closeable) =
      if (list.size < 25)
        mergeBatches1(list)
      else
        mergeBatches(
          list
            .grouped(25)
            .map { group =>
              val (it, closeable) = mergeBatches1(group)
              val tmp = dump(it)
              closeable.close
              tmp
            }
            .toList)

    def mergeBatches1(sortedFiles: List[File]): (Iterator[T], Closeable) = {
      val list = sortedFiles.map {
        case (file) =>
          val source = fileutils.createSource(file)
          val it: Iterator[T] = source.getLines map (line =>
            upickle.default
              .read[T](line))
          (it, source, file)
      }

      val iterators = list.map(_._1)

      (mergeIterators(iterators), new Closeable {
        def close = list.foreach { x =>
          x._2.close; x._3.delete
        }
      })
    }

    val sortedFiles: List[File] = iterator
      .grouped(batch)
      .map { list =>
        dump(list.sorted.iterator)
      }
      .toList

    mergeBatches(sortedFiles)

  }

  def spansByProjectionEquality[T, K](iter: Iterator[T])(
      p: T => K): Iterator[Seq[T]] = new SpanIterator(iter, p)

}

final class SpanIterator[A, T](iter: Iterator[A], p: A => T)
    extends Iterator[Seq[A]] {

  val buffer = collection.mutable.ArrayBuffer[A]()

  var currentPredicateValue: T = _

  var fwRead: Option[A] = if (iter.hasNext) Some(iter.next) else None

  fwRead.foreach { x =>
    currentPredicateValue = p(x)
  }

  def fw =
    if (iter.hasNext) {
      fwRead = Some(iter.next)
    } else {
      fwRead = None
    }

  def hasNext = fwRead.isDefined

  def next = {

    while (fwRead.isDefined && p(fwRead.get) == currentPredicateValue) {

      buffer.append(fwRead.get)

      fw
    }

    val ret = buffer.toList

    buffer.clear

    fwRead.foreach { x =>
      currentPredicateValue = p(x)
    }

    ret

  }

}
