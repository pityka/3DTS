case class CumulativeRelativeFrequencies(
    cumulative: IndexedSeq[(Double, Double)]) {

  /**
    * Binary search
    *
    * Performs binary (logarithmic search) on an ordered sequence.
    * @param a is a sequence of ordered elements.
    * @param less is a function similar to <
    * @param greater is a function similar to >
    * @param mapper comparison is made on the mapped values
    * @return an either where right contains the found element with the index and left contains the indices where the search gave up
    */
  def binarySearch[T, A](
      a: IndexedSeq[A],
      less: T => Boolean,
      greater: T => Boolean,
      mapper: A => T
  ): Either[(Int, Int), (Int, A)] = {
    def recurse(low: Int, high: Int): Either[(Int, Int), (Int, A)] =
      (low + high) / 2 match {
        case _ if high < low                      => scala.util.Left(high, low)
        case mid if greater(mapper(a.apply(mid))) => recurse(low, mid - 1)
        case mid if less(mapper(a.apply(mid)))    => recurse(mid + 1, high)
        case mid                                  => scala.util.Right((mid, a.apply(mid)))
      }
    recurse(0, a.size - 1)
  }

  /**
    * Binary search
    *
    * Performs binary (logarithmic search) on an ordered sequence.
    * @param a is a sequence of ordered elements.
    * @param less is a function similar to <
    * @param greater is a function similar to >
    * @return an either where right contains the found element with the index and left contains the indices where the search gave up
    */
  def binarySearch[T](
      a: IndexedSeq[T],
      less: T => Boolean,
      greater: T => Boolean
  ): Either[(Int, Int), (Int, T)] =
    binarySearch[T, T](a, less, greater, (x: T) => x)

  /**
    * Binary search
    *
    * Performs binary (logarithmic search) on an ordered sequence.
    * @param a is a sequence of ordered elements.
    * @param mapper comparison is made on the mapped values
    * @return an either where right contains the found element with the index and left contains the indices where the search gave up
    */
  def binarySearch[T, A](a: IndexedSeq[A], v: T, mapper: A => T)(
      implicit ordering: Ordering[T]): Either[(Int, Int), (Int, A)] = {
    val less = (x: T) => ordering.lt(x, v)
    val greater = (x: T) => ordering.gt(x, v)
    binarySearch(a, less, greater, mapper)
  }

  /**
    * Binary search
    *
    * Performs binary (logarithmic search) on an ordered sequence.
    * @param a is a sequence of ordered elements.
    * @return an either where right contains the found element with the index and left contains the indices where the search gave up
    */
  def binarySearch[T](a: IndexedSeq[T], v: T)(
      implicit ordering: Ordering[T]): Either[(Int, Int), (Int, T)] = {
    val less = (x: T) => ordering.lt(x, v)
    val greater = (x: T) => ordering.gt(x, v)
    binarySearch(a, less, greater, (x: T) => x)
  }

  def cdf(loc: Double): Double =
    binarySearch[(Double, Double)](
      cumulative,
      (x: (Double, Double)) => x._1 < loc,
      (x: (Double, Double)) => x._1 > loc
    ) match {
      case scala.util.Right((idx, elem))              => cumulative(idx)._2
      case scala.util.Left((idx1, idx2)) if idx1 >= 0 => cumulative(idx1)._2
      case scala.util.Left((idx1, idx2)) if idx1 < 0  => 0.0
    }
}

object CumulativeRelativeFrequencies {

  def empirical(data: Seq[Double]) = {
    val sorted = data.sorted
    val counts = sorted.foldLeft((sorted.head, 0, Map[Double, Int]())) {
      case ((last, count, map), elem) =>
        if (elem == last) {
          val nl = map.get(elem) match {
            case None    => map.updated(elem, count + 1)
            case Some(x) => map.updated(elem, x + 1)
          }
          (elem, count + 1, nl)
        } else {
          (elem, count + 1, map.updated(elem, count + 1))
        }
    }
    val cumulative: IndexedSeq[(Double, Double)] = counts._3
      .map(x => x._1 -> x._2.toDouble / data.size)
      .toSeq
      .sortBy(_._1)
      .toIndexedSeq
    CumulativeRelativeFrequencies(cumulative)

  }
}
