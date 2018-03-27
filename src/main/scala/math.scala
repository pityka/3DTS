object MathHelpers {

  def makeColor(s: Seq[Double]) = {
    val cdf = cumulative(s)
    val colormap = RedBlue(min = 0d, max = 1d, mid = 0.5)
    (s: Double) =>
      {
        val c = colormap.apply(cdf.cdf(s))
        MyColor(c.r, c.g, c.b)
      }
  }

  def value2Color(s: Double) = {
    val colormap = RedBlue(min = 0d, max = 1d, mid = 0.5)

    val c = colormap.apply(s)
    MyColor(c.r, c.g, c.b)

  }

  def bisect(x0: Double,
             x1: Double,
             start: Double,
             maxit: Int,
             rel: Double,
             abs: Double)(f: Double => Double): Double = {
    var i = 0
    var y0 = x0
    var y1 = x1
    var fc = f(start)
    var f0 = f(y0)
    var c = start
    def absmet = math.abs(y1 - y0) < abs
    def relmet = math.abs((y1 - y0) / y0) < rel
    while (i < maxit && !absmet && !relmet && fc != 0.0) {
      c = (y1 + y0) / 2
      fc = f(c)
      f0 = f(y0)
      if (math.signum(fc) == math.signum(f0)) {
        y0 = c
      } else {
        y1 = c
      }
      i += 1
    }
    c
  }

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

  def cumulative(data: Seq[Double]) = {
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

  def bayes(lik: Double => Double, threshold: Double) = {

    val width = 0.9
    val denom = NewtonCotes.simpson(lik, 0.0, 1.0, 100)
    val maxLoc = (0 to 100 maxBy (x => lik(x / 100d))) / 100d

    def post(x: Double) = lik(x) / denom
    def area(a: Double, b: Double, n: Int) = NewtonCotes.simpson(post, a, b, n)

    def right(a: Double, b: Double, grid: Int) = {
      (0 until grid).scanLeft(0.0) {
        case (acc, i) =>
          val x1 = a + i / grid.toDouble * (b - a)
          val x2 = a + (i + 1) / grid.toDouble * (b - a)
          acc + area(x1, x2, 4)
      }
    }
    def left(a: Double, b: Double, grid: Int) = {
      (0 until grid).scanLeft(0.0) {
        case (acc, i) =>
          val x1 = b - i / grid.toDouble * (b - a)
          val x2 = b - (i + 1) / grid.toDouble * (b - a)
          acc + area(x2, x1, 4)
      }

    }
    val grid = 100
    val r = right(maxLoc, 1.0, grid)
    val l = left(0.0, maxLoc, grid)

    val (uArea1, uIdx1) =
      r.zipWithIndex.minBy(x => math.pow(x._1 - width / 2, 2))
    val (lArea1, lIdx1) =
      l.zipWithIndex.minBy(x => math.pow(x._1 - width / 2, 2))

    val smallerArea = math.min(uArea1, lArea1)
    val target = width - smallerArea
    val idx3 =
      if (uArea1 < lArea1)
        l.zipWithIndex.minBy(x => math.pow(x._1 - target, 2))._2
      else r.zipWithIndex.minBy(x => math.pow(x._1 - target, 2))._2

    val u =
      if (uArea1 < lArea1) maxLoc + uIdx1 / grid.toDouble * (1.0 - maxLoc)
      else maxLoc + idx3 / grid.toDouble * (1.0 - maxLoc)

    val ll =
      if (lArea1 < uArea1) maxLoc - lIdx1 / grid.toDouble * (maxLoc)
      else maxLoc - idx3 / grid.toDouble * (maxLoc)

    val probLessThanT = area(0.0, threshold, 100)

    (ll, u, maxLoc, probLessThanT, u - ll, denom)

  }

  def integrate(f: Double => Double,
                a: Double,
                b: Double,
                min: Int = 16,
                max: Int = 128): Double =
    GaussLegendre.integrate(
      f,
      a,
      b,
      max
    )

  val leftSkew = (x: Double) => jdistlib.Beta.density(x, 1, 10, false)

  val rightSkew = (x: Double) => jdistlib.Beta.density(x, 10, 1, false)

  val uniform = (x: Double) => 1d

  case class Posteriors(`P(p1|x)`: Double,
                        `P(s|x,p1)`: Double => Double,
                        `P(s|x,p2)`: Double => Double,
                        `P(s<0.1|x,p2)`: Double,
                        `E(P(s|x,p2))`: Double)

  def bayesAlternatingPriors(`P(x|s)`: Double => Double,
                             prior1: Double => Double,
                             prior2: Double => Double,
                             nx: Int = 1): Posteriors = {

    val `P(x,s|p1)` = (s: Double) => `P(x|s)`(s) * prior1(s)
    val `P(x,s|p2)` = (s: Double) => `P(x|s)`(s) * prior2(s)

    val `P(x|p1)` = integrate(`P(x,s|p1)`, 0.0, 1.0, 16 * nx, 1024 * nx)
    val `P(x|p2)` = integrate(`P(x,s|p2)`, 0.0, 1.0, 16 * nx, 1024 * nx)

    val `P(s|x,p1)` = (s: Double) => `P(x,s|p1)`(s) / `P(x|p1)`
    val `P(s|x,p2)` = (s: Double) => `P(x,s|p2)`(s) / `P(x|p2)`

    val `P(x)` = 0.5 * `P(x|p1)` + 0.5 * `P(x|p2)`
    val `P(p1|x)` = `P(x|p1)` * 0.5 / `P(x)`

    val `P(s<0.1|x,p2)` = integrate(`P(s|x,p2)`, 0.0, 0.1, 16 * nx, 1024 * nx)
    val `E(P(s|x,p2))` =
      integrate((s: Double) => `P(s|x,p2)`(s) * s, 0.0, 1.0, 16 * nx, 1024 * nx)

    Posteriors(`P(p1|x)`,
               `P(s|x,p1)`,
               `P(s|x,p2)`,
               `P(s<0.1|x,p2)`,
               `E(P(s|x,p2))`)

  }

}
