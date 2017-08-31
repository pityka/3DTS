/*
 * The MIT License
 *
 * Copyright (c) 2015 ECOLE POLYTECHNIQUE FEDERALE DE LAUSANNE, Switzerland,
 * Group Fellay
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the "Software"),
 * to deal in the Software without restriction, including without limitation
 * the rights to use, copy, modify, merge, publish, distribute, sublicense,
 * and/or sell copies of the Software, and to permit persons to whom the Software
 * is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

sealed trait Quadrature {
  def integrate(f: Double => Double, a: Double, b: Double, n: Int): Double
}

object GaussLegendre extends Quadrature {

  val w0 = 128.0 / 225.0
  val x0 = 0.0
  val w1 = (322d + 13 * math.sqrt(70)) / 900d
  val x1 = 1 / 3d * math.sqrt(5d - 2 * math.sqrt(10d / 7d))
  val w2 = w1
  val x2 = -1 * x1
  val w3 = (322d - 13 * math.sqrt(70)) / 900d
  val x3 = 1 / 3d * math.sqrt(5d + 2 * math.sqrt(10d / 7d))
  val w4 = w3
  val x4 = -1 * x3

  def fifth(
      f: Double => Double,
      a: Double,
      b: Double
  ) = {
    assert(b >= a, "b<a")

    (b - a) * 0.5 * (w0 * f((b - a) * 0.5 * x0 + (a + b) * 0.5) +
      w1 * f((b - a) * 0.5 * x1 + (a + b) * 0.5) +
      w2 * f((b - a) * 0.5 * x2 + (a + b) * 0.5) +
      w3 * f((b - a) * 0.5 * x3 + (a + b) * 0.5) +
      w4 * f((b - a) * 0.5 * x4 + (a + b) * 0.5))

  }

  def integrate(
      f: Double => Double,
      a: Double,
      b: Double,
      n: Int
  ) = {
    assert(n >= 5)
    val panels = n / 5
    val h = (b - a) / panels
    var x = a
    var j = 0
    var sum = 0.0
    while (j < panels) {
      sum += fifth(f, x, x + h)
      x += h
      j += 1
    }

    sum

  }

}

object NumericalIntegration {

  @inline
  def fabs(x: Double, y: Double) = math.abs(x - y)

  @inline
  def frel(x: Double, y: Double) = math.abs((x - y) / x)

  def recursive(
      f: Double => Double,
      a: Double,
      b: Double,
      rel: Double,
      abs: Double,
      n: Int,
      formula: Quadrature,
      maxDepth: Int
  ) = {

    @scala.annotation.tailrec
    def rec(
        f: Double => Double,
        intervals: List[(Double, Double, Int, Int)],
        acc: Double
    ): Double = {
      if (intervals.isEmpty) acc
      else {
        val (a, b, depth, n) = intervals.head

        val whole = formula.integrate(f, a, b, 10)
        val h = (b - a) / n
        var x = a
        var sum = 0.0
        var i = 0
        var iv = intervals.tail
        while (i < n) {

          sum += formula.integrate(f, x, x + h, 10)
          iv = (x, x + h, depth + 1, n) :: iv
          x += h
          i += 1
        }

        if (depth < maxDepth && fabs(whole, sum) > abs && frel(whole, sum) > rel) {
          rec(f, iv, acc)
        } else {
          rec(f, intervals.tail, sum + acc)
        }
      }
    }

    rec(f, List((a, b, 0, n)), 0.0)

  }

  def iterative(
      f: Double => Double,
      a: Double,
      b: Double,
      rel: Double,
      abs: Double,
      startIter: Int,
      maxIterations: Int,
      formula: Quadrature
  ) = {
    var i = startIter
    var n = 1 << startIter
    var i0 = integrate(f, a, b, n, formula)
    n *= 2
    var i1 = integrate(f, a, b, n, formula)

    while (i <= maxIterations && fabs(i0, i1) > abs && frel(i0, i1) > rel) {
      // println(i + " " + n)
      i0 = i1
      n *= 2
      i1 = integrate(f, a, b, n, formula)
      i += 1
    }

    (i1, i)
  }

  def integrate(
      f: Double => Double,
      a: Double,
      b: Double,
      n: Int,
      formula: Quadrature
  ) = formula.integrate(f, a, b, n)

}

object NewtonCotes {
  import NumericalIntegration.{frel, fabs}

  case object Simpsons extends Quadrature {
    def integrate(f: Double => Double, a: Double, b: Double, n: Int) =
      NewtonCotes.simpson(f, a, b, n)
  }
  case object Trapezoid extends Quadrature {
    def integrate(f: Double => Double, a: Double, b: Double, n: Int) =
      NewtonCotes.trapezoid(f, a, b, n)
  }
  case object Booles extends Quadrature {
    def integrate(f: Double => Double, a: Double, b: Double, n: Int) =
      NewtonCotes.booles(f, a, b, n)
  }

  def booles(
      f: Double => Double,
      a: Double,
      b: Double,
      n1: Int
  ) = {
    assert(b >= a, "b<a")
    assert(n1 >= 4, "n<4")

    val n = n1 / 4 * 4

    val h = (b - a) / (n)
    var x = a

    var j = 0
    var sum = 0.0
    while (j <= n) {
      val t = f(x)
      val w =
        if (j == 0 || j == n) 7.0
        else if (j % 2 == 1) 32.0
        else if ((j - 2) % 4 == 0) 12.0
        else 14.0
      sum += w * t

      x += h
      j += 1
    }
    sum * h * 2 / 45.0
  }

  def trapezoid(
      f: Double => Double,
      a: Double,
      b: Double,
      n: Int
  ) = {
    assert(b >= a, "b<a")

    val h = (b - a) / n
    var x = a

    var f0 = f(x)
    x += h

    var j = 1
    var sum = 0.0
    while (j <= n) {
      val t = f(x)
      sum += f0 + t
      f0 = t
      x += h
      j += 1
    }
    sum * h / 2.0
  }

  def recursiveSimpson(
      f1: Double => Double,
      a: Double,
      b: Double,
      abs: Double,
      rel: Double,
      min: Int,
      max: Int
  ): Double = {

    val f = {
      val mmap = new it.unimi.dsi.fastutil.doubles.Double2DoubleOpenHashMap
      mmap.defaultReturnValue(-1d)
      (d: Double) =>
        {
          val c = mmap.get(d)
          if (c != -1d) c
          else {
            val r = f1(d)
            mmap.put(d, r)
            r
          }
        }
    }

    def rec(a: Double, b: Double, eps: Double, whole: Double, n: Int): Double = {
      val c = (a + b) / 2
      val left = simpson(f, a, c, 2)
      val right = simpson(f, c, b, 2)

      if ((fabs(left + right, whole) <= eps || frel(left + right, whole) <= rel || n <= 0) && n < (max - min)) {
        left + right + (left + right - whole) / 15
      } else rec(a, c, eps / 2, left, n - 1) + rec(c, b, eps / 2, right, n - 1)
    }

    rec(a, b, abs, simpson(f, a, b, 2), max)

  }

  def simpson(
      f: Double => Double,
      a: Double,
      b: Double,
      n: Int
  ) = {
    assert(b >= a, "b<a")
    assert(n % 2 == 0, "n not even")

    val h = (b - a) / n

    var x = a

    @inline def step = {
      x += h
    }

    var f0 = f(x)
    step
    var f1 = f(x)
    step
    var f2 = f(x)

    var j = 1
    val np2 = n / 2
    var sum = 0.0
    while (j <= np2) {
      sum += f0 + 4 * f1 + f2
      j += 1
      if (j <= np2) {
        f0 = f2
        step
        f1 = f(x)
        step
        f2 = f(x)
      }
    }
    sum * h / 3.0
  }

}
