package sd

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

object GaussLegendre {

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
