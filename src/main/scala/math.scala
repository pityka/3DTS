object MathHelpers {

  def makeColor(s: Seq[Double]) = {
    val cdf = CumulativeRelativeFrequencies.empirical(s)
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

  case class Posteriors(`P(s|x,p1)`: Double => Double, `E(P(s|x,p1))`: Double)

  def bayes(`P(x|s)`: Double => Double,
            prior1: Double => Double,
            nx: Int = 1): Posteriors = {

    val `P(x,s|p1)` = (s: Double) => `P(x|s)`(s) * prior1(s)

    val `P(x|p1)` = integrate(`P(x,s|p1)`, 0.0, 1.0, 16 * nx, 1024 * nx)

    val `P(s|x,p1)` = (s: Double) => `P(x,s|p1)`(s) / `P(x|p1)`

    val `E(P(s|x,p1))` =
      integrate((s: Double) => `P(s|x,p1)`(s) * s, 0.0, 1.0, 16 * nx, 1024 * nx)

    Posteriors(`P(s|x,p1)`, `E(P(s|x,p1))`)

  }

}
