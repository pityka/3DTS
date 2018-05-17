package sd

object MathHelpers {

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
                max: Int = 128): Double =
    GaussLegendre.integrate(
      f,
      a,
      b,
      max
    )

  def bayes(`P(x|s)`: Double => Double,
            prior: Double => Double,
            nx: Int = 1): Posterior = {

    val `P(x,s|prior)` = (s: Double) => `P(x|s)`(s) * prior(s)

    val `P(x|prior)` = integrate(`P(x,s|prior)`, 0.0, 1.0, 1024 * nx)

    val `P(s|x,prior)` = (s: Double) => `P(x,s|prior)`(s) / `P(x|prior)`

    val cdf = (1 to 10).toList
      .map { (i: Int) =>
        integrate(`P(s|x,prior)`, i / 10d - 0.1d, i / 10d, 1024 * nx)
      }
      .scanLeft(0d)(_ + _)

    val `E(P(s|x,prior))` =
      integrate((s: Double) => `P(s|x,prior)`(s) * s, 0.0, 1.0, 1024 * nx)

    Posterior(`E(P(s|x,prior))`, cdf)

  }

  def estimatePosteriorMeanWithImportanceSampling(likelihood: Double => Double,
                                                  prior: Double => Double,
                                                  sampleFromPrior: => Double,
                                                  n: Int): sampling.Estimate = {

    def unnormalizedPosterior(x: Double) = prior(x) * likelihood(x)

    sampling.ImportanceSampling
      .unnormalized(
        evaluateUnnormalizedTarget = unnormalizedPosterior _,
        sampleFromSamplingDistribution = sampleFromPrior,
        evaluateSamplingDistribution = prior,
        f = (x: Double) => x,
        numberOfSamples = n
      )

  }

  def findMinimum(x0: Double, x1: Double, eps: Double, maxIt: Int)(
      g: Double => Double): (Double, Int) = {

    assert(x0 < x1)

    val Phi = (1.0 + math.sqrt(5)) / 2.0

    val resPhi = 2.0 - Phi

    def memoize(f: Double => Double) = {
      val c = collection.mutable.Map[Double, Double]()
      (x: Double) =>
        {
          c.get(x) match {
            case Some(x) => x
            case None => {
              val r = f(x)
              c.update(x, r)
              r
            }
          }
        }
    }

    def goldenSectionSearch(a: Double,
                            b: Double,
                            c: Double,
                            tau: Double,
                            it: Int)(f: Double => Double): (Double, Int) = {
      val x =
        if (c - b > b - a) b + resPhi * (c - b)
        else b - resPhi * (b - a)
      if (it > maxIt) ((c + a) / 2.0, it)
      else {
        if (math.abs(c - a) < (tau * (math.abs(b) + math.abs(x))))
          ((c + a) / 2.0, it);
        else {
          // assert(f(x) != f(b), f(x) + " " + f(b));
          if (f(x) < f(b)) {
            if (c - b > b - a) goldenSectionSearch(b, x, c, tau, it + 1)(f);
            else goldenSectionSearch(a, x, b, tau, it + 1)(f);
          } else {
            if (c - b > b - a) goldenSectionSearch(a, b, x, tau, it + 1)(f);
            else goldenSectionSearch(x, b, c, tau, it + 1)(f);
          }
        }
      }
    }
    val f = memoize(g)
    goldenSectionSearch(x0, x1 - resPhi * (x1 - x0), x1, math.sqrt(eps), 0)(f)

  }

}
