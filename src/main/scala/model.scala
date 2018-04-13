import MathHelpers._
object Model {

  def appoximationPoissonWithNsWithRounds(sizes: Seq[(Int, Int, Int)],
                                          p: Double) =
    sizes map {
      case (ns, numLoci, rounds) =>
        (1d - math.exp(-1 * p * rounds * ns / 3d)) *
          numLoci
    } sum

  def appoximationPoissonCodonWithRounds(codons: Seq[(Seq[Int], Int, Int)],
                                         p: Double) =
    codons.map {
      case (codon, mult, rounds) =>
        val e = 1d - (codon map { i =>
          math.exp(-1 * p * rounds * i / 3d)
        } reduce (_ * _))
        e * mult
    } sum

  def solveForPWithNsWithRounds(size: Seq[(Int, Int, Int)],
                                successes: Double): Double = {

    val f = (p: Double) => {
      appoximationPoissonWithNsWithRounds(size, p) - successes
    }

    bisect(1E-10, 1E-3, 1E-5, 200, 1E-25, 1E-25)(f)
  }

  def probabilityOfLocusHasAtLeast1(rounds: Int,
                                    numNs: Int,
                                    p: Double,
                                    selection: Double): Double =
    1d -
      math.exp(-1 * p * rounds * selection * numNs / 3d)

  /* P(x|p,s) */
  def likelihoodLoci(lociNumNs: Array[Int],
                     lociRounds: Array[Int],
                     successes: Int,
                     p: Array[Double],
                     selection: Double): Double = {
    val ps = {
      var s = 0d
      var i = 0
      while (i < lociRounds.size) {
        s += probabilityOfLocusHasAtLeast1(rounds = lociRounds(i),
                                           numNs = lociNumNs(i),
                                           p = p(i),
                                           selection = selection)
        i += 1
      }
      s
    }

    // poibin.pdf(Array(successes), ps.toArray)(0)
    jdistlib.Poisson.density(successes, ps, false)
  }

  def posteriorMeanOfNeutralRate(lociRounds: Array[Int],
                                 successes: Int): Double =
    posteriorUnderSelection1D(lociRounds.map(_ => 3),
                              lociRounds,
                              successes,
                              lociRounds.map(_ => 1.0))._3

  def posteriorUnderSelection1D(lociNumNs: Array[Int],
                                lociRounds: Array[Int],
                                successes: Int,
                                p: Double): (Double, Double, Double) =
    posteriorUnderSelection1D(lociNumNs,
                              lociRounds,
                              successes,
                              lociNumNs.map(_ => p))

  def posteriorUnderSelection1D(lociNumNs: Array[Int],
                                lociRounds: Array[Int],
                                successes: Int,
                                p: Array[Double]): (Double, Double, Double) = {

    val `P(x|s)` =
      likelihoodLoci(lociNumNs, lociRounds, successes, p, _: Double)

    val Posteriors(postP1, _, _, postLessThan10, postMean) =
      bayesAlternatingPriors(`P(x|s)` = `P(x|s)`,
                             prior1 = MathHelpers.leftSkew,
                             prior2 = MathHelpers.uniform)

    (postP1, postLessThan10, postMean)
  }

}
