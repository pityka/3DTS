package sd

import MathHelpers._
import com.typesafe.scalalogging.StrictLogging
object Model extends StrictLogging {

  def predictionPoissonWithNsWithRounds(lociNumNs: Array[Int],
                                        lociRounds: Array[Int],
                                        p: Array[Double]) = {
    var s = 0d
    var i = 0
    val N = lociNumNs.length
    while (i < N) {
      s += (1d - math.exp(-1 * p(i) * lociRounds(i) * lociNumNs(i) / 3d))
      i += 1
    }
    s
  }

  def predictionPoissonWithNsWithRounds(sizes: Seq[(Int, Int, Int)],
                                        p: Double) =
    sizes map {
      case (ns, numLoci, rounds) =>
        (1d - math.exp(-1 * p * rounds * ns / 3d)) *
          numLoci
    } sum

  def solveForPWithNsWithRounds(size: Seq[(Int, Int, Int)],
                                successes: Double): Double = {

    val f = (p: Double) => {
      predictionPoissonWithNsWithRounds(size, p) - successes
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
  def logLikelihoodLoci(lociNumNs: Array[Int],
                        lociRounds: Array[Int],
                        successes: Int,
                        p: Array[Double],
                        selection: Double): Double = {
    val ps = {
      var s = 0d
      var i = 0
      val N = lociRounds.length
      while (i < N) {
        s += probabilityOfLocusHasAtLeast1(rounds = lociRounds(i),
                                           numNs = lociNumNs(i),
                                           p = p(i),
                                           selection = selection)
        i += 1
      }
      s
    }

    jdistlib.Poisson.density(successes.toLong, ps, true)
  }

  def likelihoodLoci(lociNumNs: Array[Int],
                     lociRounds: Array[Int],
                     successes: Int,
                     p: Array[Double],
                     selection: Double): Double = {
    val ll = logLikelihoodLoci(lociNumNs, lociRounds, successes, p, selection)
    math.exp(ll)
  }

  def mlNeutral(lociRounds: Array[Int], successes: Int): Double = {
    val lik = (d: Double) =>
      logLikelihoodLoci(lociRounds.map(_ => 3),
                        lociRounds,
                        successes,
                        lociRounds.map(_ => 1.0),
                        d)

    val (max, _) =
      findMinimum(1E-12, 0.01, 1E-20, 20000)((d: Double) => -1 * lik(d))
    // val maxV = lik(max)
    // val predicted = appoximationPoissonWithNsWithRounds(lociRounds.map {
    //   rounds =>
    //     (3, 1, rounds)
    // }, max)

    // println(s"$successes ${lociRounds.size} ${lociRounds.sorted
    //   .apply(lociRounds.size / 2)} | ${lik(1E-6)} ${lik(5E-6)} ${lik(1E-5)} ${lik(
    //   5E-5)} ${lik(1E-4)} ${lik(5E-4)}  | ($max $maxV $predicted $it)")
    max
  }

  def posteriorMeanOfNeutralRate(lociRounds: Array[Int],
                                 successes: Int): Posterior =
    posteriorUnderSelection1D(lociRounds.map(_ => 3),
                              lociRounds,
                              successes,
                              lociRounds.map(_ => 1.0))

  def posteriorUnderSelection1D(lociNumNs: Array[Int],
                                lociRounds: Array[Int],
                                successes: Int,
                                p: Double): Posterior =
    posteriorUnderSelection1D(lociNumNs,
                              lociRounds,
                              successes,
                              lociNumNs.map(_ => p))

  def posteriorUnderSelection1D(lociNumNs: Array[Int],
                                lociRounds: Array[Int],
                                successes: Int,
                                p: Array[Double]): Posterior = {

    val `P(x|s)` =
      likelihoodLoci(lociNumNs, lociRounds, successes, p, _: Double)

    val rnd = new jdistlib.rng.RandomWELL44497b
    rnd.setSeed(1L)

    val posteriors @ Posterior(postMean, _) =
      bayes(`P(x|s)` = `P(x|s)`, prior = (x: Double) => 1.0)

    val postMeanIS = estimatePosteriorMeanWithImportanceSampling(
      likelihood = `P(x|s)`,
      prior = (x: Double) => 1.0,
      sampleFromPrior = rnd.nextDouble,
      n = 20000).estimate

    if (math.abs(postMean - postMeanIS) > 0.02) {
      logger.warn(
        "Quadrature vs Importance sampling deviate: " + postMean + "\t" + postMeanIS)
    }

    posteriors
  }

}
