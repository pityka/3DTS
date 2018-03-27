import MathHelpers._
object Model {

  val rng = new jdistlib.rng.MersenneTwister

  def simulation(size: Int, rounds: Int, p: Double) = {
    val ar = Array.ofDim[Boolean](size)
    def makeRound = {
      var i = 0
      while (i < ar.size) {
        if (!ar(i)) {
          if (rng.nextDouble < p) {
            ar(i) = true
          }
        }
        i += 1
      }
    }

    0 until rounds foreach (_ => makeRound)

    ar.count(_ == true)
  }

  def approximationPoisson(size: Int, rounds: Int, p: Double) =
    (1d - math.exp(-1 * p * rounds)) * size

  def solveForP(size: Int, rounds: Int, successes: Double): Double = {
    val x = successes.toDouble / size
    val lambda = -1 * math.log(1d - x)
    lambda / rounds
  }

  def appoximationPoissonWithNs(size: Map[Int, Int], rounds: Int, p: Double) =
    1 to 3 map { i =>
      (1d - math.exp(-1 * p * rounds * i / 3d)) *
        size.get(i).getOrElse(0)
    } sum

  def appoximationPoissonWithNsWithRounds(sizes: Seq[(Int, Int, Int)],
                                          p: Double) =
    sizes map {
      case (ns, numLoci, rounds) =>
        (1d - math.exp(-1 * p * rounds * ns / 3d)) *
          numLoci
    } sum

  def appoximationPoissonCodon(codons: Seq[(Seq[Int], Int)],
                               rounds: Int,
                               p: Double) =
    codons.map {
      case (codon, mult) =>
        val e = 1d - (codon map { i =>
          math.exp(-1 * p * rounds * i / 3d)
        } reduce (_ * _))
        e * mult
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

  def solveForPCodons(codons: Seq[(Seq[Int], Int)],
                      rounds: Int,
                      successes: Double): Double = {
    val f = (p: Double) => {
      appoximationPoissonCodon(codons, rounds, p) - successes
    }

    bisect(1E-10, 10d / rounds, 0.5 / rounds, 200, 1E-25, 1E-25)(f)
  }

  def solveForPCodonsWithRounds(codons: Seq[(Seq[Int], Int, Int)],
                                successes: Double): Double = {
    val f = (p: Double) => {
      appoximationPoissonCodonWithRounds(codons, p) - successes
    }

    bisect(1E-10, 1E-3, 1E-5, 200, 1E-25, 1E-25)(f)
  }

  def solveForPWithNs(size: Map[Int, Int],
                      rounds: Int,
                      successes: Double): Double = {

    val f = (p: Double) => {
      appoximationPoissonWithNs(size, rounds, p) - successes
    }

    bisect(1E-10, 10d / rounds, 0.5 / rounds, 200, 1E-25, 1E-25)(f)
  }

  def solveForPWithNsWithRounds(size: Seq[(Int, Int, Int)],
                                successes: Double): Double = {

    val f = (p: Double) => {
      appoximationPoissonWithNsWithRounds(size, p) - successes
    }

    bisect(1E-10, 1E-3, 1E-5, 200, 1E-25, 1E-25)(f)
  }

  def probabilityOfCodonHasAtLeast1(codon: Seq[(Int, Int)],
                                    p: Double,
                                    selection: Double): Double =
    1d - (codon.foldLeft(1d) {
      case (pp, (i, rounds)) =>
        math.exp(-1 * p * rounds * selection * i / 3d) * pp
    })

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

  def likelihoodCodons(codons: Seq[Seq[(Int, Int)]],
                       successes: Int,
                       p: Double,
                       selection: Double): Double = {
    val ps =
      codons.foldLeft(0d)((s, c) =>
        s + probabilityOfCodonHasAtLeast1(c, p, selection))
    // poibin.pdf(Array(successes), ps.toArray)(0)
    jdistlib.Poisson.density(successes, ps, false)
  }

  def mlCodons(codons: Seq[Seq[(Int, Int)]],
               rounds: Int,
               successes: Int,
               p: Double): Double = {
    val f = (s: Double) => {
      likelihoodCodons(codons, successes, p, s)
    }

    (0 to 1000 maxBy (i => f(i / 1000d))) / 1000d
  }

}
