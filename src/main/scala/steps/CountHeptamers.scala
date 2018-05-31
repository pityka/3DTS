package sd.steps

import sd._
import java.io.File
import scala.concurrent._
import tasks._
import tasks.queue.NodeLocalCache
import tasks.upicklesupport._
import tasks.collection._
import akka.stream.scaladsl.Source
import JoinVariationsCore.{GnomadLine}

case class HeptamerRates(sf: SharedFile)

object CountHeptamers {

  val filterToChromosome =
    EColl.mapSourceWith[GenomeCoverage, Option[String], GenomeCoverage](
      "filterCoverageToChromosome",
      1)(mayChromosome => mayChromosome.getOrElse("")) {
      case (coverageSource, mayChromosome) =>
        implicit ctx =>
          coverageSource
            .filter { coverage =>
              if (mayChromosome.isEmpty) true
              else {
                coverage.chromosome == mayChromosome.get
              }
            }

    }

  val autosomes = 1 to 22 map (i => "chr" + i) toSet

  val filterToAutosome =
    EColl
      .filter[GenomeCoverage]("filterCoverageToAutosome", 1) {
        case (coverage) =>
          sd.steps.CountHeptamers.autosomes.contains(coverage.chromosome)
      }

  val filterTask =
    EColl.mapSourceWith[GenomeCoverage, SharedFile, GenomeCoverage](
      "filterCoverageToInterGenic",
      1)(_ => "") {
      case (coverageSource, gencode) =>
        implicit ctx =>
          val futureSource = for {
            gencodeL <- gencode.file
            exons <- NodeLocalCache.getItem("intervaltrees" + gencode) {
              fileutils.openSource(gencodeL) { gencodeSource =>
                JoinVariationsCore.exomeIntervalTree(gencodeSource)
              }
            }
          } yield {
            log.info("Interval trees done")

            coverageSource
              .filter(
                coverage =>
                  JoinVariationsCore
                    .lookup(coverage.chromosome, coverage.position, exons)
                    .isEmpty)

          }

          Source.fromFuture(futureSource).flatMapConcat(identity)
    }

  val joinGnomadGenomeCoverageWithGnomadDataTask =
    EColl.outerJoinBy2[GenomeCoverage, GnomadLine](
      "joinGnomadCoverageWithData",
      1)(1024 * 1024 * 50, _.cp, _.cp, Some(1))

  def calculateHeptamer(coverage: EColl[GenomeCoverage],
                        calls: EColl[GnomadLine],
                        fasta: SharedFile,
                        fai: SharedFile,
                        gencode: SharedFile,
                        chromosomeFilter: Option[String])(
      implicit tc: tasks.TaskSystemComponents,
      ec: ExecutionContext)
    : Future[(HeptamerRates, HeptamerIndependentIntergenicRate)] = {
    for {
      intergenicCoverage <- filterTask((coverage, gencode))(
        CPUMemoryRequest(12, 5000))
      coverageOnChromosome <- if (chromosomeFilter.isEmpty)
        filterToAutosome(intergenicCoverage)(CPUMemoryRequest(12, 5000))
      else
        filterToChromosome((intergenicCoverage, chromosomeFilter))(
          CPUMemoryRequest(12, 5000))
      joined <- joinGnomadGenomeCoverageWithGnomadDataTask(
        (coverageOnChromosome, calls))(CPUMemoryRequest((1, 12), 5000))
      mapped <- mapTask((joined, (fasta, fai)))(CPUMemoryRequest(1, 5000))
      grouped <- groupByTask(mapped)(CPUMemoryRequest((1, 12), 5000))
      summed <- sum(grouped)(CPUMemoryRequest(1, 5000))
      concatenated <- concatenate(summed)(CPUMemoryRequest((1, 12), 5000))
      globalRate <- heptamerIndependentNeutralRate(concatenated)(
        CPUMemoryRequest(1, 5000))
      _ <- heptamerCounts(summed)(CPUMemoryRequest(1, 5000))
      rates <- heptamerNeutralRates(summed)(CPUMemoryRequest(1, 5000))
      file <- heptamerFrequenciesToFile(rates)(CPUMemoryRequest(1, 5000))
    } yield (file, globalRate)
  }

  val groupByTask =
    EColl
      .groupBy[HeptamerOccurences]("groupHeptamer", 1)(1024 * 1024 * 50,
                                                       _.heptamer,
                                                       Some(1))

  val sum =
    EColl.reduceSeq[HeptamerOccurences]("sumHeptamer", 1) { seq =>
      val a = seq.toVector.map(_.sampleSizes).flatten
      val b = seq.toVector.map(_.variantAlleleCounts).flatten
      val heptamerCount = seq.map(_.heptamerCount).sum
      assert(seq.map(_.heptamer).toSet.size == 1)
      assert(heptamerCount == a.size, a.size.toString + " != " + heptamerCount)
      assert(heptamerCount == b.size, a.size.toString + " != " + heptamerCount)
      HeptamerOccurences(seq.head.heptamer, a, b, heptamerCount)
    }

  val heptamerCounts = EColl
    .map[HeptamerOccurences, (String, Int)]("hepCounts", 1) {
      case HeptamerOccurences(hep, _, _, c) => (hep, c)
    }

  case class HeptamerOccurences(heptamer: String,
                                sampleSizes: Vector[Int],
                                variantAlleleCounts: Vector[Int],
                                heptamerCount: Int)

  val mapTask =
    EColl
      .mapSourceWith[(Option[GenomeCoverage], Option[GnomadLine]),
                     (SharedFile, SharedFile),
                     HeptamerOccurences]("mapHeptamer", 2)(_ => "") {
        case (variants, (fasta, fai)) =>
          implicit ctx =>
            implicit val mat = ctx.components.actorMaterializer

            val countTable: Future[List[HeptamerOccurences]] =
              for {
                fasta <- fasta.file
                fai <- fai.file
                reference = HeptamerHelpers.openFasta(fasta, fai)
                counts <- {

                  val mutable =
                    scala.collection.mutable
                      .AnyRefMap[String, (List[Int], List[Int], Int)]()

                  variants
                    .collect {
                      case (Some(cov), calls) => (cov, calls)
                    }
                    .runFold(mutable) {
                      case (mutable,
                            (coverage: GenomeCoverage, maybeVariantCall)) =>
                        val passSNP = maybeVariantCall
                          .filter { x =>
                            x.ref.size == 1 && x.alt.size == 1 && x.filter == "PASS"
                          }
                        val sampleSize =
                          passSNP
                            .map(gl =>
                              gl.genders.male.totalChromosomeCount + gl.genders.female.totalChromosomeCount)
                            .map(_ / 2)
                            .getOrElse(
                              coverage.numberOfWellSequencedIndividuals)

                        val variantAlleleCount = passSNP
                          .map(gl =>
                            gl.genders.male.totalVariantAlleleCount + gl.genders.female.totalVariantAlleleCount)
                          .getOrElse(0)

                        val mayHeptamer =
                          HeptamerHelpers.heptamerAt(coverage.chromosome,
                                                     coverage.position,
                                                     reference)

                        mayHeptamer.failed.foreach { e =>
                          log.warning(s"Heptamer retrieval failed $coverage $e")
                        }
                        mayHeptamer.foreach { heptamer =>
                          if (!heptamer.contains('N')) {

                            passSNP.foreach { passSNP =>
                              assert(passSNP.ref == heptamer(3).toString)
                            }
                            mutable.get(heptamer) match {
                              case None =>
                                mutable.update(heptamer,
                                               (List(sampleSize),
                                                List(variantAlleleCount),
                                                1))
                              case Some(
                                  (sampleSizes,
                                   variantAlleleCounts,
                                   heptamerCount)) =>
                                mutable.update(
                                  heptamer,
                                  (sampleSize :: sampleSizes,
                                   variantAlleleCount :: variantAlleleCounts,
                                   heptamerCount + 1))
                            }
                          }
                        }
                        mutable
                    }
                    .map(_.toList)
                }
              } yield
                counts.map {
                  case (heptamer,
                        (sampleSizes, variantAlleleCounts, heptamerCount)) =>
                    HeptamerOccurences(heptamer,
                                       sampleSizes.toVector,
                                       variantAlleleCounts.toVector,
                                       heptamerCount)
                }

            Source.fromFuture(countTable).flatMapConcat(list => Source(list))

      }
  import akka.stream.scaladsl._
  import akka.util.ByteString
  def zippedTemporaryFileSink(
      implicit ec: ExecutionContext): Sink[ByteString, Future[File]] = {
    val f = tasks.util.TempFile.createTempFile("tmp")
    Flow[ByteString]
      .via(tasks.util.AkkaStreamComponents
        .strictBatchWeighted[ByteString](1024 * 512, _.size.toLong)(_ ++ _))
      .via(Compression.gzip)
      .toMat(FileIO.toPath(f.toPath))(Keep.right)
      .mapMaterializedValue(_.map(_ => f))
  }

  val heptamerFrequenciesToFile =
    AsyncTask[EColl[HeptamerNeutralRateAndVariableCount], HeptamerRates](
      "writeHeptamerRates",
      3) { ecoll => implicit ctx =>
      implicit val mat = ctx.components.actorMaterializer

      val accepted = Set('A', 'T', 'G', 'C')

      ecoll
        .source(resourceAllocated.cpu)
        .map {
          case HeptamerNeutralRateAndVariableCount(hept, rate, counts, _) =>
            if ((hept.toSet &~ accepted).isEmpty) {
              val str = s"$hept\t$rate\t$counts\n"
              ByteString(str)
            } else {
              ByteString.empty
            }
        }
        .toMat(zippedTemporaryFileSink)(Keep.right)
        .run
        .flatMap { f =>
          SharedFile(f, "heptamerRates." + ecoll.basename)
            .map(HeptamerRates.apply)
        }
    }

  case class HeptamerNeutralRateAndVariableCount(heptamer: String,
                                                 rate: Double,
                                                 countOfVariableLoci: Int,
                                                 samples: Vector[Int])

  val heptamerNeutralRates =
    EColl.map[HeptamerOccurences, HeptamerNeutralRateAndVariableCount](
      "heptamerNeutralRates",
      1) {
      case HeptamerOccurences(heptamer, samples, alleleCounts, _) =>
        val countOfVariableLoci = alleleCounts.count(_ > 0)
        val p =
          Model.mlNeutral(samples.toArray, countOfVariableLoci)
        HeptamerNeutralRateAndVariableCount(heptamer,
                                            p,
                                            countOfVariableLoci,
                                            samples)
    }

  val concatenate =
    EColl.foldLeft[HeptamerOccurences, (Vector[Int], Int)]("concatenate", 2)(
      (Vector[Int](), 0), {
        case ((accumulatedListOfSamples, accumulatedAlleleCounts),
              HeptamerOccurences(hepta, samples, alleleCounts, _)) =>
          val accepted = Set('A', 'T', 'G', 'C')
          if ((hepta.toSet &~ accepted).isEmpty) {
            val rnd = new scala.util.Random(1)
            val idx = rnd.shuffle((0 until alleleCounts.size).toList).take(2000)
            val countOfVariableLoci = idx.map(alleleCounts).count(_ > 0)

            (accumulatedListOfSamples ++ idx.map(samples),
             accumulatedAlleleCounts + countOfVariableLoci)
          } else (accumulatedListOfSamples, accumulatedAlleleCounts)
      }
    )

  val heptamerIndependentNeutralRate =
    AsyncTask[(Seq[Int], Int), HeptamerIndependentIntergenicRate](
      "heptamerIndependentNeutralRate",
      1) {
      case (samples, alleleCounts) =>
        implicit ctx =>
          val g = Model.mlNeutral(samples.toArray, alleleCounts)
          ctx.log.info("Global neutral rate " + g)
          Future.successful(HeptamerIndependentIntergenicRate(g))

    }

}
