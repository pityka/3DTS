import java.io.File
import collection.JavaConversions._
import scala.sys.process._
import scala.concurrent._
import scala.concurrent.duration._
import tasks._
import tasks.queue.NodeLocalCache
import tasks.upicklesupport._
import tasks.collection._

import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Source
import SharedTypes._

import JoinVariationsCore.{GnomadLine}

case class HeptamerRates(sf: SharedFile)

object CountHeptamers {

  val filterTask =
    EColl.mapSourceWith[GenomeCoverage, SharedFile, GenomeCoverage](
      "filterCoverageToInterGenic",
      1) {
      case (coverageSource, gencode) =>
        implicit ctx =>
          implicit val mat = ctx.components.actorMaterializer

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
      1)(1024 * 1024 * 10, _.cp, _.cp)

  def calculateHeptamer(coverage: EColl[GenomeCoverage],
                        calls: EColl[GnomadLine],
                        fasta: SharedFile,
                        fai: SharedFile,
                        gencode: SharedFile)(
      implicit tc: tasks.TaskSystemComponents,
      ec: ExecutionContext): Future[HeptamerRates] = {
    for {
      filteredCoverage <- filterTask(coverage, gencode)(
        CPUMemoryRequest(12, 5000))
      joined <- joinGnomadGenomeCoverageWithGnomadDataTask(
        (filteredCoverage, calls))(CPUMemoryRequest(12, 5000))
      mapped <- mapTask((joined, (fasta, fai)))(CPUMemoryRequest(1, 5000))
      grouped <- groupByTask(mapped)(CPUMemoryRequest(1, 5000))
      summed <- sum(grouped)(CPUMemoryRequest(1, 5000))
      _ <- heptamerCounts(summed)(CPUMemoryRequest(1, 5000))
      rates <- heptamerNeutralRates(summed)(CPUMemoryRequest(1, 5000))
      file <- heptamerFrequenciesToFile(rates)(CPUMemoryRequest(1, 5000))
    } yield file
  }

  val groupByTask =
    EColl
      .groupBy[(String, (Seq[Int], Seq[Int], Int))]("groupHeptamer", 1)(
        1024 * 1024 * 10,
        _._1)

  val sum =
    EColl.reduceSeq[(String, (Seq[Int], Seq[Int], Int))]("sumHeptamer", 1) {
      seq =>
        val a = seq.map(_._2._1).flatten
        val b = seq.map(_._2._2).flatten
        val heptamerCount = seq.map(_._2._3).sum
        assert(seq.map(_._1).toSet.size == 1)
        assert(heptamerCount == a.size,
               a.size.toString + " != " + heptamerCount)
        assert(heptamerCount == b.size,
               a.size.toString + " != " + heptamerCount)
        (seq.head._1, (a, b, heptamerCount))
    }

  val heptamerCounts = EColl
    .map[(String, (Seq[Int], Seq[Int], Int)), (String, Int)]("hepCounts", 1) {
      case (hep, (_, _, c)) => (hep, c)
    }

  val mapTask =
    EColl
      .mapSourceWith[(Option[GenomeCoverage], Option[GnomadLine]),
                     (SharedFile, SharedFile),
                     (String, (Seq[Int], Seq[Int], Int))]("mapHeptamer", 2) {
        case (variants, (fasta, fai)) =>
          implicit ctx =>
            implicit val mat = ctx.components.actorMaterializer

            val countTable: Future[List[(String, (Seq[Int], List[Int], Int))]] =
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

                        val heptamer =
                          HeptamerHelpers.heptamerAt(coverage.chromosome,
                                                     coverage.position,
                                                     reference)
                        if (!heptamer.contains('N')) {

                          passSNP.foreach { passSNP =>
                            assert(passSNP.ref == heptamer(3).toString)
                          }
                          mutable.get(heptamer) match {
                            case None =>
                              mutable.update(
                                heptamer,
                                (List(sampleSize), List(variantAlleleCount), 1))
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
                        mutable
                    }
                    .map(_.toList)
                }
              } yield counts

            Source.fromFuture(countTable).flatMapConcat(list => Source(list))

      }

  val heptamerFrequenciesToFile =
    AsyncTask[EColl[(String, Double, Int, Seq[Int])], HeptamerRates](
      "writeHeptamerRates",
      1) { ecoll => implicit ctx =>
      implicit val mat = ctx.components.actorMaterializer
      val f = fileutils.openFileWriter { writer =>
        ecoll.source(resourceAllocated.cpu).runForeach {
          case (hept, rate, counts, calls) =>
            writer.write(s"$hept\t$rate\t$counts\t${calls.mkString(",")}\n")
        }
      }._1
      SharedFile(f, "heptamerRates." + ecoll.basename)
        .map(HeptamerRates.apply)
    }

  val heptamerNeutralRates =
    EColl.map[(String, (Seq[Int], Seq[Int], Int)),
              (String, Double, Int, Seq[Int])]("heptamerNeutralRates", 1) {
      case (heptamer, (samples, alleleCounts, _)) =>
        val countOfVariableLoci = alleleCounts.count(_ > 0)
        val p =
          Model.posteriorMeanOfNeutralRate(samples.toArray, countOfVariableLoci)
        (heptamer, p, countOfVariableLoci, samples)
    }

}
