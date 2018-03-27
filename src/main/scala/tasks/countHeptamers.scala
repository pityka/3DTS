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
    EColl.outerJoinBy2[GenomeCoverage, GnomadLine]("joinGnomadCoverageWithData",
                                                   1)(4, _.cp, _.cp)

  def calculateHeptamer(coverage: EColl[GenomeCoverage],
                        calls: EColl[GnomadLine],
                        fasta: SharedFile,
                        fai: SharedFile,
                        gencode: SharedFile)(
      implicit tc: tasks.TaskSystemComponents,
      ec: ExecutionContext): Future[EColl[(String, (Int, Int))]] = {
    for {
      filteredCoverage <- filterTask(coverage, gencode)(
        CPUMemoryRequest(1, 5000))
      joined <- joinGnomadGenomeCoverageWithGnomadDataTask(
        (filteredCoverage, calls))(CPUMemoryRequest(1, 5000))
      mapped <- mapTask((joined, (fasta, fai)))(CPUMemoryRequest(1, 5000))
      grouped <- groupByTask(mapped)(CPUMemoryRequest(1, 5000))
      summed <- sum(grouped)(CPUMemoryRequest(1, 5000))
    } yield summed

  }

  val groupByTask =
    EColl.groupBy[(String, (Int, Int))]("groupHeptamer", 1)(1, _._1)

  val sum =
    EColl.reduceSeq[(String, (Int, Int))]("sumHeptamer", 1) { seq =>
      val a = seq.map(_._2._1).sum
      val b = seq.map(_._2._2).sum
      assert(seq.map(_._1).toSet.size == 1)
      (seq.head._1, (a, b))
    }

  val mapTask =
    EColl.mapSourceWith[(Option[GenomeCoverage], Option[GnomadLine]),
                        (SharedFile, SharedFile),
                        (String, (Int, Int))]("mapHeptamer", 1) {
      case (variants, (fasta, fai)) =>
        implicit ctx =>
          implicit val mat = ctx.components.actorMaterializer

          val countTable: Future[List[(String, (Int, Int))]] = for {
            fasta <- fasta.file
            fai <- fai.file
            reference = HeptamerHelpers.openFasta(fasta, fai)
            counts <- {

              val mutable =
                scala.collection.mutable.AnyRefMap[String, (Int, Int)]()

              variants
                .collect {
                  case (Some(cov), calls) => (cov, calls)
                }
                .runFold(mutable) {
                  case (mutable,
                        (coverage: GenomeCoverage, maybeVariantCall)) =>
                    val totalCount = maybeVariantCall
                      .map(gl =>
                        gl.genders.male.total_count + gl.genders.female.total_count)
                      .getOrElse(coverage.numberOfWellSequencedIndividuals)
                    val totalCall = maybeVariantCall
                      .map(gl =>
                        gl.genders.male.total_calls + gl.genders.female.total_calls)
                      .getOrElse(0)

                    val heptamer =
                      HeptamerHelpers.heptamerAt(coverage.chromosome,
                                                 coverage.position,
                                                 reference)

                    mutable.get(heptamer) match {
                      case None =>
                        mutable.update(heptamer, (totalCall, totalCount))
                      case Some((calls, counts)) =>
                        mutable.update(heptamer,
                                       (totalCall + calls, totalCount + counts))
                    }
                    mutable
                }
                .map(_.toList)
            }
          } yield counts

          Source.fromFuture(countTable).flatMapConcat(list => Source(list))

    }

}
