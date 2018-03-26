import java.io.File
import collection.JavaConversions._
import scala.sys.process._
import scala.concurrent._
import scala.concurrent.duration._
import tasks.upicklesupport._
  
import tasks._
import tasks.queue.NodeLocalCache
import tasks.util.TempFile
import java.io._

import fileutils._
import stringsplit._

import IOHelpers._
import MathHelpers._
import Model._

import akka.stream.ActorMaterializer
import akka.stream.scaladsl._
import akka.util._
import scala.collection.mutable.ArrayBuffer

case class GenomeCoverage(chromosome: String,
                          position: Int,
                          numberOfWellSequencedIndividuals: Int)

case class FilterCoverageInput(coverage: JsDump[GenomeCoverage],
                               gencodeGtf: SharedFile)

object FilterCoverage {

  val task =
    AsyncTask[FilterCoverageInput, JsDump[GenomeCoverage]]("filtercoverage-1",
                                                           1) {

      case FilterCoverageInput(coverage, gencode) =>
        implicit ctx =>
          implicit val mat = ctx.components.actorMaterializer
          log.info(
            "Start filtering " + coverage.sf.name + " to " + gencode.name)
          for {
            gencodeL <- gencode.file
            exons <- NodeLocalCache.getItem("intervaltrees" + gencode) {
              openSource(gencodeL) { gencodeSource =>
                JoinVariationsCore.exomeIntervalTree(gencodeSource)
              }
            }
            result <- {
              log.info("Interval trees done")

              coverage.source
                .filter(
                  coverage =>
                    !JoinVariationsCore
                      .lookup(coverage.chromosome, coverage.position, exons)
                      .isEmpty)
                .runWith(JsDump.sink[GenomeCoverage](
                  name = coverage.sf.name + ".filter." + gencode.name))

            }

          } yield result

    }

}
