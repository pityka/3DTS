import java.io.File
import collection.JavaConversions._
import scala.sys.process._
import scala.concurrent._
import scala.concurrent.duration._
import tasks._
import tasks.collection._
import tasks.queue.NodeLocalCache
import tasks.upicklesupport._

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

object ConvertGenomeCoverage {

  val toEColl =
    AsyncTask[JsDump[GenomeCoverage], EColl[GenomeCoverage]](
      "GenomeCoverageToEColl",
      1) { js => implicit ctx =>
      implicit val mat = ctx.components.actorMaterializer
      log.info(s"Convert $js to ecoll.")
      EColl.fromSource(js.source, js.sf.name, 1024 * 1024 * 10)
    }

  def convertLine(frame: ByteString, totalSize: Int) = {
    val spl = frame.utf8String.split1('\t')
    val chr = "chr" + spl(0)
    val bp = spl(1).toInt
    val histColumns: List[Int] = List(6)
    val callsAtLeast10x = (histColumns
      .map(spl)
      .take(3)
      .map(_.toDouble)
      .sum * totalSize.toDouble).toInt

    GenomeCoverage(chr, bp, callsAtLeast10x)
  }

  val task =
    AsyncTask[(SharedFile, Int), JsDump[GenomeCoverage]](
      "convertgenomecoverage-1",
      1) {
      case (coverageFile, totalSize) =>
        implicit ctx =>
          log.info("Process " + coverageFile.name)
          implicit val mat = ctx.components.actorMaterializer
          coverageFile.source
            .via(Framing.delimiter(ByteString("\n"),
                                   maximumFrameLength = Int.MaxValue))
            .map(convertLine(_, totalSize))
            .runWith(JsDump.sink[GenomeCoverage](
              coverageFile.name + "genomecoverage.js.gz"))

    }

  val gnomadToEColl =
    AsyncTask[(List[SharedFile], Int), EColl[GenomeCoverage]](
      "convertgenomecoverage-ecoll-1",
      1) {
      case (coverageFiles, totalSize) =>
        implicit ctx =>
          log.info("Process " + coverageFiles)
          implicit val mat = ctx.components.actorMaterializer
          val source = Source(coverageFiles).flatMapConcat { file =>
            file.source
              .via(Compression.gunzip())
              .via(tasks.util.AkkaStreamComponents
                .delimiter('\n', maximumFrameLength = Int.MaxValue))
              .drop(1)
              .map(convertLine(_, totalSize))
          }

          EColl.fromSource(source,
                           coverageFiles.head.name,
                           1024 * 1024 * 50,
                           parallelism = resourceAllocated.cpu)

    }

}
