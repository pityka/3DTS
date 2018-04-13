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
      EColl.partitionsFromSource(js.source, js.sf.name, 8)
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
            .map { frame =>
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
            .runWith(JsDump.sink[GenomeCoverage](
              coverageFile.name + "genomecoverage.js.gz"))

    }

}
