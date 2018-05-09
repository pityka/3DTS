package sd.steps

import sd._
import java.io.File
import collection.JavaConversions._
import scala.sys.process._
import scala.concurrent._
import scala.concurrent.duration._
import tasks._
import tasks.queue.NodeLocalCache
import tasks.util.TempFile
import tasks.upicklesupport._

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

case class FilterVariantsInput(gnomad: JsDump[JoinVariationsCore.GnomadLine],
                               tpe: String,
                               gencodeGtf: SharedFile)

case class FilterGnomadOutput(f: SharedFile) extends ResultWithSharedFiles(f)

object FilterVariantsToExome {

  val task =
    AsyncTask[FilterVariantsInput, FilterGnomadOutput]("filtergnomad-1", 1) {

      case FilterVariantsInput(genome, _, gencode) =>
        implicit ctx =>
          log.info("Start filtering " + genome.sf.name + " to " + gencode.name)
          for {
            gencodeL <- gencode.file
            exons <- NodeLocalCache.getItem("intervaltrees" + gencode) {
              openSource(gencodeL) { gencodeSource =>
                JoinVariationsCore.exomeIntervalTree(gencodeSource)
              }
            }
            result <- {
              log.info("Reading interval trees")

              log.info("Interval trees done")

              val transformedSource = genome.source
                .filter(
                  gnomadLine =>
                    !JoinVariationsCore
                      .lookup(gnomadLine.chromosome, gnomadLine.position, exons)
                      .isEmpty)
                .map { gl =>
                  ByteString(upickle.default.write(gl) + "\n")
                }
                .via(Compression.gzip)

              SharedFile(transformedSource,
                         genome.sf.name + ".filter." + gencode.name).map(x =>
                FilterGnomadOutput(x))

            }

          } yield result

    }

}
