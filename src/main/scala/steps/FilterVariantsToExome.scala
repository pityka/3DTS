package sd.steps

import sd._
import tasks._
import tasks.queue.NodeLocalCache
import tasks.upicklesupport._
import fileutils._
import akka.stream.scaladsl._
import akka.util._

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
