package sd.steps

import sd._
import tasks._
import tasks.queue.NodeLocalCache
import tasks.jsonitersupport._
import fileutils._
import akka.stream.scaladsl._
import akka.util._

case class FilterVariantsInput(gnomad: JsDump[JoinVariationsCore.GnomadLine],
                               tpe: String,
                               gencodeGtf: SharedFile)

case class FilterGnomadOutput(f: SharedFile)

object FilterGnomadOutput {
  import com.github.plokhotnyuk.jsoniter_scala.core._
  import com.github.plokhotnyuk.jsoniter_scala.macros._
  implicit val codec: JsonValueCodec[FilterGnomadOutput] =
    JsonCodecMaker.make[FilterGnomadOutput](CodecMakerConfig())
}

object FilterVariantsInput {
  import com.github.plokhotnyuk.jsoniter_scala.core._
  import com.github.plokhotnyuk.jsoniter_scala.macros._
  implicit val codec: JsonValueCodec[FilterVariantsInput] =
    JsonCodecMaker.make[FilterVariantsInput](CodecMakerConfig())
}

object FilterVariantsToExome {
  import com.github.plokhotnyuk.jsoniter_scala.core._
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
              log.info("Interval trees done")

              val transformedSource = genome.source
                .filter(
                  gnomadLine =>
                    !JoinVariationsCore
                      .lookup(gnomadLine.chromosome, gnomadLine.position, exons)
                      .isEmpty)
                .map { gl =>
                  ByteString(writeToString(gl) + "\n")
                }
                .via(Compression.gzip)

              SharedFile(transformedSource,
                         genome.sf.name + ".filter." + gencode.name).map(x =>
                FilterGnomadOutput(x))

            }

          } yield result

    }

}
