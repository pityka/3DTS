package sd.steps

import sd._
import tasks._
import tasks.ecoll._
import tasks.queue.NodeLocalCache
import tasks.jsonitersupport._
import fileutils._
import tasks.util.AkkaStreamComponents
import akka.util._

case class FilterVariantsInput(gnomad: EColl[JoinVariationsCore.GnomadLine],
                               tpe: String,
                               gencodeGtf: SharedFile)

case class FilterGnomadOutput(f: SharedFile)

object FilterGnomadOutput {
  import com.github.plokhotnyuk.jsoniter_scala.core._
  import com.github.plokhotnyuk.jsoniter_scala.macros._
  implicit val codec: JsonValueCodec[FilterGnomadOutput] =
    JsonCodecMaker.make[FilterGnomadOutput](sd.JsonIterConfig.config)
}

object FilterVariantsInput {
  import com.github.plokhotnyuk.jsoniter_scala.core._
  import com.github.plokhotnyuk.jsoniter_scala.macros._
  implicit val codec: JsonValueCodec[FilterVariantsInput] =
    JsonCodecMaker.make[FilterVariantsInput](sd.JsonIterConfig.config)
}

object FilterVariantsToExome {
  import com.github.plokhotnyuk.jsoniter_scala.core._
  val task =
    AsyncTask[FilterVariantsInput, FilterGnomadOutput]("filtergnomad-1", 1) {

      case FilterVariantsInput(genome, _, gencode) =>
        implicit ctx =>
          log.info("Start filtering " + genome.basename + " to " + gencode.name)
          for {
            gencodeL <- gencode.file
            exons <- NodeLocalCache.getItem("intervaltrees" + gencode) {
              openSource(gencodeL) { gencodeSource =>
                JoinVariationsCore.exomeIntervalTree(gencodeSource)
              }
            }

            result <- {
              log.info("Interval trees done")

              val transformedSource = genome
                .source(resourceAllocated.cpu)
                .filter(
                  gnomadLine =>
                    !JoinVariationsCore
                      .lookup(gnomadLine.chromosome, gnomadLine.position, exons)
                      .isEmpty)
                .map { gl =>
                  ByteString(writeToString(gl) + "\n")
                }
                .via(AkkaStreamComponents.gzip(1, 1024 * 512))

              SharedFile(transformedSource,
                         genome.basename + ".filter." + gencode.name).map(x =>
                FilterGnomadOutput(x))

            }

          } yield result

    }

}
