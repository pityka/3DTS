package sd.steps

import sd._
import tasks.jsonitersupport._
import tasks._
import tasks.ecoll._
import tasks.queue.NodeLocalCache
import fileutils._

case class FilterCoverageInput(coverage: EColl[GenomeCoverage],
                               gencodeGtf: SharedFile)

object FilterCoverageInput {
  import com.github.plokhotnyuk.jsoniter_scala.core._
  import com.github.plokhotnyuk.jsoniter_scala.macros._
  implicit val codec: JsonValueCodec[FilterCoverageInput] =
    JsonCodecMaker.make[FilterCoverageInput](sd.JsonIterConfig.config)
}

object FilterCoverageToExome {

  val task =
    AsyncTask[FilterCoverageInput, EColl[GenomeCoverage]]("filtercoverage-1", 1) {

      case FilterCoverageInput(coverage, gencode) =>
        implicit ctx =>
          implicit val mat = ctx.components.actorMaterializer
          log.info(
            "Start filtering " + coverage.basename + " to " + gencode.name)
          for {
            gencodeL <- gencode.file
            exons <- NodeLocalCache.getItem("intervaltrees" + gencode) {
              openSource(gencodeL) { gencodeSource =>
                JoinVariationsCore.exomeIntervalTree(gencodeSource)
              }
            }
            result <- {
              log.info("Interval trees done")

              coverage
                .source(resourceAllocated.cpu)
                .filter(
                  coverage =>
                    !JoinVariationsCore
                      .lookup(coverage.chromosome, coverage.position, exons)
                      .isEmpty)
                .runWith(EColl.sink[GenomeCoverage](
                  name = coverage.basename + ".filter." + gencode.name))

            }

          } yield result

    }

}
