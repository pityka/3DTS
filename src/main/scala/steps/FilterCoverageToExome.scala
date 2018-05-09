package sd.steps

import sd._
import tasks.upicklesupport._
import tasks._
import tasks.queue.NodeLocalCache
import fileutils._

case class FilterCoverageInput(coverage: JsDump[GenomeCoverage],
                               gencodeGtf: SharedFile)

object FilterCoverageToExome {

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
