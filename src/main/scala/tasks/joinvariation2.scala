import java.io.File
import collection.JavaConversions._
import scala.sys.process._
import scala.concurrent._
import scala.concurrent.duration._
import tasks._
import tasks.collection._
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
import scala.collection.mutable.ArrayBuffer

case class JoinVariationsInput(
    gnomadExome: SharedFile,
    gnomadGenome: SharedFile,
    mappedProteinCoding: JsDump[Ensembl2Uniprot.MapResult],
    gnomadExomeCoverage: JsDump[GenomeCoverage],
    gnomadGenomeCoverage: JsDump[GenomeCoverage],
    gencodeGtf: SharedFile)

object joinVariations {

  val toEColl =
    AsyncTask[JsDump[LocusVariationCountAndNumNs],
              EColl[LocusVariationCountAndNumNs]](
      "convertjoindvariations-ecoll-1",
      1) {
      case js =>
        implicit ctx =>
          EColl.fromSource(js.source,
                           js.sf.name,
                           1024 * 1024 * 50,
                           parallelism = resourceAllocated.cpu)

    }

  val task =
    AsyncTask[JoinVariationsInput, JsDump[LocusVariationCountAndNumNs]](
      "joinvariation-1",
      3) {

      case JoinVariationsInput(exome,
                               genome,
                               mapped,
                               exomeCov,
                               genomeCov,
                               gencode) =>
        implicit ctx =>
          log.info("Start joining variation files ")
          for {
            mappedL <- mapped.sf.file
            exomeL <- exome.file
            genomeL <- genome.file
            gencodeL <- gencode.file
            exomeCovL <- exomeCov.sf.file
            genomeCovL <- genomeCov.sf.file
            result <- {
              log.info("Reading interval trees")
              val exons = openSource(gencodeL) { gencodeSource =>
                JoinVariationsCore.exomeIntervalTree(gencodeSource)
              }
              log.info("Interval trees done")
              mapped.iterator(mappedL) { mappedIter2 =>
                exomeCov.iterator(exomeCovL) { exomeCovIter =>
                  genomeCov.iterator(genomeCovL) { genomeCovIter =>
                    openSource(exomeL) { exSource =>
                      openSource(genomeL) { geSource =>
                        val mappedIter =
                          JoinVariationsCore.readMappedFile(mappedIter2)
                        val exIter =
                          JoinVariationsCore.readGnomad(exSource)
                        val geIter =
                          JoinVariationsCore.readGnomad(geSource)
                        val (joinedIter, closeable) =
                          JoinVariationsCore.join(
                            mappedIter,
                            List(("exome", exIter), ("genome", geIter)),
                            List("exome" -> exomeCovIter,
                                 "genome" -> genomeCovIter),
                            1000000)((chr, bp) =>
                            !JoinVariationsCore.lookup(chr, bp, exons).isEmpty)
                        log.info("Joined iterator start")
                        JsDump
                          .fromIterator[LocusVariationCountAndNumNs](
                            joinedIter,
                            mapped.sf.name + ".variationdata.json.gz")
                          .map { x =>
                            closeable.close
                            log.info("Join done, uploading.")
                            x
                          }
                      }

                    }
                  }
                }
              }

            }

          } yield result

    }

}
