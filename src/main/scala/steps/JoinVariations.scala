package sd.steps

import sd._
import tasks._
import tasks.collection._
import tasks.upicklesupport._
import fileutils._

case class JoinVariationsInput(
    gnomadExome: SharedFile,
    gnomadGenome: SharedFile,
    mappedProteinCoding: JsDump[sd.JoinGencodeToUniprot.MapResult],
    gnomadExomeCoverage: JsDump[GenomeCoverage],
    gnomadGenomeCoverage: JsDump[GenomeCoverage],
    gencodeGtf: SharedFile)

object JoinVariations {

  val countMissense = EColl.foldLeft[LocusVariationCountAndNumNs, Long](
    "count-missense-variation-1",
    1)(0L, {
    case (count, locus) =>
      if (locus.alleleCountNonSyn > 0) count + 1
      else count
  })

  val filterMissense =
    EColl.filter[LocusVariationCountAndNumNs]("filter-missense-variation-1", 1)(
      _.alleleCountNonSyn > 0)

  val filterSynonymous =
    EColl.filter[LocusVariationCountAndNumNs]("filter-synonymous-variation-1",
                                              1)(_.alleleCountSyn > 0)

  val countSynonymous = EColl.foldLeft[LocusVariationCountAndNumNs, Long](
    "count-synonymous-variation-1",
    1)(0L, {
    case (count, locus) =>
      if (locus.alleleCountSyn > 0) count + 1
      else count
  })

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

  val siteFrequencySpectrum =
    EColl.foldLeft[LocusVariationCountAndNumNs, Map[Int, Int]](
      "sitefrequencyspectrum",
      1)(
      Map.empty[Int, Int], {
        case (acc, locus) =>
          val ac = locus.alleleCountSyn + locus.alleleCountNonSyn
          acc.get(ac) match {
            case None    => acc.updated(ac, 1)
            case Some(c) => acc.updated(ac, c + 1)
          }
      }
    )

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
