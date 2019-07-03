package sd.steps

import sd._
import tasks._
import tasks.ecoll._
import tasks.jsonitersupport._
import fileutils._
import akka.stream.scaladsl.Sink

case class JoinVariationsInput(
    gnomadExome: SharedFile,
    gnomadGenome: SharedFile,
    mappedProteinCoding: EColl[sd.JoinGencodeToUniprot.MapResult],
    gnomadExomeCoverage: EColl[GenomeCoverage],
    gnomadGenomeCoverage: EColl[GenomeCoverage],
    gencodeGtf: SharedFile)

object JoinVariationsInput {
  import com.github.plokhotnyuk.jsoniter_scala.core._
  import com.github.plokhotnyuk.jsoniter_scala.macros._
  implicit val codec: JsonValueCodec[JoinVariationsInput] =
    JsonCodecMaker.make[JoinVariationsInput](CodecMakerConfig())
}

object JoinVariations {

  val countMissense = EColl.foldConstant("count-missense-variation-1", 1, 0L)(
    spore[(Long, LocusVariationCountAndNumNs), Long] {
      case (count, locus) =>
        if (locus.alleleCountNonSyn > 0) count + 1
        else count
    })

  val filterMissense =
    EColl.filter("filter-missense-variation-1", 1)(
      spore((_: LocusVariationCountAndNumNs).alleleCountNonSyn > 0))

  val filterSynonymous =
    EColl.filter("filter-synonymous-variation-1", 1)(
      (_: LocusVariationCountAndNumNs).alleleCountSyn > 0)

  val countSynonymous =
    EColl.foldConstant("count-synonymous-variation-1", 1, 0L)(
      spore[(Long, LocusVariationCountAndNumNs), Long] {
        case (count, locus) =>
          if (locus.alleleCountSyn > 0) count + 1
          else count
      })

  val siteFrequencySpectrum =
    EColl.foldConstant("sitefrequencyspectrum", 1, Map.empty[Int, Int])(
      spore[(Map[Int, Int], LocusVariationCountAndNumNs), Map[Int, Int]] {
        case (acc, locus) =>
          val ac = locus.alleleCountSyn + locus.alleleCountNonSyn
          acc.get(ac) match {
            case None    => acc.updated(ac, 1)
            case Some(c) => acc.updated(ac, c + 1)
          }
      })

  def iteratorSink[T] = Sink.queue[T].mapMaterializedValue { queue =>
    val it = new Iterator[T] {
      var nextElementFuture = queue.pull
      var nextElement: Option[T] = None

      def hasNext: Boolean = {
        nextElement = scala.concurrent.Await
          .result(nextElementFuture, scala.concurrent.duration.Duration.Inf)
        nextElement.isDefined
      }

      def next(): T = {
        val next = nextElement.get
        nextElementFuture = queue.pull
        next
      }

    }
    val close = () => queue.cancel
    (it, close)
  }

  val task =
    AsyncTask[JoinVariationsInput, EColl[LocusVariationCountAndNumNs]](
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
          implicit val mat = ctx.components.actorMaterializer
          for {
            // mappedL <- mapped.sf.file
            exomeL <- exome.file
            genomeL <- genome.file
            gencodeL <- gencode.file
            // exomeCovL <- exomeCov.sf.file
            // genomeCovL <- genomeCov.sf.file
            result <- {
              log.info("Reading interval trees")
              val exons = openSource(gencodeL) { gencodeSource =>
                JoinVariationsCore.exomeIntervalTree(gencodeSource)
              }
              log.info("Interval trees done")
              val (mappedIter2, mappedIter2Close) =
                mapped.source(1).runWith(iteratorSink)
              val (exomeCovIter, exomeCovIterClose) =
                exomeCov.source(1).runWith(iteratorSink)
              val (genomeCovIter, genomeCovIterClose) =
                genomeCov.source(1).runWith(iteratorSink)
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
                      List("exome" -> exomeCovIter, "genome" -> genomeCovIter),
                      1000000)((chr, bp) =>
                      !JoinVariationsCore.lookup(chr, bp, exons).isEmpty)
                  log.info("Joined iterator start")
                  EColl
                    .fromIterator(joinedIter,
                                  mapped.basename + ".variationdata.json.gz")
                    .map { x =>
                      closeable.close
                      mappedIter2Close()
                      exomeCovIterClose()
                      genomeCovIterClose()
                      log.info("Join done, uploading.")
                      x
                    }
                }

              }

            }

          } yield result

    }

}
