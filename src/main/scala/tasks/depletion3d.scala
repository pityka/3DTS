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

import fileutils._
import stringsplit._

import IOHelpers._
import MathHelpers._
import Model._

import akka.stream._
import akka.stream.scaladsl._
import SharedTypes._

import htsjdk.samtools.reference._

object depletion3d {

  def computeDepletionScores(locusData: EColl[LocusVariationCountAndNumNs],
                             features: EColl[Feature2CPSecond.MappedFeatures],
                             fasta: SharedFile,
                             fai: SharedFile,
                             heptamerNeutralRates: HeptamerRates)(
      implicit tc: tasks.TaskSystemComponents,
      ec: ExecutionContext) =
    for {
      sorted <- sortByPdbId(features)(CPUMemoryRequest(12, 5000))
      grouped <- groupSortedByPdbId(sorted)(CPUMemoryRequest(12, 5000))
      unused <- groupByPdbId(features)(CPUMemoryRequest(12, 5000))
      mapped <- computeScores(
        grouped,
        Depletion3dInput(locusData, fasta, fai, heptamerNeutralRates))(
        CPUMemoryRequest(1, 5000))
    } yield mapped

  def makeScores(
      lociByCpra: Map[ChrPos, LocusVariationCountAndNumNs],
      name: String,
      features: Seq[(ChrPos, FeatureKey, Seq[UniId])],
      pSynonymous: Double,
      referenceSequence: IndexedFastaSequenceFile,
      heptamerNeutralRates: Map[String, Double]): List[DepletionRow] = {

    val lociByPdbChain: Map[(PdbId, PdbChain), Vector[ChrPos]] = features
      .groupBy(x => x._2.pdbId -> x._2.pdbChain)
      .map(x => x._1 -> x._2.map(_._1).toVector)

    val estimates = features
      .groupBy(_._2)
      .toSeq
      .filter(_._2.map(_._1).distinct.size < 300)
      // .take(100)
      .map {
        case (feature: FeatureKey, cpras) =>
          val loci: Seq[LocusVariationCountAndNumNs] =
            cpras.map(_._1).distinct.flatMap(cp => lociByCpra.get(cp))

          val lociInThisPdbChain: Seq[LocusVariationCountAndNumNs] =
            lociByPdbChain(feature.pdbId -> feature.pdbChain)
              .flatMap(cp => lociByCpra.get(cp))
              .distinct

          val unis = cpras.map(_._3).head

          val countNs = loci.count(x => x.alleleCountNonSyn > 0)
          val numNs = loci.map(_.numNs).toArray
          val sampleSizeNs = loci.map(_.sampleSize).toArray

          val exclude = {
            val countSInFullChain =
              lociInThisPdbChain.count(x => x.alleleCountSyn > 0)

            countSInFullChain == 0
          }

          if (!exclude) {

            // ns, numLoci, rounds
            val numNsGrouped: Seq[(Int, Int, Int)] = loci
              .groupBy(x => (x.numNs, x.sampleSize))
              .toSeq
              .map(x => (x._1._1, x._2.size, x._1._2))

            val numSGroupedInFullChain: Seq[(Int, Int, Int)] =
              lociInThisPdbChain
                .groupBy(x => (x.numS, x.sampleSize))
                .toSeq
                .map(x => (x._1._1, x._2.size, x._1._2))

            val expectedNs =
              predictionPoissonWithNsWithRounds(sizes = numNsGrouped,
                                                p = pSynonymous)
            val expectedSInFullChain =
              predictionPoissonWithNsWithRounds(sizes = numSGroupedInFullChain,
                                                p = pSynonymous)

            val postMeanAgainstSynonymousRate =
              posteriorUnderSelection1D(numNs,
                                        sampleSizeNs,
                                        countNs,
                                        pSynonymous)

            val postMeanHepta = {
              val pIntergenicByHeptamer = loci.map { locus =>
                val cp = locus.locus
                val heptamer =
                  HeptamerHelpers.heptamerAt(cp, referenceSequence).get

                heptamerNeutralRates(heptamer)
              }.toArray

              posteriorUnderSelection1D(numNs,
                                        sampleSizeNs,
                                        countNs,
                                        pIntergenicByHeptamer)
            }
            val row =
              (feature,
               ObsNs(countNs),
               ExpNs(expectedNs),
               ObsS(countSInFullChain),
               ExpS(expectedSInFullChain),
               NumLoci(loci.size),
               NsPostMeanGlobalSynonymousRate(postMeanAgainstSynonymousRate),
               NsPostMeanHeptamerSpecificIntergenicRate(postMeanHepta),
               unis)

            Some(row)
          } else None
      }
      .filter(_.isDefined)
      .map(_.get)
      .toList
      .sortBy(_._8.v)
      .reverse

    val tableContent: List[DepletionRow] = estimates
      .groupBy(x => x._1.pdbId.s -> x._1.pdbChain.s)
      .toList
      .sortBy(_._2.head._8.v)
      .reverse
      .flatMap {
        case (_, features) =>
          features.map { x =>
            DepletionRow(x._1, x._2, x._3, x._4, x._5, x._6, x._7, x._8, x._9)
          }
      }

    tableContent

  }

  def cacheLocusData(locusDataJsDump: EColl[LocusVariationCountAndNumNs])(
      implicit ctx: TaskSystemComponents,
      ec: ExecutionContext) =
    NodeLocalCache.getItemAsync("lociByCpra" + locusDataJsDump.hashCode) {
      implicit val am = ctx.actorMaterializer
      locusDataJsDump.source(3).runWith(Sink.seq).map(_.distinct).map {
        locusData: Seq[LocusVariationCountAndNumNs] =>
          assert(locusData.map(_.locus).distinct.size == locusData.size)
          val lociByCpra: Map[ChrPos, LocusVariationCountAndNumNs] =
            locusData.map(x => x.locus -> x).toMap
          val synonymousVariantCount: Int =
            locusData.count(x => x.alleleCountSyn > 0)
          // s, numLoci, rounds
          val totalSynonymousSizes: Seq[(Int, Int, Int)] = {
            val mmap = scala.collection.mutable.Map[(Int, Int), Int]()

            locusData.foreach { locus =>
              val numS = locus.numS
              val sampleSize = locus.sampleSize
              mmap.get((numS, sampleSize)) match {
                case None    => mmap.update((numS, sampleSize), 1)
                case Some(x) => mmap.update((numS, sampleSize), x + 1)
              }

            }
            mmap.toSeq.map(x => (x._1._1, x._2, x._1._2))
          }

          val totalSynSize = totalSynonymousSizes.map(_._2).sum

          println("total syn sizes " + totalSynSize)

          val pSyn =
            solveForPWithNsWithRounds(size = totalSynonymousSizes,
                                      successes = synonymousVariantCount)

          println("pSyn: " + pSyn)
          (lociByCpra -> pSyn)
      }
    }

  val groupByPdbId =
    EColl
      .groupBy[Feature2CPSecond.MappedFeatures](
        "groupMappedFeaturesByPdbId",
        1)(1024 * 1024 * 10, _._1.pdbId.s, Some(1))

  val sortByPdbId =
    EColl
      .sortBy[Feature2CPSecond.MappedFeatures]("sortMappedFeaturesByPdbId", 1)(
        1024 * 1024 * 10,
        _._1.pdbId.s)

  val groupSortedByPdbId =
    EColl
      .groupBySorted[Feature2CPSecond.MappedFeatures](
        "groupBySortedMappedFeaturesByPdbId",
        1)(1024 * 1024 * 10, _._1.pdbId.s)

  case class Depletion3dInput(locusData: EColl[LocusVariationCountAndNumNs],
                              fasta: SharedFile,
                              fai: SharedFile,
                              heptamerNeutralRates: HeptamerRates)

  val computeScores = EColl
    .mapSourceWith[Seq[Feature2CPSecond.MappedFeatures],
                   Depletion3dInput,
                   DepletionRow]("depletion3d", 8) {
      case (source, Depletion3dInput(loci, fasta, fai, heptamerRates)) =>
        implicit ctx =>
          implicit val mat = ctx.components.actorMaterializer
          val futureSoure = for {
            fastaLocal <- fasta.file
            faiLocal <- fai.file
            heptamerRatesLocal <- heptamerRates.sf.file
            (lociByCpra, pSyn) <- cacheLocusData(loci)
          } yield {

            val referenceSequence =
              HeptamerHelpers.openFasta(fastaLocal, faiLocal)

            val heptamerRates =
              openSource(heptamerRatesLocal)(HeptamerHelpers.readRates)

            source.mapConcat { mappedFeatures =>
              val features: Seq[(ChrPos, FeatureKey, Seq[UniId])] =
                mappedFeatures.map(x => (x._2, x._1, x._5))

              makeScores(lociByCpra,
                         "3ds-all-proteomewide",
                         features,
                         pSyn,
                         referenceSequence,
                         heptamerRates)
            }

          }

          Source.fromFuture(futureSoure).flatMapConcat(x => x)
    }

}