package sd.steps

import sd._
import scala.concurrent._
import tasks._
import tasks.collection._
import tasks.queue.NodeLocalCache
import tasks.upicklesupport._
import fileutils._
import stringsplit._
import Model._
import akka.stream.scaladsl._
import htsjdk.samtools.reference._
import com.typesafe.scalalogging.StrictLogging

object depletion3d extends StrictLogging {

  def computeDepletionScores(
      locusData: EColl[LocusVariationCountAndNumNs],
      features: EColl[JoinFeatureWithCp.MappedFeatures],
      fasta: SharedFile,
      fai: SharedFile,
      heptamerNeutralRates: HeptamerRates,
      heptamerIndependentIntergenicRate: HeptamerIndependentIntergenicRate,
      chromosomeSpecificHeptamerRates: Map[String, HeptamerRates],
      chromosomeSpecificHeptamerIndependentRates: Map[
        String,
        HeptamerIndependentIntergenicRate])(
      implicit tc: tasks.TaskSystemComponents,
      ec: ExecutionContext) =
    for {
      sorted <- sortByPdbId(features)(CPUMemoryRequest(12, 5000))
      grouped <- groupSortedByPdbId(sorted)(CPUMemoryRequest(12, 5000))
      unused <- groupByPdbId(features)(CPUMemoryRequest(12, 5000))
      mapped <- computeScores(
        (grouped,
         Depletion3dInput(locusData,
                          fasta,
                          fai,
                          heptamerNeutralRates,
                          heptamerIndependentIntergenicRate,
                          chromosomeSpecificHeptamerRates,
                          chromosomeSpecificHeptamerIndependentRates)))(
        CPUMemoryRequest(1, 5000))
    } yield mapped

  def makeScores(
      lociByCpra: Map[ChrPos, LocusVariationCountAndNumNs],
      features: Seq[(ChrPos, FeatureKey, Seq[UniId])],
      pSynonymous: Double,
      referenceSequence: IndexedFastaSequenceFile,
      heptamerNeutralRates: Map[String, Double],
      heptamerIndependentIntergenicRate: HeptamerIndependentIntergenicRate,
      chromosomeSpecificHeptamerRates: Map[String, Map[String, Double]],
      chromosomeSpecificHeptamerIndependentRates: Map[
        String,
        HeptamerIndependentIntergenicRate]): List[DepletionRow] = {

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

          val countSInFullChain =
            lociInThisPdbChain.count(x => x.alleleCountSyn > 0)

          val exclude = {
            countSInFullChain == 0 || loci.exists(
              _.locus.cp.s.split1('\t').head == "chrX")
          }

          if (!exclude) {
            logger.info(s"Scoring $feature")

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

            val postChromosomeIndependentHeptamerSpecific = {
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

            val postChromosomeSpecificHeptamerSpecific = {
              val pIntergenicByHeptamerByChromosome = loci.map { locus =>
                val cp = locus.locus
                val chr = cp.s.split1('\t').head
                val heptamer =
                  HeptamerHelpers.heptamerAt(cp, referenceSequence).get

                chromosomeSpecificHeptamerRates(chr)(heptamer)
              }.toArray

              posteriorUnderSelection1D(numNs,
                                        sampleSizeNs,
                                        countNs,
                                        pIntergenicByHeptamerByChromosome)
            }

            val postChromosomeSpecificHeptamerIndependent = {
              val pIntergenicByChromosome = loci.map { locus =>
                val cp = locus.locus
                val chr = cp.s.split1('\t').head

                chromosomeSpecificHeptamerIndependentRates(chr).v
              }.toArray

              posteriorUnderSelection1D(numNs,
                                        sampleSizeNs,
                                        countNs,
                                        pIntergenicByChromosome)
            }

            val postChromosomeIndependentHeptamerIndependentIntergenicRate =
              posteriorUnderSelection1D(numNs,
                                        sampleSizeNs,
                                        countNs,
                                        heptamerIndependentIntergenicRate.v)

            val row =
              (feature,
               ObsNs(countNs),
               ExpNs(expectedNs),
               ObsS(countSInFullChain),
               ExpS(expectedSInFullChain),
               NumLoci(loci.size),
               NsPostGlobalSynonymousRate(postMeanAgainstSynonymousRate),
               NsPostHeptamerSpecificIntergenicRate(
                 postChromosomeIndependentHeptamerSpecific),
               NsPostHeptamerIndependentIntergenicRate(
                 postChromosomeIndependentHeptamerIndependentIntergenicRate),
               NsPostHeptamerSpecificChromosomeSpecificIntergenicRate(
                 postChromosomeSpecificHeptamerSpecific),
               NsPostHeptamerIndependentChromosomeSpecificIntergenicRate(
                 postChromosomeSpecificHeptamerIndependent),
               unis)

            Some(row)
          } else {
            logger.info(s"Excloding $feature from scoring")

            None
          }
      }
      .filter(_.isDefined)
      .map(_.get)
      .toList

    val tableContent: List[DepletionRow] = estimates
      .groupBy(x => x._1.pdbId.s -> x._1.pdbChain.s)
      .toList
      .sortBy(_._2.head._8.post.mean)
      .reverse
      .flatMap {
        case (_, features) =>
          features.map { x =>
            DepletionRow(x._1,
                         x._2,
                         x._3,
                         x._4,
                         x._5,
                         x._6,
                         x._7,
                         x._8,
                         x._9,
                         x._10,
                         x._11,
                         x._12)
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
      .groupBy[JoinFeatureWithCp.MappedFeatures](
        "groupMappedFeaturesByPdbId",
        1)(1024 * 1024 * 10, _._1.pdbId.s, Some(1))

  val sortByPdbId =
    EColl
      .sortBy[JoinFeatureWithCp.MappedFeatures]("sortMappedFeaturesByPdbId", 1)(
        1024 * 1024 * 10,
        _._1.pdbId.s)

  val groupSortedByPdbId =
    EColl
      .groupBySorted[JoinFeatureWithCp.MappedFeatures](
        "groupBySortedMappedFeaturesByPdbId",
        1)(1024 * 1024 * 10, _._1.pdbId.s)

  case class Depletion3dInput(
      locusData: EColl[LocusVariationCountAndNumNs],
      fasta: SharedFile,
      fai: SharedFile,
      heptamerNeutralRates: HeptamerRates,
      heptamerIndependentIntergenicRate: HeptamerIndependentIntergenicRate,
      chromosomeSpecificHeptamerRates: Map[String, HeptamerRates],
      chromosomeSpecificHeptamerIndependentRates: Map[
        String,
        HeptamerIndependentIntergenicRate])

  val computeScores = EColl
    .mapSourceWith[Seq[JoinFeatureWithCp.MappedFeatures],
                   Depletion3dInput,
                   DepletionRow]("depletion3d", 10)(_ => "") {
      case (source,
            Depletion3dInput(loci,
                             fasta,
                             fai,
                             heptamerRates,
                             heptamerIndependentIntergenicRate,
                             chromosomeSpecificHeptamerRates,
                             chromosomeSpecificHeptamerIndependentRates)) =>
        implicit ctx =>
          val futureSoure = for {
            fastaLocal <- fasta.file
            faiLocal <- fai.file
            heptamerRatesLocal <- heptamerRates.sf.file
            chromosomeSpecificHeptamerRatesLocal <- Future.sequence(
              chromosomeSpecificHeptamerRates.map {
                case (chr, hpt) =>
                  hpt.sf.file.map(file => (chr, file))
              })
            (lociByCpra, pSyn) <- cacheLocusData(loci)
          } yield {

            val referenceSequence =
              HeptamerHelpers.openFasta(fastaLocal, faiLocal)

            val heptamerRates =
              openSource(heptamerRatesLocal)(HeptamerHelpers.readRates)

            val chromosomeSpecificHeptamerRates =
              chromosomeSpecificHeptamerRatesLocal.map {
                case (chr, file) =>
                  (chr, openSource(file)(HeptamerHelpers.readRates))
              }.toMap

            source.mapConcat { mappedFeatures =>
              val features: Seq[(ChrPos, FeatureKey, Seq[UniId])] =
                mappedFeatures.map(x => (x._2, x._1, x._5))

              makeScores(
                lociByCpra,
                features,
                pSyn,
                referenceSequence,
                heptamerRates,
                heptamerIndependentIntergenicRate,
                chromosomeSpecificHeptamerRates,
                chromosomeSpecificHeptamerIndependentRates
              )
            }

          }

          Source.fromFuture(futureSoure).flatMapConcat(x => x)
    }

}
