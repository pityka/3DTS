package sd.steps

import sd._
import scala.concurrent._
import tasks._
import tasks.ecoll._
import tasks.queue.NodeLocalCache
import tasks.jsonitersupport._
import fileutils._
import stringsplit._
import Model._
import akka.stream.scaladsl._
import htsjdk.samtools.reference._
import com.typesafe.scalalogging.StrictLogging

object depletion3d extends StrictLogging {

  val formatter = new java.text.DecimalFormat("0.0000000000")
  val sortDoubles =
    EColl.sortBy[Double]("sort-double", 1, 1024 * 1024 * 50)((d: Double) =>
      sd.steps.depletion3d.formatter.format(d))

  val extractCDF =
    EColl.mapWith[Double, Long, Seq[(Double, Double)]]("cdf-1-2", 2, false)(
      spore {
        case (source, length, ctx, _, _) =>
          implicit val mat = ctx.components.actorMaterializer
          val step = 0.001
          Source.fromFuture(
            source.zipWithIndex.runWith(
              Sink.fold(Vector.empty[(Double, Double)]) {
                case (list, (value, index)) =>
                  val nextValue = list.size * step
                  if (nextValue < value)
                    list :+ (value -> index / length.toDouble)
                  else list
              }))
      })

  val take1 = EColl.groupedTotal[(Double, Double)]("cdf-toseq", 1)

  def computeCDF(numbers: EColl[Double])(
      implicit tc: tasks.TaskSystemComponents,
      ec: ExecutionContext): Future[Seq[(Double, Double)]] =
    for {
      sorted <- sortDoubles(numbers)(ResourceRequest(1, 5000))
      cdf <- extractCDF((sorted, sorted.length))(ResourceRequest(1, 5000))
      cdfSeq <- cdf.head
    } yield cdfSeq.get

  val projectPostMeanGlobalSynonymousRate =
    EColl.map("p-global-syn-pm", 1)(
      (_: DepletionRow).nsPostGlobalSynonymousRate.post.mean)

  val projectPostMeanHeptamerSpecificIntergenicRate =
    EColl.map("p-hept-intergen-pm", 1)(
      (_: DepletionRow).nsPostHeptamerSpecificIntergenicRate.post.mean)

  val projectPostMeanHeptamerIndependentIntergenicRate =
    EColl.map("p-global-intergen-pm", 1)(
      (_: DepletionRow).nsPostHeptamerIndependentIntergenicRate.post.mean)

  val projectPostMeanHeptamerSpecificChromosomeSpecificIntergenicRate =
    EColl.map("p-heptchr-intergen-pm", 1)(
      (_: DepletionRow).nsPostHeptamerSpecificChromosomeSpecificIntergenicRate.post.mean)

  val projectPostMeanHeptamerIndependentChromosomeSpecificIntergenicRate =
    EColl.map("p-chr-intergen-pm", 1)(
      (_: DepletionRow).nsPostHeptamerIndependentChromosomeSpecificIntergenicRate.post.mean)

  def computeCDFs(scores: EColl[DepletionRow])(
      implicit tc: tasks.TaskSystemComponents,
      ec: ExecutionContext): Future[DepletionScoreCDFs] = {

    val nsPostMeanGlobalSynonymousRate = for {
      scores <- projectPostMeanGlobalSynonymousRate(scores)(
        ResourceRequest(1, 5000))
      cdf <- computeCDF(scores)
    } yield cdf

    val nsPostMeanHeptamerSpecificIntergenicRate = for {
      scores <- projectPostMeanHeptamerSpecificIntergenicRate(scores)(
        ResourceRequest(1, 5000))
      cdf <- computeCDF(scores)
    } yield cdf
    val nsPostMeanHeptamerIndependentIntergenicRate = for {
      scores <- projectPostMeanHeptamerIndependentIntergenicRate(scores)(
        ResourceRequest(1, 5000))
      cdf <- computeCDF(scores)
    } yield cdf
    val nsPostMeanHeptamerSpecificChromosomeSpecificIntergenicRate = for {
      scores <- projectPostMeanHeptamerSpecificChromosomeSpecificIntergenicRate(
        scores)(ResourceRequest(1, 5000))
      cdf <- computeCDF(scores)
    } yield cdf
    val nsPostMeanHeptamerIndependentChromosomeSpecificIntergenicRate = for {
      scores <- projectPostMeanHeptamerIndependentChromosomeSpecificIntergenicRate(
        scores)(ResourceRequest(1, 5000))
      cdf <- computeCDF(scores)
    } yield cdf

    for {
      nsPostMeanGlobalSynonymousRate <- nsPostMeanGlobalSynonymousRate
      nsPostMeanHeptamerSpecificIntergenicRate <- nsPostMeanHeptamerSpecificIntergenicRate
      nsPostMeanHeptamerIndependentIntergenicRate <- nsPostMeanHeptamerIndependentIntergenicRate
      nsPostMeanHeptamerSpecificChromosomeSpecificIntergenicRate <- nsPostMeanHeptamerSpecificChromosomeSpecificIntergenicRate
      nsPostMeanHeptamerIndependentChromosomeSpecificIntergenicRate <- nsPostMeanHeptamerIndependentChromosomeSpecificIntergenicRate
    } yield
      DepletionScoreCDFs(
        nsPostMeanGlobalSynonymousRate,
        nsPostMeanHeptamerSpecificIntergenicRate,
        nsPostMeanHeptamerIndependentIntergenicRate,
        nsPostMeanHeptamerSpecificChromosomeSpecificIntergenicRate,
        nsPostMeanHeptamerIndependentChromosomeSpecificIntergenicRate
      )

  }

  val cdfs2file =
    AsyncTask[DepletionScoreCDFs, SharedFile]("cdfs-serialized", 2) {
      cdf => implicit ctx =>
        val tmp = IOHelpers.writeCDFs(cdf)
        SharedFile(tmp, "scorecdfs.txt")
    }

  val uniquePdbIds =
    EColl.foldConstant[DepletionRow, Set[PdbId]]("unique-scored-pdbid",
                                                 1,
                                                 Set.empty[PdbId])(
      spore[(Set[PdbId], DepletionRow), Set[PdbId]] {
        case (accumulatedSet, depletionRow) =>
          val pdbId = depletionRow.featureKey.pdbId
          accumulatedSet + pdbId
      }
    )

  val uniqueUniprotIds =
    EColl.foldConstant("unique-scored-uniprotid", 1, Set.empty[UniId])(
      spore[(Set[UniId], DepletionRow), Set[UniId]] {
        case (accumulatedSet, depletionRow) =>
          accumulatedSet ++ depletionRow.uniprotIds
      }
    )

  val repartition =
    EColl.repartition[DepletionRow](Long.MaxValue - 1)

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
      grouped <- groupByPdbId(features)(ResourceRequest(12, 5000))
      mapped <- computeScores(
        (grouped,
         Depletion3dInput(locusData,
                          fasta,
                          fai,
                          heptamerNeutralRates,
                          heptamerIndependentIntergenicRate,
                          chromosomeSpecificHeptamerRates,
                          chromosomeSpecificHeptamerIndependentRates)))(
        ResourceRequest(1, 5000))
    } yield mapped

  def makeScores(
      lociByCpra: Map[ChrPos, LocusVariationCountAndNumNs],
      features: Seq[(ChrPos, FeatureKey, Seq[UniId])],
      pSynonymousAutosomal: Double,
      pSynonymousChrX: Double,
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

          def makePSynArray(ls: Seq[LocusVariationCountAndNumNs]) =
            ls.map { locus =>
              val cp = locus.locus
              val chrX = cp.s.split1('\t').head == "chrX"
              if (chrX) pSynonymousChrX else pSynonymousAutosomal

            }.toArray

          val pSynonymousArray = makePSynArray(loci)

          val unis = cpras.map(_._3).head

          val countNs = loci.count(x => x.alleleCountNonSyn > 0)
          val numNs = loci.map(_.numNs).toArray
          val sampleSizeNs = loci.map(_.sampleSize).toArray

          val countSInFullChain =
            lociInThisPdbChain.count(x => x.alleleCountSyn > 0)

          val exclude = {
            countSInFullChain == 0 || loci.exists(
              _.locus.s.split1('\t').head == "chrX")
          }

          if (!exclude) {
            logger.info(s"Scoring $feature")

            val expectedNs =
              predictionPoissonWithNsWithRounds(lociNumNs = numNs,
                                                lociRounds = sampleSizeNs,
                                                p = pSynonymousArray)

            val expectedSInFullChain =
              predictionPoissonWithNsWithRounds(
                lociNumNs = lociInThisPdbChain.map(_.numS).toArray,
                lociRounds = lociInThisPdbChain.map(_.sampleSize).toArray,
                p = makePSynArray(lociInThisPdbChain))

            val postMeanAgainstSynonymousRate =
              posteriorUnderSelection1D(numNs,
                                        sampleSizeNs,
                                        countNs,
                                        pSynonymousArray)

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
            logger.info(s"uding $feature from scoring")

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

  def calculateSynonymousRate(locusData: Seq[LocusVariationCountAndNumNs]) = {
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

    solveForPWithNsWithRounds(size = totalSynonymousSizes,
                              successes = synonymousVariantCount)
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
          val pSynAutosomal = calculateSynonymousRate(locusData.filter {
            locus =>
              locus.locus.s.split1('\t').head != "chrX"
          })

          val pSynChrX = calculateSynonymousRate(locusData.filter { locus =>
            locus.locus.s.split1('\t').head == "chrX"
          })

          logger.info("pSynAutosomal: " + pSynAutosomal)
          logger.info("pSynChrX: " + pSynChrX)
          (lociByCpra, pSynAutosomal, pSynChrX)
      }
    }

  val groupByPdbId =
    EColl
      .groupBy("groupMappedFeaturesByPdbId", 1)(
        () => Some(1),
        (_: JoinFeatureWithCp.MappedFeatures).featureKey.pdbId.s)

  val sortByPdbId =
    EColl
      .sortBy("sortMappedFeaturesByPdbId", 1, 1024 * 1024 * 10)(
        (_: JoinFeatureWithCp.MappedFeatures).featureKey.pdbId.s)

  // val groupSortedByPdbId =
  //   EColl
  //     .groupBySorted[JoinFeatureWithCp.MappedFeatures](
  //       "groupBySortedMappedFeaturesByPdbId",
  //       1)(1024 * 1024 * 10, _._1.pdbId.s)

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

  object Depletion3dInput {
    import com.github.plokhotnyuk.jsoniter_scala.core._
    import com.github.plokhotnyuk.jsoniter_scala.macros._
    implicit val codec: JsonValueCodec[Depletion3dInput] =
      JsonCodecMaker.make[Depletion3dInput](sd.JsonIterConfig.config)

    implicit val serde = tasks.makeSerDe[Depletion3dInput]
  }

  val computeScores = EColl
    .mapWith[Seq[JoinFeatureWithCp.MappedFeatures],
             Depletion3dInput,
             DepletionRow]("depletion3d", 12, false)(spore {
      case (source,
            Depletion3dInput(loci,
                             fasta,
                             fai,
                             heptamerRates,
                             heptamerIndependentIntergenicRate,
                             chromosomeSpecificHeptamerRates,
                             chromosomeSpecificHeptamerIndependentRates),
            ctx,
            _,
            _) =>
        implicit val ce = ctx
        val futureSoure = for {
          fastaLocal <- fasta.file
          faiLocal <- fai.file
          heptamerRatesLocal <- heptamerRates.sf.file
          chromosomeSpecificHeptamerRatesLocal <- Future.sequence(
            chromosomeSpecificHeptamerRates.map {
              case (chr, hpt) =>
                hpt.sf.file.map(file => (chr, file))
            })
          (lociByCpra, pSynAutosomal, pSynChrX) <- cacheLocusData(loci)
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

          source.mapConcat {
            mappedFeatures =>
              val features: Seq[(ChrPos, FeatureKey, Seq[UniId])] =
                mappedFeatures.map(x => (x.chrPos, x.featureKey, x.uniIds))

              makeScores(
                lociByCpra,
                features,
                pSynAutosomal,
                pSynChrX,
                referenceSequence,
                heptamerRates,
                heptamerIndependentIntergenicRate,
                chromosomeSpecificHeptamerRates,
                chromosomeSpecificHeptamerIndependentRates
              )
          }

        }

        Source.fromFuture(futureSoure).flatMapConcat(x => x)
    })

}
