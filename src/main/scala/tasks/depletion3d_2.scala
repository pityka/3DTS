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

case class Depletion3dInput(
    locusData: EColl[LocusVariationCountAndNumNs],
    feature: EColl[Feature2CPSecond.MappedFeatures],
    fasta: SharedFile,
    fai: SharedFile,
    heptamerNeutralRates: HeptamerRates)
object depletion3d {

  def makeScores(
      lociByCpra: Map[ChrPos, LocusVariationCountAndNumNs],
      name: String,
      features: Seq[(ChrPos, FeatureKey, Seq[UniId])],
      pSyn: Double,
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

            val countSInFullChain =
              lociInThisPdbChain.count(x => x.alleleCountSyn > 0)
            val numSInFullChain = lociInThisPdbChain.map(_.numS).toArray
            val sampleSizeSInFullChain =
              lociInThisPdbChain.map(_.sampleSize).toArray

            val exclude = countSInFullChain == 0

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
                appoximationPoissonWithNsWithRounds(sizes = numNsGrouped,
                                                    p = pSyn)
              val expectedSInFullChain =
                appoximationPoissonWithNsWithRounds(sizes =
                                                      numSGroupedInFullChain,
                                                    p = pSyn)

              val postMean =
                posteriorUnderSelection1D(numNs, sampleSizeNs, countNs, pSyn)

              val synSizeInFullChain: Seq[(Int, Int, Int)] =
                (numSInFullChain zip sampleSizeSInFullChain)
                  .groupBy(x => x)
                  .map(x => (x._1._1, x._1._2, x._2.size))
                  .toList
              val pSynByHeptamer = loci.map { locus =>
                val cp = locus.locus
                val heptamer =
                  HeptamerHelpers.heptamerAt(cp, referenceSequence).get

                heptamerNeutralRates(heptamer)
              }.toArray

              val postMeanHepta =
                posteriorUnderSelection1D(numNs,
                                          sampleSizeNs,
                                          countNs,
                                          pSynByHeptamer)
              val row =
                (feature,
                 ObsNs(countNs),
                 ExpNs(expectedNs),
                 ObsS(countSInFullChain),
                 ExpS(expectedSInFullChain),
                 NumLoci(loci.size),
                 NsPostMeanGlobalRate(postMean),
                 NsPostMeanHeptamerSpecificRate(postMeanHepta),
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
      .sortBy(_._2.head._10.v)
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
                         x._9)
          }
      }

    tableContent

  }

  def cacheLocusData(locusDataJsDump: EColl[LocusVariationCountAndNumNs])(
      implicit ctx: TaskSystemComponents,
      ec: ExecutionContext) =
    NodeLocalCache.getItemAsync("lociByCpra" + locusDataJsDump.sf.toString) {
      locusDataJsDump.sf.file.map { locusDataFileLocal =>
        val locusData: List[LocusVariationCountAndNumNs] =
          locusDataJsDump.iterator(locusDataFileLocal)(_.toList.distinct)
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

  val sortByPdbId =
    AsyncTask[JsDump[Feature2CPSecond.MappedFeatures],
              JsDump[Feature2CPSecond.MappedFeatures]]("sortbypdbid-1", 2) {
      myFeatureJsDump => implicit ctx =>
        myFeatureJsDump.sf.file.flatMap { featureFileLocal =>
          type T1 = Feature2CPSecond.MappedFeatures
          implicit val ord: Ordering[T1] = Ordering.by(_._1.pdbId.s)
          myFeatureJsDump.iterator(featureFileLocal) {
            (featureIter: Iterator[T1]) =>
              log.info("Sorting..")
              val (sorted: Iterator[T1], closeable) =
                iterator.sortAsJson(featureIter, 5000000)
              JsDump
                .fromIterator(sorted, myFeatureJsDump.sf.name + ".sorted.js.gz")
                .andThen { case _ => closeable.close }
          }
        }
    }

  val groupByPdbId =
    AsyncTask[JsDump[Feature2CPSecond.MappedFeatures],
              Set[(Int, JsDump[Feature2CPSecond.MappedFeatures])]](
      "groupbypdbid-1",
      2) { myFeatureJsDump => implicit ctx =>
      implicit val mat = ctx.components.actorMaterializer

      log.info("grouping")
      releaseResources
      sortByPdbId(myFeatureJsDump)(CPUMemoryRequest(1, 15000)).flatMap {
        sortedFeatureJsDump =>
          sortedFeatureJsDump.sf.file.flatMap { featureFileLocal =>
            type T1 = Feature2CPSecond.MappedFeatures
            log.info("sort done")
            val (sorted, close) =
              sortedFeatureJsDump.createIterator(featureFileLocal)

            val grouped: Iterator[(Seq[Seq[T1]], Int)] =
              iterator
                .spansByProjectionEquality(sorted)(_._1.pdbId.s)
                .grouped(20)
                .zipWithIndex
            Source
              .fromIterator(() => grouped)
              .mapAsync(1) {
                case (group, idx) =>
                  log.info("Creating subtask " + idx)
                  log.info(group.flatten.size.toString)
                  JsDump
                    .fromIterator(
                      group.flatten.iterator,
                      myFeatureJsDump.sf.name + "." + idx + ".js.gz")
                    .map { dumpedData =>
                      (idx, dumpedData)
                    }
              }
              .runWith(Sink.seq)
              .map(_.toSet)
              .andThen { case _ => close.close }
          }

      }
    }

  val task =
    AsyncTask[Depletion3dInputFull, Depletion3dOutput]("depletion3d-7", 8) {
      case Depletion3dInputFull(locusDataJsDump,
                                myFeatureJsDump,
                                fasta,
                                fai,
                                heptamerFrequencies) =>
        implicit ctx =>
          implicit val mat = ctx.components.actorMaterializer

          releaseResources
          val f1 = groupByPdbId(myFeatureJsDump)(CPUMemoryRequest(1, 60000))
          f1.flatMap {
              (groups: Set[(Int, JsDump[Feature2CPSecond.MappedFeatures])]) =>
                Source(groups.toList)
                  .mapAsync(15) {
                    case (idx, group) =>
                      subtask(
                        Depletion3dInput(locusDataJsDump,
                                         group,
                                         idx,
                                         fasta,
                                         fai,
                                         heptamerFrequencies))(
                        CPUMemoryRequest(1, 8000))
                  }
                  .runWith(Sink.seq)
            }
            .flatMap { (groups: Seq[Depletion3dOutput]) =>
              log.info("Subtasks done. Start concatenating.")
              val cattedLocusFile = Future
                .sequence(groups.map(_.locusFile.file))
                .map(files =>
                  openFileWriter { writer =>
                    files.foreach { locusFile =>
                      openSource(locusFile)(_.getLines.foreach { line =>
                        writer.write(line + "\n")
                      })
                    }

                  }._1)

              val cattedJsDump = Source(groups.map(_.js).toList)
                .flatMapConcat(_.source)
                .runWith(JsDump.sink[DepletionRow](
                  name = "full." + locusDataJsDump.sf.name + "." + myFeatureJsDump.sf.name + ".json.gz"))

              cattedJsDump.flatMap { f1 =>
                cattedLocusFile.flatMap { f2 =>
                  SharedFile(
                    f2,
                    name = "full." + locusDataJsDump.sf.name + "." + myFeatureJsDump.sf.name + ".loci.tsv")
                    .map { f2 =>
                      Depletion3dOutput(f2, f1)
                    }
                }
              }
            }

    }

  val subtask =
    AsyncTask[Depletion3dInput, Depletion3dOutput]("scorebatch-1", 7) {
      case Depletion3dInput(locusDataJsDump,
                            myFeatureJsDump,
                            idx,
                            fasta,
                            fai,
                            heptamerRatesF) =>
        implicit ctx =>
          log.info(
            s"Start scoring ${locusDataJsDump.sf.name} ${myFeatureJsDump.sf.name}")
          for {
            featureFileLocal <- myFeatureJsDump.sf.file
            fastaLocal <- fasta.file
            faiLocal <- fai.file
            heptamerRatesLocal <- heptamerRatesF.sf.file
            (lociByCpra, pSyn) <- cacheLocusData(locusDataJsDump)
            result <- {

              val referenceSequence =
                HeptamerHelpers.openFasta(fastaLocal, faiLocal)

              val heptamerRates =
                openSource(heptamerRatesLocal)(HeptamerHelpers.readRates)

              val features: Seq[(ChrPos, FeatureKey, Seq[UniId])] =
                myFeatureJsDump.iterator(featureFileLocal)(_.map(x =>
                  (x._2, x._1, x._5)).toList)

              log.info("pSyn: " + pSyn)

              val (locusFile, content) =
                makeScores(lociByCpra,
                           "3ds-all-proteomewide",
                           features,
                           pSyn,
                           referenceSequence,
                           heptamerRates)

              log.info("Scores done")
              val jsdump = JsDump.fromIterator(
                content.toList.iterator,
                idx + "." + locusDataJsDump.sf.name + "." + myFeatureJsDump.sf.name + ".json.gz")

              SharedFile(
                locusFile,
                name = idx + "." + locusDataJsDump.sf.name + "." + myFeatureJsDump.sf.name + ".loci.tsv")
                .flatMap(locusFile =>
                  jsdump.map(js => Depletion3dOutput(locusFile, js)))

            }
          } yield result

    }

}
