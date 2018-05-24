package sd.steps

import sd._
import java.io.File
import scala.concurrent._
import tasks._
import tasks.queue.NodeLocalCache
import tasks.upicklesupport._
import tasks.collection._
import akka.stream.scaladsl.Source
import JoinVariationsCore.{GnomadLine}

case class HeptamerRates(sf: SharedFile)

object CountHeptamers {

  val filterTask =
    EColl.mapSourceWith[GenomeCoverage, SharedFile, GenomeCoverage](
      "filterCoverageToInterGenic",
      1) {
      case (coverageSource, gencode) =>
        implicit ctx =>
          val futureSource = for {
            gencodeL <- gencode.file
            exons <- NodeLocalCache.getItem("intervaltrees" + gencode) {
              fileutils.openSource(gencodeL) { gencodeSource =>
                JoinVariationsCore.exomeIntervalTree(gencodeSource)
              }
            }
          } yield {
            log.info("Interval trees done")

            coverageSource
              .filter(
                coverage =>
                  JoinVariationsCore
                    .lookup(coverage.chromosome, coverage.position, exons)
                    .isEmpty)

          }

          Source.fromFuture(futureSource).flatMapConcat(identity)
    }

  val joinGnomadGenomeCoverageWithGnomadDataTask =
    EColl.outerJoinBy2[GenomeCoverage, GnomadLine](
      "joinGnomadCoverageWithData",
      1)(1024 * 1024 * 50, _.cp, _.cp, Some(1))

  def calculateHeptamer(coverage: EColl[GenomeCoverage],
                        calls: EColl[GnomadLine],
                        fasta: SharedFile,
                        fai: SharedFile,
                        gencode: SharedFile)(
      implicit tc: tasks.TaskSystemComponents,
      ec: ExecutionContext): Future[(HeptamerRates, GlobalIntergenicRate)] = {
    for {
      filteredCoverage <- filterTask((coverage, gencode))(
        CPUMemoryRequest(12, 5000))
      joined <- joinGnomadGenomeCoverageWithGnomadDataTask(
        (filteredCoverage, calls))(CPUMemoryRequest((1, 12), 5000))
      mapped <- mapTask((joined, (fasta, fai)))(CPUMemoryRequest(1, 5000))
      grouped <- groupByTask(mapped)(CPUMemoryRequest((1, 12), 5000))
      summed <- sum(grouped)(CPUMemoryRequest(1, 5000))
      concatenated <- concatenate(summed)(CPUMemoryRequest((1, 12), 5000))
      globalRate <- globalNeutralRate(concatenated)(CPUMemoryRequest(1, 5000))
      _ <- heptamerCounts(summed)(CPUMemoryRequest(1, 5000))
      rates <- heptamerNeutralRates(summed)(CPUMemoryRequest(1, 5000))
      file <- heptamerFrequenciesToFile(rates)(CPUMemoryRequest(1, 5000))
    } yield (file, globalRate)
  }

  val groupByTask =
    EColl
      .groupBy[(String, (Seq[Int], Seq[Int], Int))]("groupHeptamer", 1)(
        1024 * 1024 * 50,
        _._1,
        Some(1))

  val sum =
    EColl.reduceSeq[(String, (Seq[Int], Seq[Int], Int))]("sumHeptamer", 1) {
      seq =>
        val a = seq.map(_._2._1).flatten
        val b = seq.map(_._2._2).flatten
        val heptamerCount = seq.map(_._2._3).sum
        assert(seq.map(_._1).toSet.size == 1)
        assert(heptamerCount == a.size,
               a.size.toString + " != " + heptamerCount)
        assert(heptamerCount == b.size,
               a.size.toString + " != " + heptamerCount)
        (seq.head._1, (a, b, heptamerCount))
    }

  val heptamerCounts = EColl
    .map[(String, (Seq[Int], Seq[Int], Int)), (String, Int)]("hepCounts", 1) {
      case (hep, (_, _, c)) => (hep, c)
    }

  val mapTask =
    EColl
      .mapSourceWith[(Option[GenomeCoverage], Option[GnomadLine]),
                     (SharedFile, SharedFile),
                     (String, (Seq[Int], Seq[Int], Int))]("mapHeptamer", 2) {
        case (variants, (fasta, fai)) =>
          implicit ctx =>
            implicit val mat = ctx.components.actorMaterializer

            val countTable: Future[List[(String, (Seq[Int], List[Int], Int))]] =
              for {
                fasta <- fasta.file
                fai <- fai.file
                reference = HeptamerHelpers.openFasta(fasta, fai)
                counts <- {

                  val mutable =
                    scala.collection.mutable
                      .AnyRefMap[String, (List[Int], List[Int], Int)]()

                  variants
                    .collect {
                      case (Some(cov), calls) => (cov, calls)
                    }
                    .runFold(mutable) {
                      case (mutable,
                            (coverage: GenomeCoverage, maybeVariantCall)) =>
                        val passSNP = maybeVariantCall
                          .filter { x =>
                            x.ref.size == 1 && x.alt.size == 1 && x.filter == "PASS"
                          }
                        val sampleSize =
                          passSNP
                            .map(gl =>
                              gl.genders.male.totalChromosomeCount + gl.genders.female.totalChromosomeCount)
                            .map(_ / 2)
                            .getOrElse(
                              coverage.numberOfWellSequencedIndividuals)

                        val variantAlleleCount = passSNP
                          .map(gl =>
                            gl.genders.male.totalVariantAlleleCount + gl.genders.female.totalVariantAlleleCount)
                          .getOrElse(0)

                        val mayHeptamer =
                          HeptamerHelpers.heptamerAt(coverage.chromosome,
                                                     coverage.position,
                                                     reference)

                        mayHeptamer.failed.foreach { e =>
                          log.warning(s"Heptamer retrieval failed $coverage $e")
                        }
                        mayHeptamer.foreach { heptamer =>
                          if (!heptamer.contains('N')) {

                            passSNP.foreach { passSNP =>
                              assert(passSNP.ref == heptamer(3).toString)
                            }
                            mutable.get(heptamer) match {
                              case None =>
                                mutable.update(heptamer,
                                               (List(sampleSize),
                                                List(variantAlleleCount),
                                                1))
                              case Some(
                                  (sampleSizes,
                                   variantAlleleCounts,
                                   heptamerCount)) =>
                                mutable.update(
                                  heptamer,
                                  (sampleSize :: sampleSizes,
                                   variantAlleleCount :: variantAlleleCounts,
                                   heptamerCount + 1))
                            }
                          }
                        }
                        mutable
                    }
                    .map(_.toList)
                }
              } yield counts

            Source.fromFuture(countTable).flatMapConcat(list => Source(list))

      }
  import akka.stream.scaladsl._
  import akka.util.ByteString
  def zippedTemporaryFileSink(
      implicit ec: ExecutionContext): Sink[ByteString, Future[File]] = {
    val f = tasks.util.TempFile.createTempFile("tmp")
    Flow[ByteString]
      .via(tasks.util.AkkaStreamComponents
        .strictBatchWeighted[ByteString](1024 * 512, _.size.toLong)(_ ++ _))
      .via(Compression.gzip)
      .toMat(FileIO.toPath(f.toPath))(Keep.right)
      .mapMaterializedValue(_.map(_ => f))
  }

  val heptamerFrequenciesToFile =
    AsyncTask[EColl[(String, Double, Int, Seq[Int])], HeptamerRates](
      "writeHeptamerRates",
      3) { ecoll => implicit ctx =>
      implicit val mat = ctx.components.actorMaterializer

      val accepted = Set('A', 'T', 'G', 'C')

      ecoll
        .source(resourceAllocated.cpu)
        .map {
          case (hept, rate, counts, _) =>
            if ((hept.toSet &~ accepted).isEmpty) {
              val str = s"$hept\t$rate\t$counts\n"
              ByteString(str)
            } else {
              ByteString.empty
            }
        }
        .toMat(zippedTemporaryFileSink)(Keep.right)
        .run
        .flatMap { f =>
          SharedFile(f, "heptamerRates." + ecoll.basename)
            .map(HeptamerRates.apply)
        }
    }

  val heptamerNeutralRates =
    EColl.map[(String, (Seq[Int], Seq[Int], Int)),
              (String, Double, Int, Seq[Int])]("heptamerNeutralRates", 1) {
      case (heptamer, (samples, alleleCounts, _)) =>
        val countOfVariableLoci = alleleCounts.count(_ > 0)
        val p =
          Model.mlNeutral(samples.toArray, countOfVariableLoci)
        (heptamer, p, countOfVariableLoci, samples)
    }

  val concatenate =
    EColl.foldLeft[(String, (Seq[Int], Seq[Int], Int)), (Vector[Int], Int)](
      "concatenate",
      2)(
      (Vector[Int](), 0), {
        case ((accumulatedListOfSamples, accumulatedAlleleCounts),
              (hepta, (samples, alleleCounts, _))) =>
          val accepted = Set('A', 'T', 'G', 'C')
          if ((hepta.toSet &~ accepted).isEmpty) {
            println(s"concat $hepta " + accumulatedListOfSamples.size)
            val rnd = new scala.util.Random(1)
            val idx = rnd.shuffle((0 until alleleCounts.size).toList).take(2000)
            val countOfVariableLoci = idx.map(alleleCounts).count(_ > 0)

            (accumulatedListOfSamples ++ idx.map(samples),
             accumulatedAlleleCounts + countOfVariableLoci)
          } else (accumulatedListOfSamples, accumulatedAlleleCounts)
      }
    )

  val globalNeutralRate =
    AsyncTask[(Seq[Int], Int), GlobalIntergenicRate]("globalNeutralRate", 1) {
      case (samples, alleleCounts) =>
        implicit ctx =>
          val g = Model.mlNeutral(samples.toArray, alleleCounts)
          ctx.log.info("Global neutral rate " + g)
          Future.successful(GlobalIntergenicRate(g))

    }

}
