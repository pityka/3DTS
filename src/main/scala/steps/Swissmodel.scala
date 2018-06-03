package sd.steps

import sd._
import scala.concurrent._
import tasks._
import tasks.upicklesupport._
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream
import org.apache.commons.compress.utils.IOUtils
import java.io.FileInputStream
import stringsplit._
import fileutils._
import akka.stream.scaladsl._
import scala.util._

case class SwissModelPdbFiles(pdbFiles: Map[PdbId, SharedFile])
    extends ResultWithSharedFiles(pdbFiles.map(_._2).toList: _*)

object Swissmodel {

  val defineSecondaryFeaturesWithDSSP =
    AsyncTask[(PdbId, SharedFile), JsDump[JoinUniprotWithPdb.T2]]("dssp-1", 1) {
      case (swissmodelPdbId, swissmodelPdbFile) =>
        implicit ctx =>
          for {
            swissmodelPdbFileLocal <- swissmodelPdbFile.file
            result <- {
              val dsspExecutable = TempFile.getExecutableFromJar("mkdssp")
              val command = List(dsspExecutable.getAbsolutePath,
                                 "-i",
                                 swissmodelPdbFileLocal.getAbsolutePath)
              val (stdout, _, _) =
                execGetStreamsAndCodeWithLog(command,
                                             unsuccessfulOnErrorStream = false)

              val dsspFeatures = stdout
                .dropWhile(_.startsWith("#  RESIDUE"))
                .drop(1)
                .filterNot(_.contains("!*"))
                .map { dataLine =>
                  val spl = dataLine.splitM(Set('\t', ' '))
                  val residueNumber1Based = spl.drop(5).take(4).mkString.toInt
                  val chain = spl.drop(9).take(2).mkString.trim
                  val structureCode = spl(17).head
                  log.info(
                    residueNumber1Based + " " + chain + " " + structureCode + "///" + dataLine + "///")

                  val feature = structureCode match {
                    case 'H' | 'G' | 'I' => "dssp_helix"
                    case 'T'             => "dssp_turn"
                    case 'E' | 'B'       => "dssp_strand"
                    case _               => "dssp_other"
                  }
                  (chain, residueNumber1Based, feature)

                }
                .groupBy { case (chain, _, _) => chain }
                .map {
                  case (chain, residues) =>
                    val linearFeatures =
                      residues.foldLeft(List[(Int, Int, String)]()) {
                        case (Nil, (_, residueNumber1Based, feature)) =>
                          List(
                            (residueNumber1Based - 1,
                             residueNumber1Based,
                             feature))
                        case (
                            (currentStart0, currentEnd0Open, currentFeature) :: previousFeatures,
                            (_, residueNumber1Based, feature)) =>
                          if (residueNumber1Based - 1 == currentEnd0Open && currentFeature == feature) {
                            (currentStart0, residueNumber1Based, currentFeature) :: previousFeatures
                          } else
                            (residueNumber1Based - 1,
                             residueNumber1Based,
                             currentFeature) :: (currentStart0,
                                                 currentEnd0Open,
                                                 currentFeature) :: previousFeatures

                      }

                    (chain, linearFeatures)
                }

              val features = dsspFeatures.map {
                case (chain, features) =>
                  (UniId(swissmodelPdbId.s.split1('_').head),
                   swissmodelPdbId,
                   PdbChain(chain),
                   features.map {
                     case (start, end, feature) =>
                       (UniprotFeatureName(feature),
                        ((start + 1) to end)
                          .map(i => PdbResidueNumber(i, None))
                          .toSet)
                   }.toSet)

              }

              JsDump.fromIterator(
                features.iterator,
                "dsspfeatures." + swissmodelPdbId.s + ".js.gz")
            }
          } yield result

    }

  val filterMetaData =
    AsyncTask[SharedFile, SwissModelPdbFiles]("filterswissmodel-1", 1) {
      metadata => implicit ctx =>
        implicit val mat = ctx.components.actorMaterializer

        log.info("Start parsing swissmodel metadata")
        for {
          metadataLocal <- metadata.file
          result <- {
            val tarInput =
              new TarArchiveInputStream(new FileInputStream(metadataLocal))
            val index = Iterator
              .continually(tarInput.getNextTarEntry)
              .find(_.getName == "INDEX")
              .get
            val data = Array.ofDim[Byte](index.getSize.toInt)
            IOUtils.readFully(tarInput, data)
            tarInput.close
            val indexString = new String(data, "UTF-8")
            val urls = scala.io.Source
              .fromString(indexString)
              .getLines
              .dropWhile(_.startsWith("#"))
              .drop(1)
              .map { line =>
                val spl = line.split1('\t')
                val uniID = spl(0)
                val hash = spl(3)
                val from = spl(5).toInt
                val to = spl(6).toInt
                val template = spl(8)
                val qmean = spl(9).toDouble
                val url = spl(11)
                val filename = uniID + "_" + from + "_" + to + "_" + template + "_" + hash
                (filename, qmean, url)
              }
              .filter { case (_, qmean, _) => qmean >= -4.0 }
              .map { case (filename, _, url) => (filename, url) }
              .toList

            log.info(s"Will download ${urls.size} files from swissmodel.")

            Source(urls)
              .mapAsync(resourceAllocated.cpu) {
                case (filename, url) =>
                  Try(
                    AssemblyToPdb.retry(3)(
                      scalaj.http
                        .Http(url)
                        .timeout(connTimeoutMs = 1000 * 60 * 10,
                                 readTimeoutMs = 1000 * 60 * 10)
                        .asBytes
                        .body)) match {
                    case Success(pdbData) =>
                      SharedFile(writeToTempFile(new String(pdbData, "UTF-8")),
                                 filename)
                        .map(s => Some(PdbId(filename) -> s))
                    case Failure(e) =>
                      log.error(e, "Failed fetch swissmodel data from " + url)
                      Future.successful(None)
                  }

              }
              .filter(_.isDefined)
              .map(_.get)
              .runWith(Sink.seq)
              .map { downloadedFiles =>
                SwissModelPdbFiles(downloadedFiles.toMap)
              }
          }
        } yield result

    }

}
