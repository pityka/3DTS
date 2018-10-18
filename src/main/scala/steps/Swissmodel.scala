package sd.steps

import sd._
import scala.concurrent._
import tasks._
import tasks.upicklesupport._
import stringsplit._
import fileutils._
import akka.stream.scaladsl._
import scala.util._

case class SwissModelMetaDataInput(swissModelMetadata: SharedFile,
                                   uniprot: SharedFile)

case class SwissModelPdbFiles(pdbFiles: Map[PdbId, SharedFile])
    extends ResultWithSharedFiles(pdbFiles.map(_._2).toList: _*)

object Swissmodel {

  val defineSecondaryFeaturesWithDSSP =
    AsyncTask[(PdbId, SharedFile), JsDump[JoinUniprotWithPdb.T2]]("dssp-1", 3) {
      case (swissmodelPdbId, swissmodelPdbFile) =>
        implicit ctx =>
          for {
            swissmodelPdbFileLocal <- swissmodelPdbFile.file
            result <- {
              val dsspExecutable = TempFile.getExecutableFromJar("/mkdssp")
              val command = List(dsspExecutable.getAbsolutePath,
                                 "-i",
                                 swissmodelPdbFileLocal.getAbsolutePath)
              val (stdout, _, _) =
                execGetStreamsAndCode(command,
                                      unsuccessfulOnErrorStream = false)

              val dsspFeatures = stdout
                .dropWhile(line => !line.startsWith("  #"))
                .drop(1)
                .filterNot(_.contains("!"))
                .map { dataLine =>
                  val residueNumber1Based = dataLine.substring(5, 10).trim.toInt
                  val chain = dataLine.substring(10, 12).trim
                  val structureCode: Char = dataLine(16)

                  val feature = structureCode match {
                    case 'H' | 'G' | 'I' => "dssp_helix_" + structureCode
                    case 'T'             => "dssp_turn_" + structureCode
                    case 'E' | 'B'       => "dssp_strand" + structureCode
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
                             feature) :: (currentStart0,
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

  val fakeUniprotPdbMappingFromSwissmodel =
    AsyncTask[SwissModelPdbFiles, JsDump[JoinUniprotWithPdb.T1]](
      "swissmodel-uniprot-pdb-map-1",
      1) {
      case SwissModelPdbFiles(swissmodelPdbs) =>
        implicit ctx =>
          implicit val mat = ctx.components.actorMaterializer
          Source(swissmodelPdbs.toList)
            .mapAsync(resourceAllocated.cpu) {
              case (swissModelPdbId, pdbFile) =>
                val uniId = UniId(swissModelPdbId.s.split1('_').head)
                pdbFile.source.runFold(akka.util.ByteString())(_ ++ _).map {
                  data =>
                    val atomlist = PdbHelper
                      .parsePdbLines(scala.io.Source
                        .fromString(data.utf8String)
                        .getLines)
                      .assembly
                      .atoms
                    atomlist
                      .map {
                        case AtomWithLabels(_, pdbChain, pdbResidueNumber, _) =>
                          (uniId,
                           swissModelPdbId,
                           pdbChain,
                           pdbResidueNumber.toUnresolved,
                           PdbNumber(-1),
                           PdbSeq(""),
                           UniNumber(pdbResidueNumber.num - 1),
                           UniSeq("?"),
                           true)
                      }
                      .distinct
                      .map { x =>
                        log.debug(s"Swissmodel uni-pdb mapping: $x")
                        x
                      }
                }
            }
            .mapConcat(identity)
            .runWith(JsDump.sink(
              swissmodelPdbs.hashCode + ".swissmodeluniprotmap.js.gz"))
    }

  val filterMetaData =
    AsyncTask[SwissModelMetaDataInput, SwissModelPdbFiles]("filterswissmodel-2",
                                                           3) {
      case SwissModelMetaDataInput(metadata, uniprot) =>
        implicit ctx =>
          implicit val mat = ctx.components.actorMaterializer

          log.info("Start parsing swissmodel metadata")
          for {
            metadataLocal <- metadata.file
            uniprotLocal <- uniprot.file
            result <- {
              val isoforms =
                openSource(uniprotLocal)(IOHelpers.readUniProtIsoforms)
              log.info("Uniprot displayed isoforms: " + isoforms.size)
              isoforms.foreach {
                case (a, b) =>
                  log.debug(s"Uniprot displayed isoforms $a $b")
              }

              val urls = IOHelpers
                .readSwissmodelMetadata(metadataLocal, isoforms)
                .map {
                  case (filename, _, url, _, _, _) => (filename, url)
                }
                .toList

              log.info(s"Will download ${urls.size} files from swissmodel.")

              Source(urls)
                .mapAsync(resourceAllocated.cpu) {
                  case (filename, url) =>
                    log.debug(s"try $filename $url")
                    Try(
                      AssemblyToPdb.retry(3)(
                        scalaj.http
                          .Http(url)
                          .timeout(connTimeoutMs = 1000 * 60 * 10,
                                   readTimeoutMs = 1000 * 60 * 10)
                          .asBytes
                          .body)) match {
                      case Success(pdbData) =>
                        log.debug(s"OK $filename $url")
                        SharedFile(
                          writeToTempFile(new String(pdbData, "UTF-8")),
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
