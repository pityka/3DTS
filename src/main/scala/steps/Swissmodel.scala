// package sd.steps

// import sd._
// import tasks._
// import tasks.jsonitersupport._
// import stringsplit._
// import fileutils._
// import akka.stream.scaladsl._
// import scala.util._
// import tasks.ecoll._

// case class SwissModelMetaDataInput(swissModelMetadata: SharedFile,
//                                    uniprot: SharedFile)

// object SwissModelMetaDataInput {
//   import com.github.plokhotnyuk.jsoniter_scala.core._
//   import com.github.plokhotnyuk.jsoniter_scala.macros._
//   implicit val codec: JsonValueCodec[SwissModelMetaDataInput] =
//     JsonCodecMaker.make[SwissModelMetaDataInput](sd.JsonIterConfig.config)
// }

// case class SwissModelPdbEntry(id: PdbId, data: String)
// object SwissModelPdbEntry {
//   import com.github.plokhotnyuk.jsoniter_scala.core._
//   import com.github.plokhotnyuk.jsoniter_scala.macros._
//   implicit val codec: JsonValueCodec[SwissModelPdbEntry] =
//     JsonCodecMaker.make[SwissModelPdbEntry](sd.JsonIterConfig.config)
//   implicit val serde = tasks.makeSerDe[SwissModelPdbEntry]
// }

// case class SwissModelPdbFiles(pdbFiles: EColl[SwissModelPdbEntry])

// object SwissModelPdbFiles {
//   import com.github.plokhotnyuk.jsoniter_scala.core._
//   import com.github.plokhotnyuk.jsoniter_scala.macros._
//   implicit val codec: JsonValueCodec[SwissModelPdbFiles] =
//     JsonCodecMaker.make[SwissModelPdbFiles](sd.JsonIterConfig.config)
// }

// object Swissmodel {

//   val dsspExecutable = TempFile.getExecutableFromJar("/mkdssp")

//   val rundssp =
//     AsyncTask[SwissModelPdbFiles, EColl[JoinUniprotWithPdb.T2]](
//       "swissmodel-dssp-1",
//       1) {
//       case SwissModelPdbFiles(swissmodelPdbs) =>
//         implicit ctx =>
//           implicit val mat = ctx.components.actorMaterializer
//           releaseResources
//           swissmodelPdbs
//             .source(resourceAllocated.cpu)
//             .mapAsync(10) { entry =>
//               defineSecondaryFeaturesWithDSSP(entry)(ResourceRequest(1, 1000))
//             }
//             .runWith(Sink.seq)
//             .map(_.reduce(_ ++ _))
//     }

//   val defineSecondaryFeaturesWithDSSP =
//     AsyncTask[SwissModelPdbEntry, EColl[JoinUniprotWithPdb.T2]]("dssp-1", 3) {
//       case SwissModelPdbEntry(swissmodelPdbId, swissmodelPdbData) =>
//         implicit ctx =>
//           val swissmodelPdbFileLocal = writeToTempFile(swissmodelPdbData)

//           val command = List(dsspExecutable.getAbsolutePath,
//                              "-i",
//                              swissmodelPdbFileLocal.getAbsolutePath)
//           val (stdout, _, _) =
//             execGetStreamsAndCode(command, unsuccessfulOnErrorStream = false)

//           val dsspFeatures = stdout
//             .dropWhile(line => !line.startsWith("  #"))
//             .drop(1)
//             .filterNot(_.contains("!"))
//             .map { dataLine =>
//               val residueNumber1Based = dataLine.substring(5, 10).trim.toInt
//               val chain = dataLine.substring(10, 12).trim
//               val structureCode: Char = dataLine(16)

//               val feature = structureCode match {
//                 case 'H' | 'G' | 'I' => "dssp_helix_" + structureCode
//                 case 'T'             => "dssp_turn_" + structureCode
//                 case 'E' | 'B'       => "dssp_strand" + structureCode
//                 case _               => "dssp_other"
//               }
//               (chain, residueNumber1Based, feature)

//             }
//             .groupBy { case (chain, _, _) => chain }
//             .map {
//               case (chain, residues) =>
//                 val linearFeatures =
//                   residues.foldLeft(List[(Int, Int, String)]()) {
//                     case (Nil, (_, residueNumber1Based, feature)) =>
//                       List(
//                         (residueNumber1Based - 1, residueNumber1Based, feature))
//                     case ((currentStart0, currentEnd0Open, currentFeature) :: previousFeatures,
//                           (_, residueNumber1Based, feature)) =>
//                       if (residueNumber1Based - 1 == currentEnd0Open && currentFeature == feature) {
//                         (currentStart0, residueNumber1Based, currentFeature) :: previousFeatures
//                       } else
//                         (residueNumber1Based - 1, residueNumber1Based, feature) :: (currentStart0,
//                                                                                     currentEnd0Open,
//                                                                                     currentFeature) :: previousFeatures

//                   }

//                 (chain, linearFeatures)
//             }

//           val features = dsspFeatures.map {
//             case (chain, features) =>
//               JoinUniprotWithPdb.T2(
//                 UniId(swissmodelPdbId.s.split1('_').head),
//                 swissmodelPdbId,
//                 PdbChain(chain),
//                 features.map {
//                   case (start, end, feature) =>
//                     (UniprotFeatureName(feature),
//                      ((start + 1) to end)
//                        .map(i => PdbResidueNumber(i, None))
//                        .toSet)
//                 }.toSet
//               )

//           }

//           EColl.fromIterator(features.iterator,
//                              "dsspfeatures." + swissmodelPdbId.s + ".js.gz",
//                              parallelism = resourceAllocated.cpu)

//     }

//   val fakeUniprotPdbMappingFromSwissmodel =
//     AsyncTask[SwissModelPdbFiles, EColl[JoinUniprotWithPdb.T1]](
//       "swissmodel-uniprot-pdb-map-1",
//       1) {
//       case SwissModelPdbFiles(swissmodelPdbs) =>
//         implicit ctx =>
//           implicit val mat = ctx.components.actorMaterializer
//           swissmodelPdbs
//             .source(resourceAllocated.cpu)
//             .map {
//               case SwissModelPdbEntry(swissModelPdbId, pdbFileAsString) =>
//                 val uniId = UniId(swissModelPdbId.s.split1('_').head)

//                 val atomlist = PdbHelper
//                   .parsePdbLines(
//                     scala.io.Source
//                       .fromString(pdbFileAsString)
//                       .getLines)
//                   .assembly
//                   .atoms

//                 val mapping = atomlist
//                   .map {
//                     case AtomWithLabels(_, pdbChain, pdbResidueNumber, _) =>
//                       JoinUniprotWithPdb.T1(uniId,
//                                             swissModelPdbId,
//                                             pdbChain,
//                                             pdbResidueNumber.toUnresolved,
//                                             PdbNumber(-1),
//                                             PdbSeq(""),
//                                             UniNumber(pdbResidueNumber.num - 1),
//                                             UniSeq("?"),
//                                             true)
//                   }
//                 log.debug(s"Swissmodel uni-pdb mapping: $mapping")
//                 mapping

//             }
//             .mapConcat(identity)
//             .runWith(EColl.sink(
//               swissmodelPdbs.hashCode + ".swissmodeluniprotmap.js.gz",
//               parallelism = resourceAllocated.cpu))
//     }

//   val filterMetaData =
//     AsyncTask[SwissModelMetaDataInput, SwissModelPdbFiles]("filterswissmodel-2",
//                                                            4) {
//       case SwissModelMetaDataInput(metadata, uniprot) =>
//         implicit ctx =>
//           implicit val mat = ctx.components.actorMaterializer

//           log.info("Start parsing swissmodel metadata")
//           for {
//             metadataLocal <- metadata.file
//             uniprotLocal <- uniprot.file
//             result <- {
//               val isoforms =
//                 openSource(uniprotLocal)(IOHelpers.readUniProtIsoforms)
//               log.info("Uniprot displayed isoforms: " + isoforms.size)
//               isoforms.foreach {
//                 case (a, b) =>
//                   log.debug(s"Uniprot displayed isoforms $a $b")
//               }

//               log.info("Swissmodel metadata file at " + metadataLocal)

//               val urls = IOHelpers
//                 .readSwissmodelMetadata(metadataLocal, isoforms)
//                 .map {
//                   case (filename, _, url, _, _, _) => (filename, url)
//                 }
//                 .toList

//               log.info(s"Will download ${urls.size} files from swissmodel.")

//               Source(urls)
//                 .map {
//                   case (filename, url) =>
//                     log.debug(s"try $filename $url")
//                     Try(
//                       AssemblyToPdb.retry(3)(
//                         scalaj.http
//                           .Http(url)
//                           .timeout(connTimeoutMs = 1000 * 60 * 10,
//                                    readTimeoutMs = 1000 * 60 * 10)
//                           .asBytes
//                           .body)) match {
//                       case Success(pdbData) =>
//                         log.debug(s"OK $filename $url")
//                         Some(SwissModelPdbEntry(PdbId(filename),
//                                                 new String(pdbData, "UTF-8")))
//                       case Failure(e) =>
//                         log.error(e, "Failed fetch swissmodel data from " + url)
//                         (None)
//                     }

//                 }
//                 .filter(_.isDefined)
//                 .map(_.get)
//                 .runWith(EColl.sink(
//                   "filterswissmodel." + metadata.name + "." + uniprot.name + ".js.gz",
//                   parallelism = resourceAllocated.cpu))
//                 .map { downloadedFiles =>
//                   SwissModelPdbFiles(downloadedFiles)
//                 }
//             }
//           } yield result

//     }

// }
