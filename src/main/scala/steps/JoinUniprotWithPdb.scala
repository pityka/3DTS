package sd.steps

import sd._
import scala.concurrent._
import tasks._
import tasks.ecoll._

import tasks.queue.NodeLocalCache
import tasks.jsonitersupport._
import fileutils._
import akka.stream.scaladsl.Source

case class UniProtPdbInput(
    uniprotKb: SharedFile,
    uniProtId: List[UniId],
    batchName: String,
    genomeUniJoinFile: EColl[sd.JoinGencodeToUniprot.MapResult])

object UniProtPdbInput {
  import com.github.plokhotnyuk.jsoniter_scala.core._
  import com.github.plokhotnyuk.jsoniter_scala.macros._
  implicit val codec: JsonValueCodec[UniProtPdbInput] =
    JsonCodecMaker.make[UniProtPdbInput](CodecMakerConfig())
}

case class UniProtPdbFullInput(
    uniprotKb: SharedFile,
    genomeUniJoinFile: EColl[sd.JoinGencodeToUniprot.MapResult])
object UniProtPdbFullInput {
  import com.github.plokhotnyuk.jsoniter_scala.core._
  import com.github.plokhotnyuk.jsoniter_scala.macros._
  implicit val codec: JsonValueCodec[UniProtPdbFullInput] =
    JsonCodecMaker.make[UniProtPdbFullInput](CodecMakerConfig())
}

case class UniProtPdbFullOutput(
    tables: List[(List[(UniId, PdbId, PdbChain, Int, Int)],
                  EColl[JoinUniprotWithPdb.T1],
                  EColl[JoinUniprotWithPdb.T2],
                  EColl[AlignmentDetails])],
    qc: SharedFile)

object UniProtPdbFullOutput {
  import com.github.plokhotnyuk.jsoniter_scala.core._
  import com.github.plokhotnyuk.jsoniter_scala.macros._
  implicit val codec: JsonValueCodec[UniProtPdbFullOutput] =
    JsonCodecMaker.make[UniProtPdbFullOutput](CodecMakerConfig())
}

case class UniProtPdbOutput(
    tables: List[(List[(UniId, PdbId, PdbChain, Int, Int)],
                  EColl[JoinUniprotWithPdb.T1],
                  EColl[JoinUniprotWithPdb.T2],
                  EColl[AlignmentDetails])])

object UniProtPdbOutput {
  import com.github.plokhotnyuk.jsoniter_scala.core._
  import com.github.plokhotnyuk.jsoniter_scala.macros._
  implicit val codec: JsonValueCodec[UniProtPdbOutput] =
    JsonCodecMaker.make[UniProtPdbOutput](CodecMakerConfig())
}

object JoinUniprotWithPdb {

  case class T1(uniId: UniId,
                pdbId: PdbId,
                pdbChain: PdbChain,
                pdbResidueNumber: PdbResidueNumberUnresolved,
                pdbNumber: PdbNumber,
                pdbSeq: PdbSeq,
                uniNumber: UniNumber,
                uniSeq: UniSeq,
                perfectMatch: Boolean)

  object T1 {
    import com.github.plokhotnyuk.jsoniter_scala.core._
    import com.github.plokhotnyuk.jsoniter_scala.macros._
    implicit val codec: JsonValueCodec[T1] =
      JsonCodecMaker.make[T1](CodecMakerConfig())

    implicit val serde = tasks.makeSerDe[T1]
  }

  case class T2(uniId: UniId,
                pdbId: PdbId,
                pdbChain: PdbChain,
                features: Set[(UniprotFeatureName, Set[PdbResidueNumber])])

  object T2 {
    import com.github.plokhotnyuk.jsoniter_scala.core._
    import com.github.plokhotnyuk.jsoniter_scala.macros._
    implicit val codec: JsonValueCodec[T2] =
      JsonCodecMaker.make[T2](CodecMakerConfig())
  }

  def downloadUniprot2(uniprotkbSF: SharedFile, genomeUniJoin: EColl[UniId])(
      implicit ts: tasks.queue.ComputationEnvironment) = {
    log.info(
      "Downloading uniprot + genome file.." + uniprotkbSF.name + " " + genomeUniJoin.basename)
    uniprotkbSF.file.flatMap { localFile =>
      genomeUniJoin.toSeq(1).map(_.toSet).map { uniIdFilter =>
        log.debug(uniIdFilter.take(10).toString)
        log.info("Read uniprot file.. filter for: " + uniIdFilter.size)
        val uniprotkblist =
          openSource(localFile) { s =>
            val l = IOHelpers.readUniProtFile(s).toList
            log.info("Uniprot entries before pdb isempty filter: " + l.size)
            l.filterNot(_.pdbs.isEmpty)
          }
        log.info(
          "Uniprot after dropping entries without pdb: " + uniprotkblist.size)
        val r = uniprotkblist
          .flatMap(x => x.accessions.map(y => y -> x))
          .filter(x => uniIdFilter.contains(x._1))
          .toMap
        log.info("UniprotId -> UniprotEntry size: " + r.size)
        r
      }
    }
  }

  def downloadUniprot(uniprotkbSF: SharedFile,
                      genomeUniJoin: EColl[sd.JoinGencodeToUniprot.MapResult])(
      implicit ts: tasks.queue.ComputationEnvironment) = {
    implicit val mat = ts.components.actorMaterializer
    log.info(
      "Downloading uniprot + genome file.." + uniprotkbSF.name + " " + genomeUniJoin.basename)
    uniprotkbSF.file.flatMap { localFile =>
      sd.JoinGencodeToUniprot.readUniProtIds(genomeUniJoin.source(1)).run.map {
        uniIdFilter =>
          log.debug(uniIdFilter.take(10).toString)
          log.info("Read uniprot file.. filter for: " + uniIdFilter.size)
          val uniprotkblist =
            openSource(localFile) { s =>
              val l = IOHelpers.readUniProtFile(s).toList
              log.info("Uniprot entries before pdb isempty filter: " + l.size)
              l.filterNot(_.pdbs.isEmpty)
            }
          log.info(
            "Uniprot after dropping entries without pdb: " + uniprotkblist.size)
          val r = uniprotkblist
            .flatMap(x => x.accessions.map(y => y -> x))
            .filter(x => uniIdFilter.contains(x._1))
            .toMap
          log.info("UniprotId -> UniprotEntry size: " + r.size)
          r
      }
    }
  }
  import tasks.ecoll.EColl
  val extractMapppedFeatures =
    AsyncTask[UniProtPdbFullOutput, EColl[MappedUniprotFeature]](
      "uniprot2pdb-extract-features",
      1) { uniProtPdbFullOutput => implicit ctx =>
      val source = Source(uniProtPdbFullOutput.tables.toList)
        .flatMapConcat(_._3.source(1))
        .mapConcat {
          case T2(uni, pdb, chain, featureSet) =>
            featureSet.toList.map {
              case (featureName, residues) =>
                MappedUniprotFeature(uni, pdb, chain, featureName, residues)
            }

        }

      EColl.fromSource(source = source,
                       name = uniProtPdbFullOutput.hashCode + ".mappedfeatures",
                       partitionSize = 1024 * 1024 * 100,
                       parallelism = resourceAllocated.cpu)
    }

  val task =
    AsyncTask[UniProtPdbFullInput, UniProtPdbFullOutput](
      "uniprot2pdb_full_withFeatures",
      18) {

      case UniProtPdbFullInput(
          uniprotkbSF,
          genomeUniJoinFileSF
          ) =>
        implicit ctx =>
          val f =
            NodeLocalCache
              .getItemAsync("uniprotkb" + uniprotkbSF) {
                downloadUniprot(uniprotkbSF, genomeUniJoinFileSF)
              }
              .flatMap { (uniprotKb: Map[UniId, UniProtEntry]) =>
                log.info("Size of uniprotkb {}", uniprotKb.size)

                val batches =
                  uniprotKb.toSeq
                    .sortBy(_._1.s)
                    .grouped(100)
                    .toList
                    .zipWithIndex

                log.info("Number of batches {}", batches.size)

                val futureBatches: Seq[Future[UniProtPdbOutput]] =
                  batches.map {
                    case (seq, idx) =>
                      log.debug("Batch size: {}", seq.size)
                      subtask(
                        UniProtPdbInput(
                          uniprotkbSF,
                          seq.map(_._1).sortBy(_.s).toList,
                          "batch" + idx,
                          genomeUniJoinFileSF))(ResourceRequest(1, 1000))
                  }.toSeq

                Future.sequence(futureBatches).flatMap {
                  (x: Seq[UniProtPdbOutput]) =>
                    val flattened = x.map(_.tables).flatten.toList
                    val ids: Seq[(UniId, PdbId, PdbChain, Int, Int)] =
                      flattened.flatMap(_._1)

                    val qcfile = SharedFile(
                      openFileWriter { writer =>
                        ids.foreach(x =>
                          writer.write(
                            (x._1.s, x._2.s, x._3.s, x._4, x._5).productIterator
                              .mkString("\t") + "\n"))
                      }._1,
                      name = "uni-pdb-chain-qc.tsv"
                    )

                    val uni = ids.map(_._1).distinct.size
                    log.info("Joined {} uniprot files  (total {} uni x pdbs)",
                             uni,
                             ids.size)
                    qcfile.map(qc => UniProtPdbFullOutput(flattened, qc))
                }

              }
          releaseResources
          f
    }

  val subtask =
    AsyncTask[UniProtPdbInput, UniProtPdbOutput]("uniprot2pdb-2", 7) {

      case UniProtPdbInput(
          uniprotkbSF,
          uniIds,
          batchName,
          genomeUniJoinFileSF
          ) =>
        implicit ctx =>
          log.info("Start Uniprot Pdb join for {}", uniIds)
          NodeLocalCache
            .getItemAsync("uniprotkb" + uniprotkbSF) {
              downloadUniprot(uniprotkbSF, genomeUniJoinFileSF)
            }
            .flatMap { (uniprotKb: Map[UniId, UniProtEntry]) =>
              val result = uniIds.flatMap { uniId =>
                log.debug("Join $uniID with PDB")
                val r = try {
                  sd.JoinUniprotWithPdb.joinUniProtWithPdb(uniId, uniprotKb)
                } catch {
                  case e: Exception =>
                    log.error("Failed uni: " + uniId)
                    throw e
                }
                r
              }

              val ids: Seq[(UniId, PdbId, PdbChain, Int, Int)] =
                result.groupBy(x => (x._1, x._2, x._3)).toSeq.map {
                  case (key, list) =>
                    assert(list.size == 1)
                    val mapping = list.head._4
                    val mappedResidues = mapping.size
                    val mismatch = mapping.count(x => !x._4)
                    (key._1, key._2, key._3, mappedResidues, mismatch)
                }

              val iter: Iterator[T1] = result.iterator.flatMap {
                case (uniId,
                      pdbId,
                      pdbChain,
                      mapping,
                      uniSeq,
                      pdbSeq,
                      _,
                      _,
                      _) =>
                  log.debug(s"$uniId - $pdbId - $pdbChain")
                  mapping.iterator.map {
                    case (pdbSeqNum, uniNum, pdbResNum, mat) =>
                      val pdb2 =
                        pdbResNum.num.toString + pdbResNum.insertionCode
                          .getOrElse("")

                      T1(
                        uniId,
                        pdbId,
                        pdbChain,
                        PdbResidueNumberUnresolved(pdb2),
                        PdbNumber(pdbSeqNum.i),
                        PdbSeq(pdbSeq.s(pdbSeqNum.i).toString),
                        UniNumber(uniNum.i),
                        UniSeq(uniSeq.s(uniNum.i).toString),
                        mat
                      )

                  }
              }
              val mapping =
                EColl.fromIterator(iter, name = batchName + ".json.gz")

              val featureMappingIter = result.iterator.map {
                case (uniId, pdbId, pdbChain, _, _, _, _, _, features) =>
                  T2(uniId, pdbId, pdbChain, features.toSet)
              }

              val featureMapping =
                EColl.fromIterator(featureMappingIter,
                                   name = batchName + ".features.json.gz")

              val alignmentData = EColl.fromIterator(
                result.iterator.map {
                  case (uniId,
                        pdbId,
                        pdbChain,
                        _,
                        _,
                        _,
                        alignedUniSeq,
                        alignedPdbSeq,
                        _) =>
                    val percentIdentity = {
                      val matches =
                        (alignedUniSeq.s.toSeq zip alignedPdbSeq.s.toSeq)
                          .count { case (a, b) => a == b }
                      matches.toDouble / alignedUniSeq.s.size
                    }
                    val percentIdentityPDB = {
                      val matches =
                        (alignedUniSeq.s.toSeq zip alignedPdbSeq.s.toSeq)
                          .filter { case (_, pdbChar) => pdbChar != '-' }
                          .count { case (a, b) => a == b }
                      matches.toDouble / alignedPdbSeq.s.count(pdbChar =>
                        pdbChar != '-')
                    }
                    AlignmentDetails(uniId,
                                     pdbId,
                                     pdbChain,
                                     percentIdentity,
                                     percentIdentityPDB,
                                     alignedUniSeq,
                                     alignedPdbSeq)
                },
                name = batchName + ".alignments.js.gz"
              )

              mapping.flatMap { m =>
                alignmentData.flatMap { al =>
                  featureMapping.map { fm =>
                    UniProtPdbOutput(List((ids.toList, m, fm, al)))
                  }
                }
              }

            }

    }

}
