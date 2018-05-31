package sd.steps

import sd._
import scala.concurrent._
import tasks._
import tasks.queue.NodeLocalCache
import tasks.upicklesupport._
import fileutils._

case class UniProtPdbInput(
    uniprotKb: SharedFile,
    uniProtId: List[UniId],
    batchName: String,
    genomeUniJoinFile: JsDump[sd.JoinGencodeToUniprot.MapResult])

case class UniProtPdbFullInput(
    uniprotKb: SharedFile,
    genomeUniJoinFile: JsDump[sd.JoinGencodeToUniprot.MapResult])

case class UniProtPdbFullOutput(
    tables: List[(List[(UniId, PdbId, PdbChain, Int, Int)],
                  JsDump[JoinUniprotWithPdb.T1],
                  JsDump[JoinUniprotWithPdb.T2],
                  JsDump[AlignmentDetails])],
    qc: SharedFile)
    extends ResultWithSharedFiles(qc +: tables.map(_._2.sf): _*)

case class UniProtPdbOutput(
    tables: List[(List[(UniId, PdbId, PdbChain, Int, Int)],
                  JsDump[JoinUniprotWithPdb.T1],
                  JsDump[JoinUniprotWithPdb.T2],
                  JsDump[AlignmentDetails])])
    extends ResultWithSharedFiles(tables.map(_._2.sf): _*)

object JoinUniprotWithPdb {

  type T1 = (UniId,
             PdbId,
             PdbChain,
             PdbResidueNumberUnresolved,
             PdbNumber,
             PdbSeq,
             UniNumber,
             UniSeq,
             Boolean)

  type T2 =
    (UniId, PdbId, PdbChain, Set[(UniprotFeatureName, Set[PdbResidueNumber])])

  def downloadUniprot2(uniprotkbSF: SharedFile, genomeUniJoin: JsDump[UniId])(
      implicit ts: tasks.queue.ComputationEnvironment) = {
    log.info(
      "Downloading uniprot + genome file.." + uniprotkbSF.name + " " + genomeUniJoin.sf.name)
    uniprotkbSF.file.flatMap { localFile =>
      genomeUniJoin.sf.file.map { genomeJoinFile =>
        val uniIdFilter: Set[UniId] =
          genomeUniJoin.iterator(genomeJoinFile)(it => it.toSet)
        log.info(uniIdFilter.take(10).toString)
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
                      genomeUniJoin: JsDump[sd.JoinGencodeToUniprot.MapResult])(
      implicit ts: tasks.queue.ComputationEnvironment) = {
    log.info(
      "Downloading uniprot + genome file.." + uniprotkbSF.name + " " + genomeUniJoin.sf.name)
    uniprotkbSF.file.flatMap { localFile =>
      genomeUniJoin.sf.file.map { genomeJoinFile =>
        val uniIdFilter = genomeUniJoin.iterator(genomeJoinFile)(it =>
          sd.JoinGencodeToUniprot.readUniProtIds(it))
        log.info(uniIdFilter.take(10).toString)
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
                      log.info("Batch size: {}", seq.size)
                      subtask(
                        UniProtPdbInput(
                          uniprotkbSF,
                          seq.map(_._1).sortBy(_.s).toList,
                          "batch" + idx,
                          genomeUniJoinFileSF))(CPUMemoryRequest(1, 1000))
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
                log.info("Join $uniID with PDB")
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
                  log.info(s"$uniId - $pdbId - $pdbChain")
                  mapping.iterator.map {
                    case (pdbSeqNum, uniNum, pdbResNum, mat) =>
                      val pdb2 =
                        pdbResNum.num.toString + pdbResNum.insertionCode
                          .getOrElse("")

                      (uniId,
                       pdbId,
                       pdbChain,
                       PdbResidueNumberUnresolved(pdb2),
                       PdbNumber(pdbSeqNum.i),
                       PdbSeq(pdbSeq.s(pdbSeqNum.i).toString),
                       UniNumber(uniNum.i),
                       UniSeq(uniSeq.s(uniNum.i).toString),
                       mat)

                  }
              }
              val mapping =
                JsDump.fromIterator(iter, name = batchName + ".json.gz")

              val featureMappingIter = result.iterator.map {
                case (uniId, pdbId, pdbChain, _, _, _, _, _, features) =>
                  (uniId, pdbId, pdbChain, features.toSet)
              }

              val featureMapping =
                JsDump.fromIterator(featureMappingIter,
                                    name = batchName + ".features.json.gz")

              val alignmentData = JsDump.fromIterator(
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
