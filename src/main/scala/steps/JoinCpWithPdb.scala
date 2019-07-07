package sd.steps

import sd._
import scala.concurrent._
import tasks._
import tasks.jsonitersupport._
import tasks.util.TempFile
import fileutils._
import stringsplit._
import tasks.ecoll._

import index2._

case class JoinCPWithPdbInput(
    gencodeUniprot: EColl[sd.JoinGencodeToUniprot.MapResult],
    pdbUniprot: List[EColl[JoinUniprotWithPdb.T1]])

object JoinCPWithPdbInput {
  import com.github.plokhotnyuk.jsoniter_scala.core._
  import com.github.plokhotnyuk.jsoniter_scala.macros._
  implicit val codec: JsonValueCodec[JoinCPWithPdbInput] =
    JsonCodecMaker.make[JoinCPWithPdbInput](sd.JsonIterConfig.config)
}

case class CpPdbIndex(fs: Set[SharedFile])

object CpPdbIndex {
  import com.github.plokhotnyuk.jsoniter_scala.core._
  import com.github.plokhotnyuk.jsoniter_scala.macros._
  implicit val codec: JsonValueCodec[CpPdbIndex] =
    JsonCodecMaker.make[CpPdbIndex](sd.JsonIterConfig.config)
}

object JoinCPWithPdb {

  val CpPdbTable = Table(name = "CPPDBxTerms",
                         uniqueDocuments = true,
                         compressedDocuments = true)

  val concatenate =
    AsyncTask[(EColl[PdbUniGencodeRow], EColl[PdbUniGencodeRow]),
              EColl[PdbUniGencodeRow]]("concatenatedCpPdb", 1) {
      case (js1, js2) =>
        implicit ctx =>
          implicit val mat = ctx.components.actorMaterializer
          (js1.source(resourceAllocated.cpu) ++ js2.source(
            resourceAllocated.cpu))
            .runWith(EColl.sink(js1.basename + ".concat." + js2.basename))

    }

  val indexCpPdb =
    AsyncTask[EColl[PdbUniGencodeRow], CpPdbIndex]("indexcppdb", 2) {
      pdbunigencode => implicit ctx =>
        import com.github.plokhotnyuk.jsoniter_scala.core._
        log.info("start indexing " + pdbunigencode)
        implicit val mat = ctx.components.actorMaterializer
        val tmpFolder = TempFile.createTempFile("index")
        tmpFolder.delete
        tmpFolder.mkdirs
        val tableManager = TableManager(tmpFolder)

        val writer = tableManager.writer(CpPdbTable)

        pdbunigencode
          .source(resourceAllocated.cpu)
          .runForeach { pdbunigencode =>
            val pdbId = pdbunigencode.pdbId.s
            val enst = pdbunigencode.ensT.s
            val uniid = pdbunigencode.uniId.s
            val chrpos = pdbunigencode.cp.s.split1('\t')
            val pdbchain = pdbunigencode.pdbChain.s
            val pdbres = pdbunigencode.pdbResidueNumberUnresolved.s

            val cp = chrpos(0) + "_" + chrpos(2)
            val js = writeToString(pdbunigencode)
            writer.add(Doc(js),
                       List(enst,
                            uniid,
                            pdbId,
                            cp,
                            pdbId + "_" + pdbchain,
                            pdbId + "_" + pdbchain + "_" + pdbres))
          }
          .map { _ =>
            writer.makeIndex(1000000, 50)
          }
          .flatMap { _ =>
            Future
              .sequence(tmpFolder.listFiles.toList.map(f =>
                SharedFile(f, name = f.getName)))
              .map(x => CpPdbIndex(x.toSet))
          }
    }

  val joinCpWithPdb = EColl.join2InnerTx(
    "joincpwithpdb-join",
    1,
    None,
    1024 * 1024 * 50
  )(
    preTransformA = spore[sd.JoinGencodeToUniprot.MapResult,
                          List[MappedTranscriptToUniprot]]({
      case succ: sd.JoinGencodeToUniprot.Success =>
        succ.v.filter(_.uniNumber.isDefined).toList
      case _ => Nil
    }),
    preTransformB =
      spore[JoinUniprotWithPdb.T1, List[JoinUniprotWithPdb.T1]](x => List(x)),
    keyA = spore[MappedTranscriptToUniprot, String](s =>
      s.uniId.s + "_" + s.uniNumber.get.i),
    keyB = spore[JoinUniprotWithPdb.T1, String](s =>
      s.uniId.s + "_" + s.uniNumber.i),
    postTransform =
      spore[Seq[(MappedTranscriptToUniprot, JoinUniprotWithPdb.T1)],
            List[PdbUniGencodeRow]] { list =>
        list.map {
          case (left, right) =>
            PdbUniGencodeRow(
              right.pdbId,
              right.pdbChain,
              right.pdbResidueNumber,
              right.pdbSeq,
              right.uniId,
              right.uniNumber,
              right.uniSeq,
              left.ensT,
              left.cp,
              left.indexInCodon,
              left.indexInTranscript,
              left.missenseConsequences,
              left.uniprotSequence,
              left.referenceNucleotide,
              left.indexInCds,
              left.perfectMatch
            )

        }.toList
      }
  )
  val task =
    AsyncTask[JoinCPWithPdbInput, EColl[PdbUniGencodeRow]]("cppdb-2", 1) {
      case JoinCPWithPdbInput(
          gencodeUniprot,
          pdbMaps
          ) =>
        implicit ctx =>
          joinCpWithPdb((gencodeUniprot, pdbMaps.reduce(_ ++ _)))(
            ResourceRequest((1, 3), 10000))
      // val uniprot2Genome
      //   : Future[scala.collection.mutable.Map[String, List[String]]] =
      //   gencodeUniprot.sf.file.map { localFile =>
      //     log.info(
      //       "Reading UniProt -> genome map .." + gencodeUniprot.sf.name)

      //     val mmap =
      //       scala.collection.mutable.AnyRefMap[String, List[String]]()

      //     gencodeUniprot.iterator(localFile)(
      //       i =>
      //         i.filter(_.isInstanceOf[sd.JoinGencodeToUniprot.Success])
      //           .map(_.asInstanceOf[sd.JoinGencodeToUniprot.Success])
      //           .flatMap(_.v)
      //           .map { line =>
      //             log.debug(s"Add $line to index")
      //             val uni: UniId = line.uniId
      //             val uninum: Option[UniNumber] = line.uniNumber
      //             (uni, uninum) -> writeToString(line)
      //           }
      //           .filter(_._1._2.isDefined)
      //           .map(x => (x._1._1.s + "_" + x._1._2.get.i) -> x._2)
      //           .foreach {
      //             case (k, line) =>
      //               mmap.get(k) match {
      //                 case None    => mmap.update(k, List(line))
      //                 case Some(l) => mmap.update(k, line :: l)
      //               }
      //         })

      //     log.info(
      //       "grouping done " + mmap.size + " " + mmap.foldLeft(0)(
      //         _ + _._2.size))
      //     mmap

      //   }

      // uniprot2Genome.flatMap { uniprot2Genome =>
      //   Future
      //     .sequence(pdbMaps.map {
      //       case jsondump =>
      //         jsondump.sf.file.map(f => jsondump -> f)
      //     })
      //     .flatMap { files =>
      //       val iters = files.map(x => x._1.createIterator(x._2))
      //       val iter: Iterator[PdbUniGencodeRow] =
      //         iters.map(_._1).iterator.flatMap { iter =>
      //           iter
      //             .map {
      //               case unipdbline =>
      //                 val uni: UniId = unipdbline.uniId
      //                 val uninum: UniNumber = unipdbline.uniNumber
      //                 val genomeLines =
      //                   uniprot2Genome.get(uni.s + "_" + uninum.i)
      //                 log.debug(s"$unipdbline JOIN $genomeLines")
      //                 (genomeLines, unipdbline)
      //             }
      //             .filter(_._1.isDefined)
      //             .map(x => (x._1.get -> x._2))
      //             .flatMap {
      //               case (genomeLines,
      //                     JoinUniprotWithPdb.T1(uniid,
      //                                           pdbid,
      //                                           pdbch,
      //                                           pdbres,
      //                                           _,
      //                                           pdbaa,
      //                                           unin,
      //                                           uniaa,
      //                                           _)) =>
      //                 genomeLines.iterator.map {
      //                   case genomeLine =>
      //                     val t1: MappedTranscriptToUniprot =
      //                       readFromString[MappedTranscriptToUniprot](
      //                         genomeLine)
      //                     PdbUniGencodeRow(
      //                       pdbid,
      //                       pdbch,
      //                       pdbres,
      //                       pdbaa,
      //                       uniid,
      //                       unin,
      //                       uniaa,
      //                       t1.ensT,
      //                       t1.cp,
      //                       t1.indexInCodon,
      //                       t1.indexInTranscript,
      //                       t1.missenseConsequences,
      //                       t1.uniprotSequence,
      //                       t1.referenceNucleotide,
      //                       t1.indexInCds,
      //                       t1.perfectMatch
      //                     )
      //                 }

      //             }

      //         }

      //       EColl
      //         .fromIterator[PdbUniGencodeRow](
      //           iter,
      //           gencodeUniprot.sf.name + "." + pdbMaps.hashCode)
      //         .andThen { case _ => iters.foreach(_._2.close) }

      //     }

      // }
    }

}
