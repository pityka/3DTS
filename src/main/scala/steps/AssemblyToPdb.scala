package sd.steps

import sd._
import java.io.File
import collection.JavaConversions._
import scala.sys.process._
import scala.concurrent._
import scala.concurrent.duration._
import tasks._
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

import akka.stream.ActorMaterializer

import akka.actor.Extension
import scala.util._
import akka.util.ByteString

import scala.util.Try

case class GeneSymbol(s: String) extends AnyVal
object GeneSymbol {
  implicit val pickler = AnyValPicklers(GeneSymbol.unapply, GeneSymbol.apply)
}

case class Assembly2PdbInput(uniprotKb: SharedFile, mappableIds: JsDump[UniId])

case class FetchCifOutput(cifFiles: Map[PdbId, SharedFile])
    extends ResultWithSharedFiles(cifFiles.map(_._2).toList: _*)

case class Assembly2PdbOutput(pdbFiles: Map[PdbId, SharedFile])
    extends ResultWithSharedFiles(pdbFiles.map(_._2).toList: _*)

object AssemblyToPdb {

  def retry[A](i: Int)(f: => A): A = Try(f) match {
    case Success(x)          => x
    case Failure(x) if i > 0 => retry(i - 1)(f)
    case Failure(e)          => throw e
  }

  val fetchCif =
    AsyncTask[Assembly2PdbInput, FetchCifOutput]("ciffetch", 3) {

      case Assembly2PdbInput(
          uniprotKb,
          mappableIds
          ) =>
        implicit ctx =>
          log.info("Fetching cif files.")
          implicit val mat = ctx.components.actorMaterializer

          JoinUniprotWithPdb.downloadUniprot2(uniprotKb, mappableIds).flatMap {
            (uniprotKb: Map[UniId, UniProtEntry]) =>
              log.info("Size of uniprotkb {}", uniprotKb.size)

              val pdbIds = sd.JoinUniprotWithPdb
                .extractPdbIdsFromUniprot(uniprotKb)
                .map(_._2)
                .distinct
                .toList
              log.info("Unique pdb ids: " + pdbIds.size)

              Source(pdbIds)
                .mapAsync(resourceAllocated.cpu) { pdbId =>
                  Future {
                    log.info("Fetching " + pdbId)

                    Try(retry(3)(sd.JoinUniprotWithPdb.fetchCif(pdbId))) match {
                      case Success(cifString) =>
                        SharedFile(writeToTempFile(cifString), pdbId.s + ".cif")
                          .map(s => Some(pdbId -> s))
                      case Failure(e) =>
                        log.error(e, "Failed fetch cif " + pdbId)
                        Future.successful(None)
                    }

                  }.flatMap(x => x)
                }
                .runWith(Sink.seq)
                .map { seq =>
                  FetchCifOutput(seq.filter(_.isDefined).map(_.get).toMap)
                }

          }

    }

  val assembly =
    AsyncTask[FetchCifOutput, Assembly2PdbOutput]("pdbassembly", 2) {

      case FetchCifOutput(
          cifs
          ) =>
        implicit ctx =>
          implicit val mat = ctx.components.actorMaterializer

          Source(cifs)
            .mapAsync(resourceAllocated.cpu) {
              case (pdbId, cifSf) =>
                cifSf.source
                  .runFold(ByteString())(_ ++ _)
                  .map(_.utf8String)
                  .flatMap { cifString =>
                    log.info("Assembly " + pdbId)
                    if (!cifString.startsWith("data_"))
                      Future.successful(None)
                    else {

                      val cifContents = CIFContents.parseCIF(
                        scala.io.Source.fromString(cifString).getLines.toList)
                      if (cifContents.isFailure) {
                        log.info("Cif parsing failed for " + pdbId)
                        Future.successful(None)
                      } else {
                        val pdbString = cifContents.get.asPDB
                        if (pdbString.isEmpty || cifString.isEmpty) {
                          println(pdbString)
                          println(cifString)
                          println(cifContents)
                          println(pdbId)
                          log.info("Cif parsing failed for " + pdbId)
                          Future.successful(None)
                        } else {

                          SharedFile(writeToTempFile(pdbString),
                                     pdbId.s + ".assembly.pdb").map(s =>
                            Some(pdbId -> s))

                        }
                      }
                    }
                  }

            }
            .runWith(Sink.seq)
            .map { seq =>
              Assembly2PdbOutput(seq.filter(_.isDefined).map(_.get).toMap)
            }

    }
}
