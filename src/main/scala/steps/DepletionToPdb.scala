package sd.steps

import sd._
import scala.concurrent._
import tasks._
import tasks.ecoll._
import tasks.jsonitersupport._
import tasks.util.TempFile
import fileutils._
import index2._

case class Depletion2PdbInput(
    posteriorFile: EColl[DepletionRow],
    contextFile: JsDump[StructuralContext.T1]
)

object Depletion2PdbInput {
  import com.github.plokhotnyuk.jsoniter_scala.core._
  import com.github.plokhotnyuk.jsoniter_scala.macros._
  implicit val codec: JsonValueCodec[Depletion2PdbInput] =
    JsonCodecMaker.make[Depletion2PdbInput](CodecMakerConfig())
}

case class ScoresIndexedByPdbId(fs: Set[SharedFile])

object ScoresIndexedByPdbId {
  import com.github.plokhotnyuk.jsoniter_scala.core._
  import com.github.plokhotnyuk.jsoniter_scala.macros._
  implicit val codec: JsonValueCodec[ScoresIndexedByPdbId] =
    JsonCodecMaker.make[ScoresIndexedByPdbId](CodecMakerConfig())
}

object DepletionToPdb {
  import com.github.plokhotnyuk.jsoniter_scala.core._
  val ScoresByPdbIdTable = Table(name = "SCORESbyPDBID",
                                 uniqueDocuments = true,
                                 compressedDocuments = true)

  val indexByPdbId =
    AsyncTask[JsDump[DepletionScoresByResidue], ScoresIndexedByPdbId](
      "indexscores",
      2) { scoresOnPdb => implicit ctx =>
      log.info("start indexing " + scoresOnPdb)
      implicit val mat = ctx.components.actorMaterializer
      val tmpFolder = TempFile.createTempFile("indexscores")
      tmpFolder.delete
      tmpFolder.mkdirs
      val tableManager = TableManager(tmpFolder)

      val writer = tableManager.writer(ScoresByPdbIdTable)

      scoresOnPdb.source
        .runForeach { scoreByResidue =>
          val pdbId = scoreByResidue.pdbId
          val js = writeToString(scoreByResidue)
          writer.add(Doc(js), List(pdbId))
        }
        .map { _ =>
          writer.makeIndex(1000000, 50)
        }
        .flatMap { _ =>
          Future
            .sequence(tmpFolder.listFiles.toList.map(f =>
              SharedFile(f, name = f.getName)))
            .map(x => ScoresIndexedByPdbId(x.toSet))
        }
    }

  val task =
    AsyncTask[Depletion2PdbInput, JsDump[DepletionScoresByResidue]](
      "depletion2pdb",
      1) {

      case Depletion2PdbInput(
          scoresJs,
          contextJs
          ) =>
        implicit ctx =>
          implicit val am = ctx.components.actorMaterializer
          for {
            contextL <- contextJs.sf.file
            scoreMap <- scoresJs
              .source(resourceAllocated.cpu)
              .runWith(akka.stream.scaladsl.Sink.seq)
              .map(_.groupBy(_.featureKey))
            result <- {

              contextJs.iterator(contextL) { contextIter =>
                val iter = contextIter.flatMap {
                  case StructuralContextFeature(fkey, pdbResidues, _) =>
                    val scores = scoreMap.get(fkey).getOrElse(Vector())
                    log.debug(s"Scores found for $fkey: ${scores.size}")
                    pdbResidues.iterator.flatMap {
                      case (pdbChain, pdbResidue) =>
                        scores.iterator.map { longLine =>
                          DepletionScoresByResidue(fkey.pdbId.s,
                                                   pdbChain.s,
                                                   pdbResidue.s,
                                                   longLine)
                        }

                    }
                }
                JsDump.fromIterator(iter,
                                    name = scoresJs.partitions.headOption.fold(
                                      "scores")(_.name) + ".back2pdb.json.gz")
              }

            }

          } yield result

    }
}
