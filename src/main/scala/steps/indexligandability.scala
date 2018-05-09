package sd.steps

import sd._
import scala.concurrent._
import tasks._
import tasks.upicklesupport._
import tasks.util.TempFile

import index2._

case class LigandabilityIndexedByUniId(fs: Set[SharedFile])
    extends ResultWithSharedFiles(fs.toSeq: _*)

object LigandabilityCsvToJs {

  val task =
    AsyncTask[SharedFile, JsDump[LigandabilityRow]]("indexligandabilityByUniId",
                                                    1) {
      ligandability => implicit ctx =>
        log.info("start converting ligandability to js " + ligandability)
        ligandability.file.flatMap { ligandabilityL =>
          val source = fileutils.createSource(ligandabilityL)

          JsDump
            .fromIterator(IOHelpers.readLigandability(source),
                          name = ligandability.name + ".json.gz")
            .andThen {
              case _ => source.close
            }

        }
    }
}

object IndexLigandability {

  val LigandabilityByUniId = Table(name = "LIGANDABILITYbyUNIID",
                                   uniqueDocuments = true,
                                   compressedDocuments = true)

  val task =
    AsyncTask[JsDump[LigandabilityRow], LigandabilityIndexedByUniId](
      "indexligandabilityByUniId",
      1) { ligandability => implicit ctx =>
      log.info("start indexing " + ligandability)
      implicit val mat = ctx.components.actorMaterializer
      val tmpFolder = TempFile.createTempFile("indexligandabilitybyuniid")
      tmpFolder.delete
      tmpFolder.mkdirs
      val tableManager = TableManager(tmpFolder)

      val writer = tableManager.writer(LigandabilityByUniId)

      ligandability.source
        .runForeach { elem =>
          val js = upickle.default.write(elem)
          writer.add(Doc(js), List(elem.uniid.s))
        }
        .map { done =>
          writer.makeIndex(100000, 50)
        }
        .flatMap { _ =>
          Future
            .sequence(tmpFolder.listFiles.toList.map(f =>
              SharedFile(f, name = f.getName)))
            .map(x => LigandabilityIndexedByUniId(x.toSet))
        }
        .andThen {
          case e =>
            log.info("Finished indexing " + e)
        }
    }
}
