package sd.steps

import sd._
import java.io.File
import collection.JavaConversions._
import scala.sys.process._
import scala.concurrent._
import scala.concurrent.duration._
import tasks._
import tasks.upicklesupport._
import tasks.queue.NodeLocalCache
import tasks.util.TempFile
import java.io._

import fileutils._
import stringsplit._

import IOHelpers._
import MathHelpers._
import Model._

import akka.stream.ActorMaterializer

import index2._
import SharedTypes._

object UniprotKbToJs {
  val task =
    AsyncTask[SharedFile, JsDump[UniProtEntry]]("uniprotkb2js", 2) {
      uniprotkb => implicit ctx =>
        log.info("start converting uniprot kb to js " + uniprotkb)
        implicit val mat = ctx.components.actorMaterializer
        uniprotkb.file.flatMap { uniprotkbL =>
          val source = fileutils.createSource(uniprotkbL)

          JsDump
            .fromIterator(IOHelpers.readUniProtFile(source),
                          name = uniprotkb.name + ".json.gz")
            .andThen {
              case _ => source.close
            }

        }
    }
}

case class UniprotIndexedByGene(fs: Set[SharedFile])
    extends ResultWithSharedFiles(fs.toSeq: _*)

object IndexUniByGeneName {

  val UniEntryByGene = Table(name = "UNIbyGENE",
                             uniqueDocuments = true,
                             compressedDocuments = true)

  val task =
    AsyncTask[JsDump[UniProtEntry], UniprotIndexedByGene]("indexpdbnamesbygene",
                                                          1) {
      uniprot => implicit ctx =>
        log.info("start indexing " + uniprot)
        implicit val mat = ctx.components.actorMaterializer
        val tmpFolder = TempFile.createTempFile("indexunibygene")
        tmpFolder.delete
        tmpFolder.mkdirs
        val tableManager = TableManager(tmpFolder)

        val writer = tableManager.writer(UniEntryByGene)

        uniprot.source
          .runForeach { uniprotelem =>
            val js = upickle.default.write(uniprotelem)
            writer.add(Doc(js), uniprotelem.geneNames.map(_.value))
          }
          .map { done =>
            writer.makeIndex(100000, 50)
          }
          .flatMap { _ =>
            Future
              .sequence(tmpFolder.listFiles.toList.map(f =>
                SharedFile(f, name = f.getName)))
              .map(x => UniprotIndexedByGene(x.toSet))
          }
    }
}
