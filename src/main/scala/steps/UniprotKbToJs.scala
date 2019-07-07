package sd.steps

import sd._
import scala.concurrent._
import tasks._
import tasks.util.AkkaStreamComponents
import tasks.jsonitersupport._
import tasks.ecoll._
import tasks.util.TempFile
import fileutils._
import index2._
import akka.stream.scaladsl.{Source}
import akka.stream.scaladsl.StreamConverters

object UniprotKbToJs {
  val task =
    AsyncTask[SharedFile, EColl[UniProtEntry]]("uniprotkb2js", 2) {
      uniprotkb => implicit ctx =>
        log.info("start converting uniprot kb to js " + uniprotkb)
        implicit val mat = ctx.components.actorMaterializer
        val scalaSource = scala.io.Source.fromInputStream(
          uniprotkb.source
            .via(AkkaStreamComponents.gunzip())
            .runWith(StreamConverters.asInputStream()))
        val parsed =
          Source.fromIterator(() => IOHelpers.readUniProtFile(scalaSource))

        EColl
          .fromSource(parsed,
                      name = uniprotkb.name + ".json.gz",
                      partitionSize = 1024 * 1024 * 50,
                      parallelism = resourceAllocated.cpu)

    }
}

case class UniprotIndexedByGene(fs: Set[SharedFile])

object UniprotIndexedByGene {
  import com.github.plokhotnyuk.jsoniter_scala.macros._
  import com.github.plokhotnyuk.jsoniter_scala.core._
  implicit val codec: JsonValueCodec[UniprotIndexedByGene] =
    JsonCodecMaker.make[UniprotIndexedByGene](CodecMakerConfig())
}

object IndexUniByGeneName {

  val UniEntryByGene = Table(name = "UNIbyGENE",
                             uniqueDocuments = true,
                             compressedDocuments = true)

  val task =
    AsyncTask[EColl[UniProtEntry], UniprotIndexedByGene]("indexpdbnamesbygene",
                                                         1) {
      uniprot => implicit ctx =>
        log.info("start indexing " + uniprot)
        implicit val mat = ctx.components.actorMaterializer
        val tmpFolder = TempFile.createTempFile("indexunibygene")
        tmpFolder.delete
        tmpFolder.mkdirs
        val tableManager = TableManager(tmpFolder)

        val writer = tableManager.writer(UniEntryByGene)

        uniprot
          .source(resourceAllocated.cpu)
          .runForeach { uniprotelem =>
            import com.github.plokhotnyuk.jsoniter_scala.core.writeToString
            val js = writeToString(uniprotelem)
            writer.add(Doc(js), uniprotelem.geneNames.map(_.value))
          }
          .map { _ =>
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
