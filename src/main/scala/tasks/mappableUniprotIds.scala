import java.io.File
import collection.JavaConversions._
import scala.sys.process._
import scala.concurrent._
import scala.concurrent.duration._
import tasks._
import tasks.upicklesupport._
  
import tasks.queue.NodeLocalCache
import tasks.util.TempFile

import fileutils._
import stringsplit._

import IOHelpers._
import MathHelpers._
import Model._

import akka.stream.ActorMaterializer

import akka.actor.Extension

object MappableUniprot {

  val task =
    AsyncTask[JsDump[Ensembl2Uniprot.MapResult], JsDump[UniId]](
      "mappableUniprot",
      1) { mapresult => implicit ctx =>
      log.info("Start extracting uniprot ids from " + mapresult.sf.name)
      mapresult.sf.file.flatMap { genomeJoinFile =>
        val uni: Set[UniId] = mapresult.iterator(genomeJoinFile)(it =>
          Ensembl2Uniprot.readUniProtIds(it))

        JsDump.fromIterator(uni.iterator,
                            name = mapresult.sf.name + ".uniids.json.gz")

      }
    }
}
