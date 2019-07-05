package sd.steps

import sd._
import tasks._
import tasks.jsonitersupport._
import tasks.ecoll._

object MappableUniprot {

  val task =
    AsyncTask[EColl[sd.JoinGencodeToUniprot.MapResult], EColl[UniId]](
      "mappableUniprot",
      1) { mapresult => implicit ctx =>
      implicit val mat = ctx.components.actorMaterializer
      log.info("Start extracting uniprot ids from " + mapresult.basename)
      sd.JoinGencodeToUniprot.readUniProtIds(mapresult.source(1)).run.flatMap {
        uni =>
          uni.foreach { uni =>
            log.debug(s"$uni mappable.")
          }

          EColl.fromIterator(uni.iterator,
                             name = mapresult.basename + ".uniids.json.gz")
      }

    }
}
