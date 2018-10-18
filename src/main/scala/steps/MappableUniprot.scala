package sd.steps

import sd._
import tasks._
import tasks.upicklesupport._

object MappableUniprot {

  val task =
    AsyncTask[JsDump[sd.JoinGencodeToUniprot.MapResult], JsDump[UniId]](
      "mappableUniprot",
      1) { mapresult => implicit ctx =>
      log.info("Start extracting uniprot ids from " + mapresult.sf.name)
      mapresult.sf.file.flatMap { genomeJoinFile =>
        val uni: Set[UniId] = mapresult.iterator(genomeJoinFile)(it =>
          sd.JoinGencodeToUniprot.readUniProtIds(it))

        uni.foreach { uni =>
          log.debug(s"$uni mappable.")
        }

        JsDump.fromIterator(uni.iterator,
                            name = mapresult.sf.name + ".uniids.json.gz")

      }
    }
}
