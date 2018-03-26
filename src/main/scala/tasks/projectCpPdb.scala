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
import SharedTypes._

object ProjectCpPdb {

  case class SimplePdbUniGencodeRow(pdbId: String,
                                    pbdChain: String,
                                    pdbResidue: String,
                                    enst: String,
                                    chromosome: String,
                                    position1: Int,
                                    reference: String)

  val task = AsyncTask[JsDump[SharedTypes.PdbUniGencodeRow],
                       JsDump[SimplePdbUniGencodeRow]]("projectcppdb", 1) {

    cppdb => implicit ctx =>
      implicit val mat = ctx.components.actorMaterializer
      cppdb.source.map {
        case (PdbId(pdbid),
              PdbChain(pdbch),
              PdbResidueNumberUnresolved(pdbres),
              _,
              _,
              _,
              _,
              EnsT(enst),
              ChrPos(chrpos),
              _,
              _,
              _,
              _,
              RefNuc(ref),
              _,
              _) =>
          val spl = chrpos.split('\t')

          SimplePdbUniGencodeRow(pdbid,
                                 pdbch,
                                 pdbres,
                                 enst,
                                 spl(0),
                                 spl(2).toInt,
                                 ref.toString)
      }.runWith(JsDump.sink[SimplePdbUniGencodeRow](
        name = cppdb.sf.name + ".projected.json.gz"))

  }

}
