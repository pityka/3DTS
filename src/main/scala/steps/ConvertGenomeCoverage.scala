package sd.steps

import sd._
import tasks._

import tasks.jsonitersupport._
import stringsplit._
import akka.stream.scaladsl._
import akka.util._
import scala.collection.mutable.ArrayBuffer
import tasks.util.AkkaStreamComponents

case class GnomadCoverageFile(files: SharedFile, totalSize: Int)

object GnomadCoverageFile {
  import com.github.plokhotnyuk.jsoniter_scala.macros._
  import com.github.plokhotnyuk.jsoniter_scala.core._
  implicit val codec: JsonValueCodec[GnomadCoverageFile] =
    JsonCodecMaker.make[GnomadCoverageFile](CodecMakerConfig())
}

case class GnomadCoverageFiles(files: Seq[SharedFile], totalSize: Int)

object GnomadCoverageFiles {
  import com.github.plokhotnyuk.jsoniter_scala.macros._
  import com.github.plokhotnyuk.jsoniter_scala.core._
  implicit val codec: JsonValueCodec[GnomadCoverageFiles] =
    JsonCodecMaker.make[GnomadCoverageFiles](CodecMakerConfig())
}

object ConvertGenomeCoverage {
  import tasks.ecoll.EColl

  def convertLine(frame: ByteString,
                  totalSize: Int,
                  spl: ArrayBuffer[String]) = {
    stringsplit.storeIterInArrayAll(frame.utf8String.split1Iter('\t').take(7),
                                    spl)
    val chr = "chr" + spl(0)
    val bp = spl(1).toInt
    val histColumns: List[Int] = List(6)
    val callsAtLeast10x = (histColumns
      .map(spl)
      .take(3)
      .map(_.toDouble)
      .sum * totalSize.toDouble).toInt

    GenomeCoverage(chr, bp, callsAtLeast10x)
  }

  val task =
    AsyncTask[GnomadCoverageFile, EColl[GenomeCoverage]](
      "convertgenomecoverage-1",
      1) {
      case GnomadCoverageFile(coverageFile, totalSize) =>
        implicit ctx =>
          log.info("Process " + coverageFile.name)
          implicit val mat = ctx.components.actorMaterializer
          val buffer = scala.collection.mutable.ArrayBuffer[String]()
          coverageFile.source
            .via(Framing.delimiter(ByteString("\n"),
                                   maximumFrameLength = Int.MaxValue))
            .map(convertLine(_, totalSize, buffer))
            .runWith(EColl.sink[GenomeCoverage](
              coverageFile.name + "genomecoverage.js.gz"))

    }

  val gnomadToEColl =
    AsyncTask[GnomadCoverageFiles, EColl[GenomeCoverage]](
      "convertgenomecoverage-ecoll-1",
      1) {
      case GnomadCoverageFiles(coverageFiles, totalSize) =>
        implicit ctx =>
          log.info("Process " + coverageFiles)
          val buffer = scala.collection.mutable.ArrayBuffer[String]()
          val source = Source(coverageFiles.toList).flatMapConcat { file =>
            file.source
              .via(AkkaStreamComponents.gunzip())
              .via(AkkaStreamComponents
                .delimiter('\n', maximumFrameLength = Int.MaxValue))
              .drop(1)
              .map(convertLine(_, totalSize, buffer))
          }

          EColl.fromSource(source,
                           coverageFiles.head.name,
                           1024 * 1024 * 50,
                           parallelism = resourceAllocated.cpu)

    }

}
